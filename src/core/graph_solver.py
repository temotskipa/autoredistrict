"""Graph-based redistricting solver primitives.

The solver in this module is deliberately deterministic and constraint-first:
it builds districts by growing contiguous regions across an adjacency graph, then
uses local boundary moves that preserve contiguity. It is not a final
research-grade optimizer, but it establishes the architecture and invariants the
production solver needs.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set

import geopandas as gpd
import numpy as np
import pandas as pd

from .redistricting_algorithms import _polsby_popper_static, _weighted_partisan_share
from .utils import build_adjacency, is_contiguous


@dataclass(frozen=True)
class GraphSolveResult:
    districts: List[gpd.GeoDataFrame]
    assignment: Dict[str, int]
    objective_score: float
    metadata: Dict[str, object]


@dataclass(frozen=True)
class OptimizationProfile:
    name: str
    step_share: float
    starting_temperature: float
    population_multiplier: float = 1.0
    compactness_multiplier: float = 1.0
    centering_multiplier: float = 1.0
    partisan_multiplier: float = 1.0
    surface_tension_power: float = 2.0


FINAL_OBJECTIVE_PROFILE = OptimizationProfile(
    name="final_objective",
    step_share=1.0,
    starting_temperature=0.08,
)


class GraphRedistrictingSolver:
    """Generate contiguous district plans over a geographic adjacency graph."""

    def __init__(
        self,
        state_data: gpd.GeoDataFrame,
        num_districts: int,
        population_equality_weight: float = 1.0,
        compactness_weight: float = 1.0,
        partisan_weight: float = 0.0,
        target_party: Optional[int] = None,
        vra_compliance: bool = False,
        communities_of_interest: Optional[Sequence[str]] = None,
        random_seed: int = 0,
        attempts: int = 5,
        local_search_passes: int = 3,
        annealing_steps: int = 50,
        recombination_passes: int = 4,
    ):
        if num_districts < 1:
            raise ValueError("num_districts must be at least 1.")
        if state_data.empty:
            raise ValueError("state_data must not be empty.")

        self.gdf = state_data.reset_index(drop=True).copy()
        self.metric_gdf = self._metric_gdf(self.gdf)
        self.num_districts = min(int(num_districts), len(self.gdf))
        self.population_equality_weight = self._clamp_weight(population_equality_weight)
        self.compactness_weight = self._clamp_weight(compactness_weight)
        self.partisan_weight = self._clamp_weight(partisan_weight)
        self.target_party = target_party
        self.vra_compliance = bool(vra_compliance)
        self.communities_of_interest = set(str(item) for item in (communities_of_interest or []))
        self.random_seed = int(random_seed)
        self.attempts = max(1, int(attempts))
        self.local_search_passes = max(0, int(local_search_passes))
        self.annealing_steps = max(0, int(annealing_steps))
        self.recombination_passes = max(0, int(recombination_passes))
        self.adjacency = build_adjacency(self.gdf)

        self.populations = pd.to_numeric(
            self.gdf["P1_001N"], errors="coerce"
        ).fillna(0).to_numpy(dtype=float)
        self.total_population = float(self.populations.sum())
        self.ideal_population = (
            self.total_population / self.num_districts if self.num_districts else 0.0
        )
        self.minority_populations = self._minority_populations()
        self.statewide_minority_share = (
            float(self.minority_populations.sum() / self.total_population)
            if self.total_population > 0
            else 0.0
        )

    def solve(self) -> GraphSolveResult:
        """Return the best plan found across deterministic multi-start attempts."""
        best_assignment: Optional[List[int]] = None
        best_score = float("inf")
        best_strategy = "unknown"
        best_annealing_stats: Dict[str, object] = {
            "planned_steps": 0,
            "attempted_moves": 0,
            "accepted_moves": 0,
            "accepted_worse_moves": 0,
            "phases": [],
        }

        attempt_seeds = [self.random_seed + offset for offset in range(self.attempts)]
        if 0 not in attempt_seeds:
            attempt_seeds.append(0)

        for seed in attempt_seeds:
            rng = random.Random(seed)
            candidates = []
            balanced = self._try_balanced_recursive_plan(rng)
            if balanced is not None:
                candidates.append(("balanced_recursive", balanced))
            candidates.append(("greedy_growth", self._grow_initial_plan(rng)))

            for strategy, assignment in candidates:
                assignment = self._improve_by_boundary_moves(assignment, rng)
                assignment, annealing_stats = self._anneal_boundary_moves(assignment, rng)
                assignment = self._improve_by_boundary_moves(assignment, rng)
                assignment = self._improve_by_pair_recom_splits(assignment, rng)
                score = self._assignment_score(assignment)
                if score < best_score:
                    best_score = score
                    best_assignment = assignment
                    best_strategy = strategy
                    best_annealing_stats = annealing_stats

        if best_assignment is None:
            raise RuntimeError("Graph solver failed to produce an assignment.")

        districts = self._districts_from_assignment(best_assignment)
        assignment_by_geoid = {
            str(self.gdf.at[idx, "GEOID"]): int(district_id) + 1
            for idx, district_id in enumerate(best_assignment)
        }
        metadata = {
            "solver": "graph_balanced_local_search",
            "strategy": best_strategy,
            "random_seed": self.random_seed,
            "attempts": self.attempts,
            "attempted_seed_count": len(attempt_seeds),
            "seed_schedule": attempt_seeds,
            "local_search_passes": self.local_search_passes,
            "annealing_steps": self.annealing_steps,
            "recombination_passes": self.recombination_passes,
            "objective_score": round(float(best_score), 6),
            "unit_count": int(len(self.gdf)),
            "adjacency_edges": int(sum(len(v) for v in self.adjacency.values()) / 2),
            "hard_constraints": [
                "all_units_assigned_once",
                "districts_seeded_nonempty",
                "local_contiguity_rejection_before_scoring",
            ],
            "contiguity_enforced": True,
            "vra_compliance_requested": self.vra_compliance,
            "statewide_minority_share": round(self.statewide_minority_share, 6),
            "communities_of_interest_units": len(self.communities_of_interest),
            "annealing": {
                "accepts_worse_moves": True,
                "boundary_weighting": "more_exposed_boundary_units_flip_more_often",
                "temperature_schedule": "three_phase_linear_cooling",
                **best_annealing_stats,
            },
            "video_informed_features": [
                "voronoi_like_spatial_seed_layout",
                "boundary_unit_flip_annealing",
                "fourth_power_population_outlier_penalty",
                "center_of_population_distance_metric",
                "boundary_exposure_surface_tension",
                "three_phase_weight_schedule",
                "contiguity_rejection_before_scoring",
                "adjacent_pair_recom_rebalancing",
            ],
        }
        return GraphSolveResult(districts, assignment_by_geoid, best_score, metadata)

    def _try_balanced_recursive_plan(self, rng: random.Random) -> Optional[List[int]]:
        try:
            parts = self._recursive_balanced_partition(
                set(range(len(self.gdf))),
                self.num_districts,
                rng,
            )
        except RuntimeError:
            return None
        if len(parts) != self.num_districts or any(not part for part in parts):
            return None

        assignment = [-1 for _ in range(len(self.gdf))]
        for district_id, part in enumerate(parts):
            for node in part:
                assignment[node] = district_id
        if any(item < 0 for item in assignment):
            return None
        return assignment

    def _recursive_balanced_partition(
        self,
        nodes: Set[int],
        seats: int,
        rng: random.Random,
    ) -> List[Set[int]]:
        if seats <= 1:
            return [set(nodes)]
        if len(nodes) < seats:
            raise RuntimeError("Not enough units to seed all requested districts.")

        seats_a = seats // 2
        seats_b = seats - seats_a
        split = self._find_balanced_connected_split(nodes, seats_a, seats_b, rng)
        if split is None:
            raise RuntimeError("Unable to find connected balanced split.")

        part_a, part_b = split
        return (
            self._recursive_balanced_partition(part_a, seats_a, rng)
            + self._recursive_balanced_partition(part_b, seats_b, rng)
        )

    def _find_balanced_connected_split(
        self,
        nodes: Set[int],
        seats_a: int,
        seats_b: int,
        rng: random.Random,
    ) -> Optional[tuple[Set[int], Set[int]]]:
        total_pop = float(self.populations[list(nodes)].sum())
        target_pop = total_pop * (seats_a / (seats_a + seats_b))
        max_nodes_a = len(nodes) - seats_b
        best_split: Optional[tuple[float, Set[int], Set[int]]] = None

        exact_split = self._find_exact_small_connected_split(
            nodes,
            seats_a,
            seats_b,
            target_pop,
        )
        if exact_split is not None:
            return exact_split

        seeds = list(nodes)
        rng.shuffle(seeds)
        seeds.sort(key=lambda node: abs(self.populations[node] - target_pop))

        for seed in seeds[: min(len(seeds), max(8, self.attempts))]:
            part_a = {seed}
            part_b = set(nodes) - part_a
            if len(part_b) < seats_b or not self._nodes_connected(part_b):
                continue

            pop_a = float(self.populations[seed])
            while len(part_a) < max_nodes_a:
                current_error = abs(pop_a - target_pop)
                frontier = self._frontier(part_a, part_b)
                if not frontier:
                    break

                candidates = []
                for candidate in frontier:
                    next_b = part_b - {candidate}
                    if len(next_b) < seats_b:
                        continue
                    if next_b and not self._nodes_connected(next_b):
                        continue
                    next_a = part_a | {candidate}
                    next_pop = pop_a + float(self.populations[candidate])
                    error = abs(next_pop - target_pop)
                    compactness = self._compactness_for_nodes(next_a)
                    candidates.append((error, 1.0 - compactness, candidate))

                if not candidates:
                    break

                candidates.sort()
                next_error, _compactness_penalty, next_node = candidates[0]
                if len(part_a) >= seats_a and pop_a >= target_pop and next_error > current_error:
                    break

                part_a.add(next_node)
                part_b.remove(next_node)
                pop_a += float(self.populations[next_node])

            if (
                len(part_a) >= seats_a
                and len(part_b) >= seats_b
                and self._nodes_connected(part_a)
                and self._nodes_connected(part_b)
            ):
                score = abs(pop_a - target_pop) / target_pop if target_pop else 0.0
                if best_split is None or score < best_split[0]:
                    best_split = (score, set(part_a), set(part_b))

        if best_split is None:
            return None
        return best_split[1], best_split[2]

    def _find_exact_small_connected_split(
        self,
        nodes: Set[int],
        seats_a: int,
        seats_b: int,
        target_pop: float,
    ) -> Optional[tuple[Set[int], Set[int]]]:
        if len(nodes) > 14:
            return None

        node_list = sorted(nodes)
        total_mask = (1 << len(node_list)) - 1
        best_split: Optional[tuple[float, Set[int], Set[int]]] = None

        for mask in range(1, total_mask):
            if seats_a == seats_b and (mask & 1) == 0:
                continue
            part_size = mask.bit_count()
            if part_size < seats_a or len(node_list) - part_size < seats_b:
                continue

            part_a = {
                node_list[idx]
                for idx in range(len(node_list))
                if mask & (1 << idx)
            }
            part_b = nodes - part_a
            if not self._nodes_connected(part_a) or not self._nodes_connected(part_b):
                continue

            pop_a = float(self.populations[list(part_a)].sum())
            population_error = abs(pop_a - target_pop) / target_pop if target_pop else 0.0
            compactness_penalty = 1.0 - min(
                self._compactness_for_nodes(part_a),
                self._compactness_for_nodes(part_b),
            )
            score = population_error + (0.02 * compactness_penalty)
            if best_split is None or score < best_split[0]:
                best_split = (score, part_a, part_b)

        if best_split is None:
            return None
        return best_split[1], best_split[2]

    def _grow_initial_plan(self, rng: random.Random) -> List[int]:
        seed_nodes = self._choose_seed_nodes(rng)
        assignment = [-1 for _ in range(len(self.gdf))]
        district_nodes: List[Set[int]] = [set() for _ in range(self.num_districts)]
        district_populations = [0.0 for _ in range(self.num_districts)]

        for district_id, node in enumerate(seed_nodes):
            assignment[node] = district_id
            district_nodes[district_id].add(node)
            district_populations[district_id] += self.populations[node]

        unassigned = set(range(len(self.gdf))) - set(seed_nodes)
        while unassigned:
            choice = self._choose_next_assignment(
                unassigned,
                district_nodes,
                district_populations,
                rng,
            )
            if choice is None:
                node = min(unassigned)
                district_id = min(
                    range(self.num_districts),
                    key=lambda idx: (district_populations[idx], idx),
                )
            else:
                district_id, node = choice

            assignment[node] = district_id
            district_nodes[district_id].add(node)
            district_populations[district_id] += self.populations[node]
            unassigned.remove(node)

        return assignment

    def _choose_seed_nodes(self, rng: random.Random) -> List[int]:
        coords = self._centroid_coordinates()
        first = rng.randrange(len(coords))
        selected = [first]

        while len(selected) < self.num_districts:
            best_node = None
            best_distance = -1.0
            for idx, coord in enumerate(coords):
                if idx in selected:
                    continue
                min_distance = min(
                    float(np.linalg.norm(coord - coords[chosen])) for chosen in selected
                )
                if min_distance > best_distance:
                    best_node = idx
                    best_distance = min_distance
            if best_node is None:
                break
            selected.append(best_node)
        return selected

    def _choose_next_assignment(
        self,
        unassigned: Set[int],
        district_nodes: List[Set[int]],
        district_populations: List[float],
        rng: random.Random,
    ) -> Optional[tuple[int, int]]:
        district_order = list(range(self.num_districts))
        rng.shuffle(district_order)
        district_order.sort(
            key=lambda idx: (
                district_populations[idx] / self.ideal_population
                if self.ideal_population
                else district_populations[idx],
                idx,
            )
        )

        for district_id in district_order:
            frontier = self._frontier(district_nodes[district_id], unassigned)
            if not frontier:
                continue
            node = min(
                frontier,
                key=lambda candidate: self._candidate_score(
                    district_id,
                    candidate,
                    district_nodes[district_id],
                    district_populations[district_id],
                ),
            )
            return district_id, node
        return None

    def _improve_by_boundary_moves(
        self,
        assignment: List[int],
        rng: random.Random,
    ) -> List[int]:
        best = list(assignment)
        best_score = self._assignment_score(best)

        for _ in range(self.local_search_passes):
            moved = False
            nodes = list(range(len(best)))
            rng.shuffle(nodes)

            for node in nodes:
                donor = best[node]
                neighbor_districts = {
                    best[neighbor]
                    for neighbor in self.adjacency[node]
                    if best[neighbor] != donor
                }
                for target in sorted(neighbor_districts):
                    candidate = list(best)
                    candidate[node] = target
                    if not self._assignment_is_contiguous(candidate, donor):
                        continue
                    if not self._assignment_is_contiguous(candidate, target):
                        continue
                    score = self._assignment_score(candidate)
                    if score + 1e-9 < best_score:
                        best = candidate
                        best_score = score
                        moved = True
                        break
                if moved:
                    break
            if not moved:
                break
        return best

    def _anneal_boundary_moves(
        self,
        assignment: List[int],
        rng: random.Random,
    ) -> tuple[List[int], Dict[str, object]]:
        stats: Dict[str, object] = {
            "planned_steps": 0,
            "attempted_moves": 0,
            "accepted_moves": 0,
            "accepted_worse_moves": 0,
            "phases": [],
        }
        if self.annealing_steps <= 0:
            return list(assignment), stats

        current = list(assignment)
        best = list(current)
        best_score = self._assignment_score(best)
        profiles = self._optimization_profiles()
        phase_steps = self._phase_steps(profiles)
        stats["planned_steps"] = int(sum(phase_steps))
        phase_stats: List[Dict[str, object]] = []

        for profile, steps in zip(profiles, phase_steps):
            current_score = self._assignment_score(current, profile)
            phase_result: Dict[str, object] = {
                "name": profile.name,
                "steps": int(steps),
                "accepted_moves": 0,
                "accepted_worse_moves": 0,
                "surface_tension_power": profile.surface_tension_power,
            }

            for step in range(steps):
                boundary_nodes = self._boundary_nodes(current)
                if not boundary_nodes:
                    break
                node = self._weighted_boundary_choice(
                    boundary_nodes,
                    current,
                    rng,
                    surface_tension_power=profile.surface_tension_power,
                )
                donor = current[node]
                targets = sorted(
                    {
                        current[neighbor]
                        for neighbor in self.adjacency[node]
                        if current[neighbor] != donor
                    }
                )
                rng.shuffle(targets)

                for target in targets:
                    candidate = list(current)
                    candidate[node] = target
                    if not self._assignment_is_contiguous(candidate, donor):
                        continue
                    if not self._assignment_is_contiguous(candidate, target):
                        continue

                    stats["attempted_moves"] = int(stats["attempted_moves"]) + 1
                    candidate_score = self._assignment_score(candidate, profile)
                    temperature = self._temperature(
                        step,
                        steps,
                        starting_temperature=profile.starting_temperature,
                    )
                    is_worse = candidate_score > current_score
                    if self._accept_annealed_move(
                        current_score,
                        candidate_score,
                        temperature,
                        rng,
                    ):
                        current = candidate
                        current_score = candidate_score
                        stats["accepted_moves"] = int(stats["accepted_moves"]) + 1
                        phase_result["accepted_moves"] = int(phase_result["accepted_moves"]) + 1
                        if is_worse:
                            stats["accepted_worse_moves"] = (
                                int(stats["accepted_worse_moves"]) + 1
                            )
                            phase_result["accepted_worse_moves"] = (
                                int(phase_result["accepted_worse_moves"]) + 1
                            )

                        objective_score = self._assignment_score(candidate)
                        if objective_score < best_score:
                            best = list(candidate)
                            best_score = objective_score
                        break

            phase_stats.append(phase_result)

        stats["phases"] = phase_stats
        return best, stats

    def _improve_by_pair_recom_splits(
        self,
        assignment: List[int],
        rng: random.Random,
    ) -> List[int]:
        """Rebalance adjacent district pairs with connected two-way splits."""
        best = list(assignment)
        best_score = self._assignment_score(best)

        for _ in range(self.recombination_passes):
            improved = False
            district_pairs = self._adjacent_district_pairs(best)
            rng.shuffle(district_pairs)

            for left_id, right_id in district_pairs:
                nodes = set(self._nodes_for_district(best, left_id))
                nodes.update(self._nodes_for_district(best, right_id))
                if len(nodes) < 2:
                    continue

                split = self._find_balanced_connected_split(nodes, 1, 1, rng)
                if split is None:
                    continue

                for left_nodes, right_nodes in (split, (split[1], split[0])):
                    candidate = list(best)
                    for node in left_nodes:
                        candidate[node] = left_id
                    for node in right_nodes:
                        candidate[node] = right_id

                    score = self._assignment_score(candidate)
                    if score + 1e-9 < best_score:
                        best = candidate
                        best_score = score
                        improved = True
                        break
                if improved:
                    break

            if not improved:
                break

        return best

    def _candidate_score(
        self,
        district_id: int,
        node: int,
        current_nodes: Set[int],
        current_population: float,
    ) -> float:
        candidate_nodes = current_nodes | {node}
        new_population = current_population + self.populations[node]
        population_score = (
            abs(new_population - self.ideal_population) / self.ideal_population
            if self.ideal_population
            else 0.0
        )
        compactness = self._compactness_for_nodes(candidate_nodes)
        partisan_score = self._partisan_penalty(candidate_nodes)
        coi_score = self._coi_penalty(district_id, node, current_nodes)

        return (
            self.population_equality_weight * 8.0 * population_score
            + self.compactness_weight * 0.35 * (1.0 - compactness)
            + self.partisan_weight * partisan_score
            + coi_score
        )

    def _assignment_score(
        self,
        assignment: Sequence[int],
        profile: Optional[OptimizationProfile] = None,
    ) -> float:
        profile = profile or FINAL_OBJECTIVE_PROFILE
        districts = self._districts_from_assignment(assignment, metric=True)
        deviations = []
        compactness_scores = []
        partisan_penalties = []
        noncontiguous_penalties = 0.0

        for district_id, district in enumerate(districts):
            population = float(district["P1_001N"].sum())
            deviation = (
                abs(population - self.ideal_population) / self.ideal_population
                if self.ideal_population
                else 0.0
            )
            deviations.append(deviation)
            compactness_scores.append(float(_polsby_popper_static(district)))
            partisan_penalties.append(self._partisan_penalty(self._nodes_for_district(assignment, district_id)))
            if not self._assignment_is_contiguous(assignment, district_id):
                noncontiguous_penalties += 1000.0

        population_score = max(deviations) if deviations else 0.0
        population_outlier_score = float(np.mean([dev ** 4 for dev in deviations])) if deviations else 0.0
        compactness_score = 1.0 - float(np.mean(compactness_scores)) if compactness_scores else 1.0
        centering_score = self._centering_score(assignment)
        partisan_score = float(np.mean(partisan_penalties)) if partisan_penalties else 0.0
        coi_score = self._coi_assignment_penalty(assignment)
        vra_score = self._vra_assignment_penalty(assignment)
        return (
            self.population_equality_weight
            * profile.population_multiplier
            * 10.0
            * population_score
            + self.population_equality_weight
            * profile.population_multiplier
            * 25.0
            * population_outlier_score
            + self.compactness_weight
            * profile.compactness_multiplier
            * 0.35
            * compactness_score
            + self.compactness_weight
            * profile.centering_multiplier
            * 0.15
            * centering_score
            + self.partisan_weight * profile.partisan_multiplier * partisan_score
            + coi_score
            + vra_score
            + noncontiguous_penalties
        )

    def _frontier(self, district_nodes: Set[int], unassigned: Set[int]) -> Set[int]:
        frontier: Set[int] = set()
        for node in district_nodes:
            frontier.update(self.adjacency[node] & unassigned)
        return frontier

    def _nodes_connected(self, nodes: Set[int]) -> bool:
        if len(nodes) <= 1:
            return True
        start = next(iter(nodes))
        seen = {start}
        stack = [start]
        while stack:
            current = stack.pop()
            for neighbor in self.adjacency[current] & nodes:
                if neighbor not in seen:
                    seen.add(neighbor)
                    stack.append(neighbor)
        return len(seen) == len(nodes)

    def _boundary_nodes(self, assignment: Sequence[int]) -> List[int]:
        return [
            node
            for node, district_id in enumerate(assignment)
            if any(assignment[neighbor] != district_id for neighbor in self.adjacency[node])
        ]

    def _weighted_boundary_choice(
        self,
        boundary_nodes: Sequence[int],
        assignment: Sequence[int],
        rng: random.Random,
        surface_tension_power: float = 2.0,
    ) -> int:
        weights = []
        for node in boundary_nodes:
            exposure = self._boundary_exposure(node, assignment)
            weights.append(1.0 + (float(exposure) ** max(0.0, surface_tension_power)))
        return rng.choices(list(boundary_nodes), weights=weights, k=1)[0]

    def _boundary_exposure(self, node: int, assignment: Sequence[int]) -> int:
        return sum(
            1
            for neighbor in self.adjacency[node]
            if assignment[neighbor] != assignment[node]
        )

    def _adjacent_district_pairs(self, assignment: Sequence[int]) -> List[tuple[int, int]]:
        pairs = set()
        for node, district_id in enumerate(assignment):
            for neighbor in self.adjacency[node]:
                neighbor_district = assignment[neighbor]
                if neighbor_district == district_id:
                    continue
                pairs.add(tuple(sorted((int(district_id), int(neighbor_district)))))
        return sorted(pairs)

    def _optimization_profiles(self) -> List[OptimizationProfile]:
        partisan_final_multiplier = 2.0 if self.partisan_weight > 0 else 1.0
        partisan_setup_multiplier = 0.45 if self.partisan_weight > 0 else 1.0
        return [
            OptimizationProfile(
                name="phase_one_population_centering",
                step_share=0.45,
                starting_temperature=0.05,
                population_multiplier=1.6,
                compactness_multiplier=0.8,
                centering_multiplier=1.6,
                partisan_multiplier=partisan_setup_multiplier,
                surface_tension_power=2.0,
            ),
            OptimizationProfile(
                name="phase_two_boundary_exploration",
                step_share=0.30,
                starting_temperature=0.16,
                population_multiplier=0.9,
                compactness_multiplier=0.15,
                centering_multiplier=0.6,
                partisan_multiplier=1.0,
                surface_tension_power=0.2,
            ),
            OptimizationProfile(
                name="phase_three_objective_polish",
                step_share=0.25,
                starting_temperature=0.06,
                population_multiplier=1.1,
                compactness_multiplier=1.2,
                centering_multiplier=1.0,
                partisan_multiplier=partisan_final_multiplier,
                surface_tension_power=2.0,
            ),
        ]

    def _phase_steps(self, profiles: Sequence[OptimizationProfile]) -> List[int]:
        total_steps = min(self.annealing_steps, max(40, len(self.gdf) * 8))
        remaining = total_steps
        allocations: List[int] = []
        for idx, profile in enumerate(profiles):
            phases_left = len(profiles) - idx - 1
            if idx == len(profiles) - 1:
                steps = remaining
            else:
                requested = max(1, int(round(total_steps * profile.step_share)))
                steps = min(remaining, requested)
                if remaining - steps < phases_left:
                    steps = max(0, remaining - phases_left)
            allocations.append(int(steps))
            remaining -= steps
        return allocations

    @staticmethod
    def _temperature(
        step: int,
        steps: int,
        starting_temperature: float = 0.08,
    ) -> float:
        if steps <= 1:
            return 0.001
        progress = step / (steps - 1)
        return max(0.001, starting_temperature * (1.0 - progress))

    @staticmethod
    def _accept_annealed_move(
        current_score: float,
        candidate_score: float,
        temperature: float,
        rng: random.Random,
    ) -> bool:
        if candidate_score <= current_score:
            return True
        if temperature <= 0:
            return False
        probability = math.exp((current_score - candidate_score) / temperature)
        return rng.random() < probability

    def _assignment_is_contiguous(self, assignment: Sequence[int], district_id: int) -> bool:
        nodes = self._nodes_for_district(assignment, district_id)
        if len(nodes) <= 1:
            return True
        seen = {nodes[0]}
        stack = [nodes[0]]
        node_set = set(nodes)
        while stack:
            current = stack.pop()
            for neighbor in self.adjacency[current] & node_set:
                if neighbor not in seen:
                    seen.add(neighbor)
                    stack.append(neighbor)
        return len(seen) == len(nodes)

    def _districts_from_assignment(
        self,
        assignment: Sequence[int],
        metric: bool = False,
    ) -> List[gpd.GeoDataFrame]:
        source = self.metric_gdf if metric else self.gdf
        return [
            source[[assigned == district_id for assigned in assignment]].copy()
            for district_id in range(self.num_districts)
        ]

    def _nodes_for_district(self, assignment: Sequence[int], district_id: int) -> List[int]:
        return [idx for idx, assigned in enumerate(assignment) if assigned == district_id]

    def _compactness_for_nodes(self, nodes: Iterable[int]) -> float:
        district = self.metric_gdf.iloc[list(nodes)]
        return float(_polsby_popper_static(district))

    def _centering_score(self, assignment: Sequence[int]) -> float:
        scores = []
        centroids = self.metric_gdf.geometry.centroid
        for district_id in range(self.num_districts):
            nodes = self._nodes_for_district(assignment, district_id)
            if not nodes:
                scores.append(1.0)
                continue
            weights = self.populations[nodes]
            total_weight = float(weights.sum())
            if total_weight <= 0:
                scores.append(0.0)
                continue
            xs = np.array([centroids.iloc[node].x for node in nodes])
            ys = np.array([centroids.iloc[node].y for node in nodes])
            center_x = float((xs * weights).sum() / total_weight)
            center_y = float((ys * weights).sum() / total_weight)
            distances = np.sqrt((xs - center_x) ** 2 + (ys - center_y) ** 2)
            district_geom = self.metric_gdf.iloc[nodes].geometry.union_all()
            scale = max(math.sqrt(district_geom.area), 1.0)
            scores.append(float((distances * weights).sum() / total_weight / scale))
        return float(np.mean(scores)) if scores else 0.0

    def _partisan_penalty(self, nodes: Iterable[int]) -> float:
        district = self.gdf.iloc[list(nodes)]
        share = _weighted_partisan_share(district)
        if self.target_party == 1:
            return abs(share - 0.55) if share >= 0.5 else (0.5 - share) + 0.25
        if self.target_party == 0:
            return abs(share - 0.45) if share <= 0.5 else (share - 0.5) + 0.25
        return abs(share - _weighted_partisan_share(self.gdf))

    def _coi_assignment_penalty(self, assignment: Sequence[int]) -> float:
        if not self.communities_of_interest:
            return 0.0

        coi_districts = {
            int(assignment[idx])
            for idx in range(len(assignment))
            if str(self.gdf.at[idx, "GEOID"]) in self.communities_of_interest
        }
        if len(coi_districts) <= 1:
            return 0.0
        return 50.0 * (len(coi_districts) - 1)

    def _vra_assignment_penalty(self, assignment: Sequence[int]) -> float:
        if not self.vra_compliance or "P1_003N" not in self.gdf.columns:
            return 0.0
        if self.statewide_minority_share <= 0.30:
            return 0.0

        threshold = max(0.45, self.statewide_minority_share)
        target_opportunity_districts = max(
            1,
            min(self.num_districts, round(self.statewide_minority_share * self.num_districts)),
        )
        district_shares = []
        for district_id in range(self.num_districts):
            nodes = self._nodes_for_district(assignment, district_id)
            pop = float(self.populations[nodes].sum()) if nodes else 0.0
            minority = float(self.minority_populations[nodes].sum()) if nodes else 0.0
            district_shares.append(minority / pop if pop > 0 else 0.0)

        opportunity_count = sum(share >= threshold for share in district_shares)
        shortfall = max(0, target_opportunity_districts - opportunity_count)
        if shortfall == 0:
            return 0.0

        best_shares = sorted(district_shares, reverse=True)[:target_opportunity_districts]
        share_gap = sum(max(0.0, threshold - share) for share in best_shares)
        return (2.5 * shortfall) + (4.0 * share_gap)

    def _minority_populations(self) -> np.ndarray:
        if "P1_003N" not in self.gdf.columns:
            return np.zeros(len(self.gdf), dtype=float)
        white_populations = pd.to_numeric(
            self.gdf["P1_003N"], errors="coerce"
        ).fillna(0).to_numpy(dtype=float)
        return np.maximum(self.populations - white_populations, 0.0)

    def _coi_penalty(self, district_id: int, node: int, current_nodes: Set[int]) -> float:
        if not self.communities_of_interest:
            return 0.0
        geoid = str(self.gdf.at[node, "GEOID"])
        if geoid not in self.communities_of_interest:
            return 0.0
        current_geoids = {str(self.gdf.at[idx, "GEOID"]) for idx in current_nodes}
        if current_geoids & self.communities_of_interest:
            return 0.0
        return 0.5 + (0.01 * district_id)

    def _centroid_coordinates(self) -> np.ndarray:
        centroids = self.metric_gdf.geometry.centroid
        return np.array([[geom.x, geom.y] for geom in centroids])

    @staticmethod
    def _metric_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if gdf.crs and gdf.crs.is_geographic:
            return gdf.to_crs(epsg=2163)
        return gdf.copy()

    @staticmethod
    def _clamp_weight(value: float) -> float:
        return max(0.0, min(1.0, float(value)))


def validate_contiguous_districts(districts: Iterable[gpd.GeoDataFrame]) -> bool:
    return all(is_contiguous(district) for district in districts)
