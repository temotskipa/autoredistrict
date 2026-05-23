from src.core.graph_solver import GraphRedistrictingSolver
from src.core.utils import is_contiguous
from src.services.redistricting_service import build_demo_dataset


def test_graph_solver_is_deterministic_for_seed():
    data = build_demo_dataset(size=4)

    first = GraphRedistrictingSolver(data, 4, random_seed=42).solve()
    second = GraphRedistrictingSolver(data, 4, random_seed=42).solve()

    assert first.assignment == second.assignment
    assert first.metadata["solver"] == "graph_balanced_local_search"
    assert first.metadata["strategy"] in {"balanced_recursive", "greedy_growth"}
    assert first.metadata["random_seed"] == 42
    assert first.metadata["seed_schedule"][-1] == 0
    assert first.metadata["attempted_seed_count"] == first.metadata["attempts"] + 1
    assert first.metadata["annealing"]["boundary_weighting"] == (
        "more_exposed_boundary_units_flip_more_often"
    )
    assert first.metadata["recombination_passes"] == 4
    assert [phase["name"] for phase in first.metadata["annealing"]["phases"]] == [
        "phase_one_population_centering",
        "phase_two_boundary_exploration",
        "phase_three_objective_polish",
    ]
    assert "three_phase_weight_schedule" in first.metadata["video_informed_features"]
    assert "center_of_population_distance_metric" in first.metadata["video_informed_features"]
    assert "adjacent_pair_recom_rebalancing" in first.metadata["video_informed_features"]


def test_graph_solver_returns_contiguous_nonempty_districts():
    data = build_demo_dataset(size=5)

    result = GraphRedistrictingSolver(data, 5, random_seed=7).solve()

    assert len(result.districts) == 5
    assert all(not district.empty for district in result.districts)
    assert result.metadata["contiguity_enforced"] is True
    assert all(is_contiguous(district) for district in result.districts)
    assert all(len(district) > 0 for district in result.districts)


def test_pair_recom_improves_demo_population_balance():
    data = build_demo_dataset(size=4)

    baseline = GraphRedistrictingSolver(
        data,
        4,
        random_seed=0,
        recombination_passes=0,
    ).solve()
    improved = GraphRedistrictingSolver(data, 4, random_seed=0).solve()

    def max_deviation(result):
        total = sum(float(district["P1_001N"].sum()) for district in result.districts)
        ideal = total / len(result.districts)
        return max(
            abs(float(district["P1_001N"].sum()) - ideal) / ideal
            for district in result.districts
        )

    assert max_deviation(improved) < max_deviation(baseline)
    assert max_deviation(improved) <= 0.02
    assert all(is_contiguous(district) for district in improved.districts)


def test_vra_penalty_tracks_minority_opportunity_shortfall():
    data = build_demo_dataset(size=4, rich=True)
    solver = GraphRedistrictingSolver(data, 4, vra_compliance=True)

    packed_assignment = [0 for _ in range(len(data))]
    rows_assignment = [idx // 4 for idx in range(len(data))]

    assert solver._vra_assignment_penalty(packed_assignment) > 0
    assert solver._vra_assignment_penalty(rows_assignment) == 0

    result = solver.solve()

    assert result.metadata["vra_compliance_requested"] is True
    assert result.metadata["statewide_minority_share"] > 0.3
