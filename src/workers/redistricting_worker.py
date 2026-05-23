from typing import Callable, Optional

from ..core.graph_solver import GraphRedistrictingSolver


class RedistrictingWorker:
    def __init__(
        self,
        state_data,
        num_districts,
        algorithm_name,
        population_equality_weight,
        compactness_weight,
        vra_compliance,
        communities_of_interest,
        progress_callback: Optional[Callable[[int], None]] = None,
        finished_callback: Optional[Callable[[object], None]] = None,
        error_callback: Optional[Callable[[str], None]] = None,
    ):
        self.state_data = state_data
        self.num_districts = num_districts
        self.algorithm_name = algorithm_name
        self.population_equality_weight = population_equality_weight
        self.compactness_weight = compactness_weight
        self.vra_compliance = vra_compliance
        self.communities_of_interest = communities_of_interest
        self.progress_callback = progress_callback
        self.finished_callback = finished_callback
        self.error_callback = error_callback

    def _emit_progress(self, value: int):
        if self.progress_callback:
            try:
                self.progress_callback(int(value))
            except Exception:
                pass

    def _emit_finished(self, result):
        if self.finished_callback:
            try:
                self.finished_callback(result)
            except Exception:
                pass

    def _emit_error(self, message: str):
        if self.error_callback:
            try:
                self.error_callback(message)
            except Exception:
                pass

    def run(self):
        try:
            coi_list = None
            if self.communities_of_interest:
                try:
                    import pandas as pd
                    coi_df = pd.read_csv(self.communities_of_interest, dtype=str)
                    geoid_col = None
                    for candidate in ("GEOID", "geoid", "geoid20", "GEOID20"):
                        if candidate in coi_df.columns:
                            geoid_col = candidate
                            break
                    if geoid_col:
                        coi_list = (
                            coi_df[geoid_col]
                            .dropna()
                            .astype(str)
                            .str.strip()
                            .tolist()
                        )
                except Exception:
                    coi_list = None

            partisan_weight = 0.0
            target_party = None
            if "Democrat" in self.algorithm_name:
                target_party = 1
                partisan_weight = 1.0
            elif "Republican" in self.algorithm_name:
                target_party = 0
                partisan_weight = 1.0
            elif "Gerrymander" in self.algorithm_name:
                partisan_weight = 1.0

            self._emit_progress(5)
            solver = GraphRedistrictingSolver(
                self.state_data,
                self.num_districts,
                population_equality_weight=self.population_equality_weight,
                compactness_weight=self.compactness_weight,
                partisan_weight=partisan_weight,
                vra_compliance=self.vra_compliance,
                communities_of_interest=coi_list,
                target_party=target_party,
                random_seed=0,
            )
            result = solver.solve()

            self._emit_progress(100)
            self._emit_finished(result.districts)
        except Exception as e:
            self._emit_error(str(e))
