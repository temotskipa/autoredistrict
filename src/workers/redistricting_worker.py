from typing import Callable, Optional

from ..core.redistricting_algorithms import RedistrictingAlgorithm


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
                        coi_list = coi_df[geoid_col].astype(str).str.zfill(15).tolist()
                except Exception:
                    coi_list = None
                algorithm = RedistrictingAlgorithm(
                    self.state_data,
                    self.num_districts,
                    population_equality_weight=self.population_equality_weight,
                    compactness_weight=self.compactness_weight,
                    vra_compliance=self.vra_compliance,
                    communities_of_interest=coi_list
                )
            algorithm.progress_update.connect(self._emit_progress)

            if "Divide and Conquer" in self.algorithm_name:
                districts_list = algorithm.divide_and_conquer()
            else:
                districts_list = algorithm.gerrymander()

            self._emit_finished(districts_list)
        except Exception as e:
            self._emit_error(str(e))
