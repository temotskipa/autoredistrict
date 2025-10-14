from PyQt5.QtCore import QObject, pyqtSignal
from redistricting_algorithms import RedistrictingAlgorithm
import multiprocessing

class RedistrictingWorker(QObject):
    finished = pyqtSignal(object)
    progress = pyqtSignal(int)
    error = pyqtSignal(str)

    def __init__(self, state_data, num_districts, algorithm_name, population_equality_weight, compactness_weight, vra_compliance, communities_of_interest, use_gpu=False):
        super().__init__()
        self.state_data = state_data
        self.num_districts = num_districts
        self.algorithm_name = algorithm_name
        self.population_equality_weight = population_equality_weight
        self.compactness_weight = compactness_weight
        self.vra_compliance = vra_compliance
        self.communities_of_interest = communities_of_interest
        self.use_gpu = use_gpu

    def run(self):
        try:
            algorithm = RedistrictingAlgorithm(
                self.state_data,
                self.num_districts,
                population_equality_weight=self.population_equality_weight,
                compactness_weight=self.compactness_weight,
                vra_compliance=self.vra_compliance,
                communities_of_interest=self.communities_of_interest,
                use_gpu=self.use_gpu
            )
            algorithm.progress_update.connect(self.progress.emit)

            if "Divide and Conquer" in self.algorithm_name:
                districts_list = algorithm.divide_and_conquer()
            else:
                districts_list = algorithm.gerrymander()

            self.finished.emit(districts_list)
        except Exception as e:
            self.error.emit(str(e))
