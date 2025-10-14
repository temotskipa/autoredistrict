import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QCheckBox, QPushButton, QGraphicsView, QGraphicsScene, QSpinBox, QSlider, QLineEdit
from PyQt5.QtCore import Qt, QThread
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QFileDialog, QMessageBox
import geopandas as gpd
import pandas as pd
import numpy as np
from map_generator import MapGenerator
from data_fetcher import DataFetcher
from redistricting_algorithms import RedistrictingAlgorithm
from apportionment import calculate_apportionment
from worker import DataFetcherWorker

class MainWindow(QMainWindow):
    def __init__(self):
        self.map_generator = None
        self.apportionment = None
        self.coi_file_path = None
        super().__init__()
        self.setWindowTitle('Congressional Redistricting')

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QHBoxLayout()
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

        # Controls layout
        controls_layout = QVBoxLayout()

        # API Key
        api_key_layout = QHBoxLayout()
        api_key_layout.addWidget(QLabel('Census API Key:'))
        self.api_key_input = QLineEdit()
        api_key_layout.addWidget(self.api_key_input)
        controls_layout.addLayout(api_key_layout)

        # House size
        house_size_layout = QHBoxLayout()
        house_size_layout.addWidget(QLabel('House Size:'))
        self.house_size_spinbox = QSpinBox()
        self.house_size_spinbox.setMinimum(50)
        self.house_size_spinbox.setMaximum(1000)
        self.house_size_spinbox.setValue(435)
        house_size_layout.addWidget(self.house_size_spinbox)
        self.calculate_apportionment_button = QPushButton('Calculate Apportionment')
        self.calculate_apportionment_button.clicked.connect(self.run_apportionment_calculation)
        house_size_layout.addWidget(self.calculate_apportionment_button)
        controls_layout.addLayout(house_size_layout)

        # State selection
        controls_layout.addWidget(QLabel('Select State:'))
        self.state_combo = QComboBox()
        self.state_combo.setEnabled(False)
        self.state_combo.currentIndexChanged.connect(self.update_num_districts)
        controls_layout.addWidget(self.state_combo)

        # Number of districts
        controls_layout.addWidget(QLabel('Number of Districts:'))
        self.num_districts_spinbox = QSpinBox()
        self.num_districts_spinbox.setMinimum(1)
        self.num_districts_spinbox.setValue(1)
        self.num_districts_spinbox.setReadOnly(True)
        controls_layout.addWidget(self.num_districts_spinbox)

        # VRA compliance
        self.vra_checkbox = QCheckBox('Enable VRA Compliance')
        controls_layout.addWidget(self.vra_checkbox)

        # Population equality weight
        controls_layout.addWidget(QLabel('Population Equality Weight:'))
        self.pop_equality_slider = QSlider(Qt.Horizontal)
        self.pop_equality_slider.setMinimum(0)
        self.pop_equality_slider.setMaximum(100)
        self.pop_equality_slider.setValue(100)
        controls_layout.addWidget(self.pop_equality_slider)

        # Compactness weight
        controls_layout.addWidget(QLabel('Compactness Weight:'))
        self.compactness_slider = QSlider(Qt.Horizontal)
        self.compactness_slider.setMinimum(0)
        self.compactness_slider.setMaximum(100)
        self.compactness_slider.setValue(100)
        controls_layout.addWidget(self.compactness_slider)

        # Community of Interest
        coi_layout = QHBoxLayout()
        self.coi_upload_button = QPushButton('Upload COI File')
        self.coi_upload_button.clicked.connect(self.upload_coi_file)
        coi_layout.addWidget(self.coi_upload_button)
        self.coi_file_label = QLabel('No file uploaded.')
        coi_layout.addWidget(self.coi_file_label)
        controls_layout.addLayout(coi_layout)

        # Algorithm selection
        controls_layout.addWidget(QLabel('Select Algorithm:'))
        self.algorithm_combo = QComboBox()
        self.algorithm_combo.addItem("Divide and Conquer (Fair)")
        self.algorithm_combo.addItem("Gerrymander (Packed)")
        controls_layout.addWidget(self.algorithm_combo)

        # Run button
        self.run_button = QPushButton('Generate Map')
        self.run_button.setEnabled(False)
        self.run_button.clicked.connect(self.run_redistricting)
        controls_layout.addWidget(self.run_button)

        # Export buttons
        self.export_png_button = QPushButton('Export as PNG')
        self.export_png_button.clicked.connect(self.export_as_png)
        controls_layout.addWidget(self.export_png_button)

        self.export_shapefile_button = QPushButton('Export as Shapefile')
        self.export_shapefile_button.clicked.connect(self.export_as_shapefile)
        controls_layout.addWidget(self.export_shapefile_button)

        # Map display
        self.map_view = QGraphicsView()
        self.map_scene = QGraphicsScene()
        self.map_view.setScene(self.map_scene)

        # Add layouts to main layout
        main_layout.addLayout(controls_layout)
        main_layout.addWidget(self.map_view)

    def upload_coi_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Upload COI File", "", "CSV Files (*.csv)")
        if file_path:
            self.coi_file_path = file_path
            self.coi_file_label.setText(file_path.split('/')[-1])

    def run_apportionment_calculation(self):
        api_key = self.api_key_input.text()
        fetcher = DataFetcher(api_key)
        state_populations = fetcher.get_all_states_population_data()

        if not state_populations:
            QMessageBox.critical(self, "Error", "Failed to fetch population data. Please check the console for details.")
            return

        house_size = self.house_size_spinbox.value()
        self.apportionment = calculate_apportionment(state_populations, house_size)

        # Populate the state dropdown
        self.state_combo.clear()

        states = {
            "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas", "06": "California",
            "08": "Colorado", "09": "Connecticut", "10": "Delaware", "11": "District of Columbia",
            "12": "Florida", "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
            "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky", "22": "Louisiana",
            "23": "Maine", "24": "Maryland", "25": "Massachusetts", "26": "Michigan",
            "27": "Minnesota", "28": "Mississippi", "29": "Missouri", "30": "Montana",
            "31": "Nebraska", "32": "Nevada", "33": "New Hampshire", "34": "New Jersey",
            "35": "New Mexico", "36": "New York", "37": "North Carolina", "38": "North Dakota",
            "39": "Ohio", "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
            "44": "Rhode Island", "45": "South Carolina", "46": "South Dakota", "47": "Tennessee",
            "48": "Texas", "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
            "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming"
        }

        for fips, name in states.items():
            if fips in self.apportionment:
                self.state_combo.addItem(name, userData=fips)

        self.state_combo.setEnabled(True)
        self.run_button.setEnabled(True)
        self.update_num_districts()

    def update_num_districts(self):
        if self.apportionment:
            state_fips = self.state_combo.currentData()
            if state_fips in self.apportionment:
                num_districts = self.apportionment[state_fips]
                self.num_districts_spinbox.setValue(num_districts)

    def run_redistricting(self):
        state_fips = self.state_combo.currentData()
        api_key = self.api_key_input.text()
        self.run_button.setEnabled(False)
        self.run_button.setText("Generating...")

        self.thread = QThread()
        self.worker = DataFetcherWorker(state_fips, api_key)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.fetch_data)
        self.worker.finished.connect(self.handle_data_fetched)
        self.worker.error.connect(self.handle_data_fetch_error)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def handle_data_fetched(self, census_df, shapefile_path):
        state_fips = self.state_combo.currentData()
        num_districts = self.num_districts_spinbox.value()
        vra_compliance = self.vra_checkbox.isChecked()
        algorithm_name = self.algorithm_combo.currentText()

        state_gdf = gpd.read_file(shapefile_path)
        state_gdf['GEOID'] = state_gdf['GEOID20']
        merged_gdf = state_gdf.merge(census_df, on='GEOID')

        np.random.seed(0)
        merged_gdf['party'] = np.random.rand(len(merged_gdf))

        pop_equality_weight = self.pop_equality_slider.value() / 100.0
        compactness_weight = self.compactness_slider.value() / 100.0

        coi_data = None
        if self.coi_file_path:
            coi_df = pd.read_csv(self.coi_file_path)
            coi_data = list(coi_df['GEOID'])

        algorithm = RedistrictingAlgorithm(merged_gdf, num_districts,
                                           population_equality_weight=pop_equality_weight,
                                           compactness_weight=compactness_weight,
                                           vra_compliance=vra_compliance,
                                           communities_of_interest=coi_data)

        if "Divide and Conquer" in algorithm_name:
            districts_list = algorithm.divide_and_conquer()
        else:
            districts_list = algorithm.gerrymander()

        all_districts_gdf = gpd.GeoDataFrame()
        for i, district_gdf in enumerate(districts_list):
            district_gdf['district_id'] = i
            all_districts_gdf = all_districts_gdf.append(district_gdf)

        self.map_generator = MapGenerator(all_districts_gdf)
        map_image_path = self.map_generator.generate_map_image("temp_map.png")
        self.map_scene.clear()
        self.map_scene.addPixmap(QPixmap(map_image_path))

        self.run_button.setEnabled(True)
        self.run_button.setText("Generate Map")

    def handle_data_fetch_error(self, error_message):
        QMessageBox.critical(self, "Error", f"Failed to fetch data: {error_message}")
        self.run_button.setEnabled(True)
        self.run_button.setText("Generate Map")

    def export_as_png(self):
        if self.map_generator:
            file_path, _ = QFileDialog.getSaveFileName(self, "Save Map as PNG", "", "PNG Files (*.png)")
            if file_path:
                self.map_generator.generate_map_image(file_path)
                print(f"Map saved to {file_path}")

    def export_as_shapefile(self):
        if self.map_generator:
            file_path, _ = QFileDialog.getSaveFileName(self, "Save Districts as Shapefile", "", "Shapefiles (*.shp)")
            if file_path:
                self.map_generator.export_to_shapefile(file_path)
                print(f"Districts saved to {file_path}")

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
