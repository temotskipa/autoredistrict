import sys
import json
import us
import shutil
import glob
import os
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QCheckBox, QPushButton, QGraphicsView, QGraphicsScene, QSpinBox, QSlider, QLineEdit, QProgressBar
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
from redistricting_worker import RedistrictingWorker

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
        self.num_districts_spinbox.setEnabled(False)
        controls_layout.addWidget(self.num_districts_spinbox)

        # VRA compliance
        self.vra_checkbox = QCheckBox('Enable VRA Compliance')
        controls_layout.addWidget(self.vra_checkbox)

        # GPU acceleration
        self.gpu_checkbox = QCheckBox('Use GPU Acceleration')
        controls_layout.addWidget(self.gpu_checkbox)

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

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        controls_layout.addWidget(self.progress_bar)

        # Export buttons
        self.export_png_button = QPushButton('Export as PNG')
        self.export_png_button.setEnabled(False)
        self.export_png_button.clicked.connect(self.export_as_png)
        controls_layout.addWidget(self.export_png_button)

        self.export_shapefile_button = QPushButton('Export as Shapefile')
        self.export_shapefile_button.setEnabled(False)
        self.export_shapefile_button.clicked.connect(self.export_as_shapefile)
        controls_layout.addWidget(self.export_shapefile_button)

        # Clear Cache button
        self.clear_cache_button = QPushButton('Clear Cache')
        self.clear_cache_button.clicked.connect(self.clear_cache)
        controls_layout.addWidget(self.clear_cache_button)

        # Map display
        self.map_view = QGraphicsView()
        self.map_scene = QGraphicsScene()
        self.map_view.setScene(self.map_scene)

        # Add layouts to main layout
        main_layout.addLayout(controls_layout)
        main_layout.addWidget(self.map_view)

        self._load_api_key()

    def clear_cache(self):
        cache_dir = ".cache"
        try:
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
                QMessageBox.information(self, "Cache Cleared", f"The cache directory ({cache_dir}) has been cleared successfully.")
            else:
                QMessageBox.information(self, "Cache Cleared", "No cache directory found to clear.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred while clearing the cache: {e}")

    def _load_api_key(self):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                api_key = config.get('api_key')
                if api_key:
                    self.api_key_input.setText(api_key)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def _save_api_key(self):
        api_key = self.api_key_input.text()
        with open('config.json', 'w') as f:
            json.dump({'api_key': api_key}, f)

    def upload_coi_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Upload COI File", "", "CSV Files (*.csv)")
        if file_path:
            self.coi_file_path = file_path
            self.coi_file_label.setText(file_path.split('/')[-1])

    def run_apportionment_calculation(self):
        self._save_api_key()
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

        for state in us.states.STATES:
            if state.fips in self.apportionment:
                self.state_combo.addItem(state.name, userData=state.fips)

        self.state_combo.setEnabled(True)
        self.update_num_districts()

    def update_num_districts(self):
        self.num_districts_spinbox.setEnabled(True)
        if self.apportionment:
            state_fips = self.state_combo.currentData()
            if state_fips in self.apportionment:
                num_districts = self.apportionment[state_fips]
                self.num_districts_spinbox.setValue(num_districts)
                if num_districts == 1:
                    self.run_button.setEnabled(False)
                else:
                    self.run_button.setEnabled(True)

    def run_redistricting(self):
        self._save_api_key()
        state_fips = self.state_combo.currentData()
        api_key = self.api_key_input.text()

        use_gpu = self.gpu_checkbox.isChecked()

        # Disable UI controls
        self.api_key_input.setEnabled(False)
        self.house_size_spinbox.setEnabled(False)
        self.calculate_apportionment_button.setEnabled(False)
        self.state_combo.setEnabled(False)
        self.vra_checkbox.setEnabled(False)
        self.gpu_checkbox.setEnabled(False)
        self.pop_equality_slider.setEnabled(False)
        self.compactness_slider.setEnabled(False)
        self.coi_upload_button.setEnabled(False)
        self.algorithm_combo.setEnabled(False)
        self.run_button.setEnabled(False)
        self.export_png_button.setEnabled(False)
        self.export_shapefile_button.setEnabled(False)
        self.clear_cache_button.setEnabled(False)

        self.run_button.setText("Generating...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Fetching data... %p%")

        self.thread = QThread()
        self.worker = DataFetcherWorker(state_fips, api_key, use_gpu)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.fetch_data)
        self.worker.finished.connect(self.handle_data_fetched)
        self.worker.error.connect(self.handle_data_fetch_error)
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def handle_data_fetched(self, merged_gdf):
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Redistricting... %p%")
        self.run_button.setText("Redistricting...")

        use_gpu = self.gpu_checkbox.isChecked()

        # Add party affiliation data
        if hasattr(merged_gdf, 'to_pandas'): # Check if it's a cuDF DataFrame
            import cupy as cp
            cp.random.seed(0)
            merged_gdf['party'] = cp.random.rand(len(merged_gdf))
        else: # It's a pandas DataFrame
            np.random.seed(0)
            merged_gdf['party'] = np.random.rand(len(merged_gdf))

        self.redistricting_thread = QThread()
        self.redistricting_worker = RedistrictingWorker(
            state_data=merged_gdf,
            num_districts=self.num_districts_spinbox.value(),
            algorithm_name=self.algorithm_combo.currentText(),
            population_equality_weight=self.pop_equality_slider.value() / 100.0,
            compactness_weight=self.compactness_slider.value() / 100.0,
            vra_compliance=self.vra_checkbox.isChecked(),
            communities_of_interest=self.coi_file_path,
            use_gpu=use_gpu
        )
        self.redistricting_worker.moveToThread(self.redistricting_thread)

        self.redistricting_thread.started.connect(self.redistricting_worker.run)
        self.redistricting_worker.finished.connect(self.handle_redistricting_finished)
        self.redistricting_worker.error.connect(self.handle_redistricting_error)
        self.redistricting_worker.progress.connect(self.progress_bar.setValue)

        self.redistricting_worker.finished.connect(self.redistricting_thread.quit)
        self.redistricting_worker.finished.connect(self.redistricting_worker.deleteLater)
        self.redistricting_thread.finished.connect(self.redistricting_thread.deleteLater)

        self.redistricting_thread.start()

    def _re_enable_ui_controls(self):
        """Re-enables all UI controls after processing is finished or an error occurs."""
        self.api_key_input.setEnabled(True)
        self.house_size_spinbox.setEnabled(True)
        self.calculate_apportionment_button.setEnabled(True)
        self.state_combo.setEnabled(True)
        self.vra_checkbox.setEnabled(True)
        self.gpu_checkbox.setEnabled(True)
        self.pop_equality_slider.setEnabled(True)
        self.compactness_slider.setEnabled(True)
        self.coi_upload_button.setEnabled(True)
        self.algorithm_combo.setEnabled(True)
        self.run_button.setEnabled(True)
        # Note: Export buttons are not re-enabled here as they depend on a generated map.
        self.clear_cache_button.setEnabled(True)
        self.run_button.setText("Generate Map")
        self.progress_bar.setVisible(False)
        self.progress_bar.setFormat("")
        self.update_num_districts() # Re-evaluates if run_button should be enabled

    def handle_redistricting_finished(self, districts_list):
        if districts_list and hasattr(districts_list[0], 'to_pandas'):
            # Convert cuDF GeoDataFrames to GeoPandas DataFrames
            districts_list = [d.to_pandas() for d in districts_list]

        all_districts_gdf = gpd.GeoDataFrame()
        for i, district_gdf in enumerate(districts_list):
            district_gdf['district_id'] = i
            all_districts_gdf = pd.concat([all_districts_gdf, district_gdf])

        self.map_generator = MapGenerator(all_districts_gdf)
        map_image_path = self.map_generator.generate_map_image("temp_map.png")
        self.map_scene.clear()
        self.map_scene.addPixmap(QPixmap(map_image_path))

        self._re_enable_ui_controls()
        self.export_png_button.setEnabled(True)
        self.export_shapefile_button.setEnabled(True)

    def handle_redistricting_error(self, error_message):
        QMessageBox.critical(self, "Error", f"Failed to run redistricting: {error_message}")
        self._re_enable_ui_controls()

    def handle_data_fetch_error(self, error_message):
        QMessageBox.critical(self, "Error", f"Failed to fetch data: {error_message}")
        self._re_enable_ui_controls()

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
