import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QCheckBox, QPushButton, QGraphicsView, QGraphicsScene, QSpinBox, QSlider
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QFileDialog, QMessageBox
import geopandas as gpd
import pandas as pd
import numpy as np
from map_generator import MapGenerator
from data_fetcher import DataFetcher
from redistricting_algorithms import RedistrictingAlgorithm

class MainWindow(QMainWindow):
    def __init__(self):
        self.map_generator = None
        super().__init__()
        self.setWindowTitle('Congressional Redistricting')

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QHBoxLayout()
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

        # Controls layout
        controls_layout = QVBoxLayout()

        # State selection
        controls_layout.addWidget(QLabel('Select State:'))
        self.state_combo = QComboBox()
        # In a real application, you would populate this with a list of states and their FIPS codes
        self.state_combo.addItem("California", userData="06")
        self.state_combo.addItem("Texas", userData="48")
        self.state_combo.addItem("Florida", userData="12")
        self.state_combo.addItem("New York", userData="36")
        controls_layout.addWidget(self.state_combo)

        # Number of districts
        controls_layout.addWidget(QLabel('Number of Districts:'))
        self.num_districts_spinbox = QSpinBox()
        self.num_districts_spinbox.setMinimum(1)
        self.num_districts_spinbox.setValue(10) # Default value
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

        # Algorithm selection
        controls_layout.addWidget(QLabel('Select Algorithm:'))
        self.algorithm_combo = QComboBox()
        self.algorithm_combo.addItem("Divide and Conquer (Fair)")
        self.algorithm_combo.addItem("Gerrymander (Packed)")
        controls_layout.addWidget(self.algorithm_combo)

        # Run button
        self.run_button = QPushButton('Generate Map')
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

    def run_redistricting(self):
        # Get parameters from the GUI
        state_fips = self.state_combo.currentData()
        num_districts = self.num_districts_spinbox.value()
        vra_compliance = self.vra_checkbox.isChecked()
        algorithm_name = self.algorithm_combo.currentText()

        # Fetch the data
        fetcher = DataFetcher()
        census_data = fetcher.get_census_data(state_fips)
        shapefile_path = fetcher.get_shapefiles(state_fips)

        if not census_data or not shapefile_path:
            QMessageBox.critical(self, "Error", "Failed to fetch data. Please check the console for details.")
            return

        # Load the shapefile and census data
        state_gdf = gpd.read_file(shapefile_path)
        census_df = pd.DataFrame(census_data[1:], columns=census_data[0])

        # Merge the data
        state_gdf['GEOID'] = state_gdf['GEOID20']
        merged_gdf = state_gdf.merge(census_df, on='GEOID')

        # Add placeholder party data for gerrymandering demonstration
        np.random.seed(0)
        merged_gdf['party'] = np.random.rand(len(merged_gdf))

        # Get weights from sliders
        pop_equality_weight = self.pop_equality_slider.value() / 100.0
        compactness_weight = self.compactness_slider.value() / 100.0

        # Run the selected algorithm
        algorithm = RedistrictingAlgorithm(merged_gdf, num_districts,
                                           population_equality_weight=pop_equality_weight,
                                           compactness_weight=compactness_weight,
                                           vra_compliance=vra_compliance)

        if "Divide and Conquer" in algorithm_name:
            districts_list = algorithm.divide_and_conquer()
        else:
            districts_list = algorithm.gerrymander()

        # Combine the districts into a single GeoDataFrame for visualization
        all_districts_gdf = gpd.GeoDataFrame()
        for i, district_gdf in enumerate(districts_list):
            district_gdf['district_id'] = i
            all_districts_gdf = all_districts_gdf.append(district_gdf)

        # Generate and display the map
        self.map_generator = MapGenerator(all_districts_gdf)
        map_image_path = self.map_generator.generate_map_image("temp_map.png")
        self.map_scene.clear()
        self.map_scene.addPixmap(QPixmap(map_image_path))

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
