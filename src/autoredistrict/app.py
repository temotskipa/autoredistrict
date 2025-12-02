import sys
import json
import us
import shutil
import os
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QCheckBox, QPushButton, QGraphicsView, QGraphicsScene, QSpinBox, QSlider, QLineEdit, QProgressBar, QGroupBox
from PyQt5.QtCore import Qt, QThread
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QFileDialog, QMessageBox
import geopandas as gpd
import pandas as pd
from .rendering.map_generator import MapGenerator
from .data.data_fetcher import DataFetcher
from .core.redistricting_algorithms import RedistrictingAlgorithm
from .core.apportionment import calculate_apportionment
from .workers.data_worker import DataFetcherWorker
from .data.partisan_providers import (
    AVAILABLE_PARTISAN_YEARS,
    provider_chain_for_state,
    available_manual_providers,
)
from .workers.redistricting_worker import RedistrictingWorker

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

        # GitHub token
        github_layout = QHBoxLayout()
        github_layout.addWidget(QLabel('GitHub Token:'))
        self.github_token_input = QLineEdit()
        self.github_token_input.setPlaceholderText('Optional – used for provider discovery')
        github_layout.addWidget(self.github_token_input)
        controls_layout.addLayout(github_layout)

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

        # Election year selection for partisan data
        controls_layout.addWidget(QLabel('Election Year (Partisan Data):'))
        self.election_year_combo = QComboBox()
        for year in AVAILABLE_PARTISAN_YEARS:
            self.election_year_combo.addItem(str(year), userData=year)
        self.election_year_combo.setCurrentIndex(len(AVAILABLE_PARTISAN_YEARS) - 1)
        self.election_year_combo.currentIndexChanged.connect(self._handle_election_year_changed)
        controls_layout.addWidget(self.election_year_combo)

        # Data quality summary
        self.data_quality_group = QGroupBox('Data Quality')
        dq_layout = QVBoxLayout()
        self.data_resolution_label = QLabel('Resolution: —')
        self.data_recency_label = QLabel('Recency: —')
        self.data_confidence_label = QLabel('Confidence: —')
        self.data_contest_label = QLabel('Contest: —')
        self.data_source_status_label = QLabel('Source: —')
        dq_layout.addWidget(self.data_resolution_label)
        dq_layout.addWidget(self.data_recency_label)
        dq_layout.addWidget(self.data_confidence_label)
        dq_layout.addWidget(self.data_contest_label)
        dq_layout.addWidget(self.data_source_status_label)
        self.data_details_button = QPushButton('View Details')
        self.data_details_button.clicked.connect(self._show_data_details)
        dq_layout.addWidget(self.data_details_button)
        self.data_quality_group.setLayout(dq_layout)
        controls_layout.addWidget(self.data_quality_group)

        # Manual override controls
        self.manual_override_checkbox = QCheckBox('Manual data source override')
        self.manual_override_checkbox.toggled.connect(self._handle_manual_override_toggled)
        controls_layout.addWidget(self.manual_override_checkbox)

        self.partisan_provider_combo = QComboBox()
        self.partisan_provider_combo.currentIndexChanged.connect(self._handle_manual_provider_changed)
        self.partisan_provider_combo.setVisible(False)
        controls_layout.addWidget(self.partisan_provider_combo)

        # Run button
        self.run_button = QPushButton('Generate Map')
        self.run_button.setEnabled(False)
        self.run_button.clicked.connect(self.run_redistricting)
        controls_layout.addWidget(self.run_button)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        controls_layout.addWidget(self.progress_bar)

        # Fast mode (tract-level) toggle
        self.fast_mode_checkbox = QCheckBox('Fast mode (tract-level data)')
        self.fast_mode_checkbox.setToolTip('Use tract-level Census + tract shapefiles for faster runs (lower resolution).')
        controls_layout.addWidget(self.fast_mode_checkbox)

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
        self.manual_provider_key = None
        self.current_provider_chain = []
        self.last_applied_provider_meta = None
        self.provider_details_text = ""
        self._refresh_provider_chain()
        self._auto_apportion_on_start()

    def _auto_apportion_on_start(self):
        api_key = self.api_key_input.text()
        if api_key:
            try:
                self.run_apportionment_calculation()
            except Exception:
                pass

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
                github_token = config.get('github_token')
                if github_token:
                    self.github_token_input.setText(github_token)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def _save_api_key(self):
        api_key = self.api_key_input.text()
        github_token = self.github_token_input.text()
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            config = {}
        config['api_key'] = api_key
        if github_token:
            config['github_token'] = github_token
        elif 'github_token' in config:
            del config['github_token']
        with open('config.json', 'w') as f:
            json.dump(config, f)

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
        self._refresh_provider_chain()

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
        self._refresh_provider_chain()

    def _refresh_provider_chain(self):
        state_fips = self.state_combo.currentData()
        manual_key = self.manual_provider_key if self.manual_override_checkbox.isChecked() else None
        requested_year = self.election_year_combo.currentData()
        if self.manual_override_checkbox.isChecked():
            available = available_manual_providers(state_fips, requested_year)
            self._populate_manual_provider_combo(available)
            available_keys = [meta.key for meta in available]
            if not available_keys:
                QMessageBox.warning(self, "No Providers", "No manual providers are available for this state. Reverting to automatic selection.")
                self.manual_override_checkbox.blockSignals(True)
                self.manual_override_checkbox.setChecked(False)
                self.manual_override_checkbox.blockSignals(False)
                self.manual_provider_key = None
                manual_key = None
            elif manual_key not in available_keys:
                self.manual_provider_key = available_keys[0]
                self.partisan_provider_combo.blockSignals(True)
                self.partisan_provider_combo.setCurrentIndex(0)
                self.partisan_provider_combo.blockSignals(False)
                manual_key = self.manual_provider_key
        else:
            # keep combo populated for reference but hidden
            available = available_manual_providers(state_fips, requested_year)
            self._populate_manual_provider_combo(available)

        chain = provider_chain_for_state(state_fips, requested_year, manual_key)
        self.current_provider_chain = chain
        active_meta = chain[0] if chain else None
        self._update_data_quality_panel(active_meta)
        self._update_election_year_control()

    def _populate_manual_provider_combo(self, providers):
        self.partisan_provider_combo.blockSignals(True)
        current_key = self.manual_provider_key
        self.partisan_provider_combo.clear()
        for meta in providers:
            self.partisan_provider_combo.addItem(meta.label, userData=meta.key)
        if current_key:
            index = self.partisan_provider_combo.findData(current_key)
            if index != -1:
                self.partisan_provider_combo.setCurrentIndex(index)
        self.partisan_provider_combo.blockSignals(False)

    def _update_data_quality_panel(self, metadata, actual=False):
        if metadata is None:
            self.data_resolution_label.setText("Resolution: —")
            self.data_recency_label.setText("Recency: —")
            self.data_confidence_label.setText("Confidence: —")
            self.data_contest_label.setText("Contest: —")
            self.data_source_status_label.setText("Source: —")
            self.provider_details_text = "No data sources configured."
            return
        resolution_text = metadata.granularity.capitalize()
        self.data_resolution_label.setText(f"Resolution: {resolution_text}")
        if metadata.supports_year_selection:
            year = self.election_year_combo.currentData()
            recency = f"{year} general election"
        else:
            recency = metadata.recency_note or "Latest certified data"
        self.data_recency_label.setText(f"Recency: {recency}")
        self.data_confidence_label.setText(f"Confidence: {metadata.confidence}")
        self.data_contest_label.setText(f"Contest: {metadata.label}")
        status_prefix = "In use" if actual else "Planned"
        self.data_source_status_label.setText(f"{status_prefix}: {metadata.label}")
        details = []
        for idx, meta in enumerate(self.current_provider_chain, start=1):
            role = "Active" if actual and meta.key == metadata.key else ("Primary" if idx == 1 else "Fallback")
            details.append(f"{idx}. {meta.label} – {meta.granularity.capitalize()} • {meta.confidence} ({role})\n   {meta.description}")
        self.provider_details_text = "\n\n".join(details) if details else "No providers available."

    def _show_data_details(self):
        QMessageBox.information(self, "Partisan Data Sources", self.provider_details_text or "No provider information available yet.")

    def _handle_manual_override_toggled(self, checked):
        self.partisan_provider_combo.setVisible(checked)
        self.partisan_provider_combo.setEnabled(checked)
        if not checked:
            self.manual_provider_key = None
        else:
            state_fips = self.state_combo.currentData()
            requested_year = self.election_year_combo.currentData()
            available = available_manual_providers(state_fips, requested_year)
            if not available:
                QMessageBox.warning(self, "No Providers", "No manual providers are available for this state.")
                self.manual_override_checkbox.blockSignals(True)
                self.manual_override_checkbox.setChecked(False)
                self.manual_override_checkbox.blockSignals(False)
                self.partisan_provider_combo.setVisible(False)
                return
            self.manual_provider_key = available[0].key if self.manual_provider_key not in [meta.key for meta in available] else self.manual_provider_key
            self._populate_manual_provider_combo(available)
            index = self.partisan_provider_combo.findData(self.manual_provider_key)
            if index != -1:
                self.partisan_provider_combo.setCurrentIndex(index)
        self._refresh_provider_chain()

    def _handle_manual_provider_changed(self, _index):
        if not self.manual_override_checkbox.isChecked():
            return
        self.manual_provider_key = self.partisan_provider_combo.currentData()
        self._refresh_provider_chain()

    def _update_election_year_control(self):
        active_meta = self.current_provider_chain[0] if self.current_provider_chain else None
        allow = bool(active_meta and active_meta.supports_year_selection and self.state_combo.isEnabled())
        self.election_year_combo.setEnabled(allow)

    def _handle_election_year_changed(self, _index):
        self._refresh_provider_chain()

    def run_redistricting(self):
        self._save_api_key()
        state_fips = self.state_combo.currentData()
        api_key = self.api_key_input.text()

        # Disable UI controls
        self.api_key_input.setEnabled(False)
        self.house_size_spinbox.setEnabled(False)
        self.calculate_apportionment_button.setEnabled(False)
        self.state_combo.setEnabled(False)
        self.vra_checkbox.setEnabled(False)
        self.pop_equality_slider.setEnabled(False)
        self.compactness_slider.setEnabled(False)
        self.coi_upload_button.setEnabled(False)
        self.algorithm_combo.setEnabled(False)
        self.manual_override_checkbox.setEnabled(False)
        self.partisan_provider_combo.setEnabled(False)
        self.election_year_combo.setEnabled(False)
        self.run_button.setEnabled(False)
        self.export_png_button.setEnabled(False)
        self.export_shapefile_button.setEnabled(False)
        self.clear_cache_button.setEnabled(False)

        self.run_button.setText("Generating...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Fetching data... %p%")

        self.thread = QThread()
        provider_keys = [meta.key for meta in self.current_provider_chain] or ["county_presidential"]
        active_meta = self.current_provider_chain[0] if self.current_provider_chain else None
        election_year = self.election_year_combo.currentData() if active_meta and active_meta.supports_year_selection else None
        resolution = "tract" if self.fast_mode_checkbox.isChecked() else "block"
        self.worker = DataFetcherWorker(
            state_fips,
            api_key,
            election_year=election_year,
            provider_keys=provider_keys,
            resolution=resolution
        )
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.fetch_data)
        self.worker.finished.connect(self.handle_data_fetched)
        self.worker.error.connect(self.handle_data_fetch_error)
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def handle_data_fetched(self, census_df, shapefile_path):
        actual_meta = getattr(self.worker, "active_provider_meta", None)
        if actual_meta:
            self.last_applied_provider_meta = actual_meta
            self._update_data_quality_panel(actual_meta, actual=True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Redistricting... %p%")
        self.run_button.setText("Redistricting...")

        state_gdf = gpd.read_file(shapefile_path)
        state_gdf['GEOID'] = state_gdf['GEOID20']
        merged_gdf = state_gdf.merge(census_df, on='GEOID')
        if 'partisan_score' not in merged_gdf.columns:
            merged_gdf['partisan_score'] = 0.5
        merged_gdf['partisan_score'] = pd.to_numeric(
            merged_gdf['partisan_score'], errors='coerce'
        )
        fallback = merged_gdf['partisan_score'].mean()
        if pd.isna(fallback):
            fallback = 0.5
        merged_gdf['partisan_score'] = merged_gdf['partisan_score'].fillna(fallback)

        self.redistricting_thread = QThread()
        self.redistricting_worker = RedistrictingWorker(
            state_data=merged_gdf,
            num_districts=self.num_districts_spinbox.value(),
            algorithm_name=self.algorithm_combo.currentText(),
            population_equality_weight=self.pop_equality_slider.value() / 100.0,
            compactness_weight=self.compactness_slider.value() / 100.0,
            vra_compliance=self.vra_checkbox.isChecked(),
            communities_of_interest=self.coi_file_path
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
        self.pop_equality_slider.setEnabled(True)
        self.compactness_slider.setEnabled(True)
        self.coi_upload_button.setEnabled(True)
        self.algorithm_combo.setEnabled(True)
        self.manual_override_checkbox.setEnabled(True)
        self.partisan_provider_combo.setEnabled(self.manual_override_checkbox.isChecked())
        self._refresh_provider_chain()
        self.run_button.setEnabled(True)
        # Note: Export buttons are not re-enabled here as they depend on a generated map.
        self.clear_cache_button.setEnabled(True)
        self.run_button.setText("Generate Map")
        self.progress_bar.setVisible(False)
        self.progress_bar.setFormat("")
        self.update_num_districts() # Re-evaluates if run_button should be enabled

    def handle_redistricting_finished(self, districts_list):
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
