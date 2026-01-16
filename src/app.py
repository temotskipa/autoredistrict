import json
import os
import shutil
import threading
import tkinter as tk
from typing import Dict, Optional

import geopandas as gpd
import pandas as pd
import ttkbootstrap as tb
import us
from tkinter import filedialog, messagebox

from .core.apportionment import calculate_apportionment
from .data.data_fetcher import DataFetcher
from .data.partisan_providers import (
    AVAILABLE_PARTISAN_YEARS,
    available_manual_providers,
    provider_chain_for_state,
)
from .rendering.map_generator import MapGenerator
from .workers.data_worker import DataFetcherWorker
from .workers.redistricting_worker import RedistrictingWorker


class MainWindow(tb.Window):
    def __init__(self):
        super().__init__(themename="flatly")
        self.title("Congressional Redistricting")
        self.geometry("1280x820")

        # state
        self.map_generator: Optional[MapGenerator] = None
        self.apportionment: Optional[Dict[str, int]] = None
        self.coi_file_path: Optional[str] = None
        self.manual_provider_key: Optional[str] = None
        self.current_provider_chain = []
        self.last_applied_provider_meta = None
        self.provider_details_text = ""
        self.state_fips_by_name: Dict[str, str] = {}
        self._map_photo = None

        # Tk variables
        self.api_key_var = tb.StringVar()
        self.github_token_var = tb.StringVar()
        self.house_size_var = tb.IntVar(value=435)
        self.num_districts_var = tb.IntVar(value=1)
        self.vra_var = tb.BooleanVar(value=False)
        self.fast_mode_var = tb.BooleanVar(value=False)
        self.manual_override_var = tb.BooleanVar(value=False)
        self.pop_weight_var = tb.IntVar(value=100)
        self.compactness_var = tb.IntVar(value=100)
        self.election_year_var = tb.StringVar()
        self.algorithm_var = tb.StringVar(value="Divide and Conquer (Fair)")
        self.progress_value = tb.IntVar(value=0)
        self.progress_text = tb.StringVar(value="")
        self.coi_label_var = tb.StringVar(value="No file uploaded.")
        self.data_resolution_var = tb.StringVar(value="Resolution: -")
        self.data_recency_var = tb.StringVar(value="Recency: -")
        self.data_confidence_var = tb.StringVar(value="Confidence: -")
        self.data_contest_var = tb.StringVar(value="Contest: -")
        self.data_source_status_var = tb.StringVar(value="Source: -")

        self._build_ui()
        self._load_api_key()
        self._refresh_provider_chain()
        self._auto_apportion_on_start()

    # ------------------------- UI BUILD ------------------------- #
    def _build_ui(self):
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        main = tb.Frame(self, padding=10)
        main.grid(row=0, column=0, sticky="nsew")
        main.columnconfigure(1, weight=1)
        main.rowconfigure(0, weight=1)

        controls = tb.Frame(main)
        controls.grid(row=0, column=0, sticky="ns")

        # API key
        self._labeled_entry(controls, "Census API Key:", self.api_key_var)
        self._labeled_entry(
            controls,
            "GitHub Token:",
            self.github_token_var,
            placeholder="Optional - used for provider discovery",
        )

        # House size and apportionment
        row = tb.Frame(controls)
        row.pack(fill="x", pady=4)
        tb.Label(row, text="House Size:").pack(side="left")
        tb.Spinbox(
            row,
            from_=50,
            to=1000,
            textvariable=self.house_size_var,
            width=8,
        ).pack(side="left", padx=6)
        self.calc_apportion_btn = tb.Button(
            row,
            text="Calculate Apportionment",
            command=self.run_apportionment_calculation,
        )
        self.calc_apportion_btn.pack(side="left", padx=6)

        # State selection
        tb.Label(controls, text="Select State:").pack(anchor="w", pady=(6, 0))
        self.state_combo = tb.Combobox(
            controls, state="disabled", textvariable=tb.StringVar()
        )
        self.state_combo.bind("<<ComboboxSelected>>", self._on_state_changed)
        self.state_combo.pack(fill="x", pady=2)

        # Number of districts
        tb.Label(controls, text="Number of Districts:").pack(anchor="w", pady=(6, 0))
        self.num_districts_spin = tb.Spinbox(
            controls,
            from_=1,
            to=1000,
            textvariable=self.num_districts_var,
            state="disabled",
            width=8,
            command=self.update_num_districts,
        )
        self.num_districts_spin.pack(fill="x", pady=2)

        # VRA
        tb.Checkbutton(
            controls, text="Enable VRA Compliance", variable=self.vra_var
        ).pack(anchor="w", pady=2)

        # Sliders
        self._labeled_scale(
            controls, "Population Equality Weight:", self.pop_weight_var
        )
        self._labeled_scale(controls, "Compactness Weight:", self.compactness_var)

        # COI upload
        coi_row = tb.Frame(controls)
        coi_row.pack(fill="x", pady=4)
        self.coi_button = tb.Button(coi_row, text="Upload COI File", command=self.upload_coi_file)
        self.coi_button.pack(side="left")
        tb.Label(coi_row, textvariable=self.coi_label_var, bootstyle="secondary").pack(
            side="left", padx=6
        )

        # Algorithm
        tb.Label(controls, text="Select Algorithm:").pack(anchor="w", pady=(6, 0))
        self.algorithm_combo = tb.Combobox(
            controls,
            values=["Divide and Conquer (Fair)", "Gerrymander (Democrat)", "Gerrymander (Republican)"],
            textvariable=self.algorithm_var,
            state="readonly",
        )
        self.algorithm_combo.pack(fill="x", pady=2)

        # Election year
        tb.Label(
            controls, text="Election Year (Partisan Data):"
        ).pack(anchor="w", pady=(6, 0))
        year_values = [str(y) for y in AVAILABLE_PARTISAN_YEARS]
        self.election_year_combo = tb.Combobox(
            controls,
            values=year_values,
            textvariable=self.election_year_var,
            state="readonly",
        )
        if year_values:
            self.election_year_var.set(year_values[-1])
        self.election_year_combo.bind(
            "<<ComboboxSelected>>", self._handle_election_year_changed
        )
        self.election_year_combo.pack(fill="x", pady=2)

        # Data quality group
        dq = tb.Labelframe(controls, text="Data Quality")
        dq.pack(fill="x", pady=6)
        for var in [
            self.data_resolution_var,
            self.data_recency_var,
            self.data_confidence_var,
            self.data_contest_var,
            self.data_source_status_var,
        ]:
            tb.Label(dq, textvariable=var).pack(anchor="w")
        tb.Button(dq, text="View Details", command=self._show_data_details).pack(
            anchor="w", pady=(4, 0)
        )

        # Manual override
        tb.Checkbutton(
            controls,
            text="Manual data source override",
            variable=self.manual_override_var,
            command=self._handle_manual_override_toggled,
        ).pack(anchor="w", pady=4)
        self.partisan_provider_combo = tb.Combobox(
            controls, state="disabled", values=[]
        )
        self.partisan_provider_combo.bind(
            "<<ComboboxSelected>>", self._handle_manual_provider_changed
        )
        self.partisan_provider_combo.pack(fill="x", pady=2)

        # Run + progress
        self.generate_btn = tb.Button(controls, text="Generate Map", command=self.run_redistricting)
        self.generate_btn.pack(fill="x", pady=(8, 2))
        self.progress_bar = tb.Progressbar(
            controls, variable=self.progress_value, maximum=100
        )
        self.progress_bar.pack(fill="x", pady=2)
        self.progress_label = tb.Label(
            controls, textvariable=self.progress_text, bootstyle="secondary"
        )
        self.progress_label.pack(anchor="w")

        # Fast mode
        tb.Checkbutton(
            controls,
            text="Fast mode (tract-level data)",
            variable=self.fast_mode_var,
        ).pack(anchor="w", pady=2)

        # Export buttons
        self.export_png_btn = tb.Button(
            controls, text="Export as PNG", command=self.export_as_png, state="disabled"
        )
        self.export_png_btn.pack(fill="x", pady=(8, 2))
        self.export_shp_btn = tb.Button(
            controls,
            text="Export as Shapefile",
            command=self.export_as_shapefile,
            state="disabled",
        )
        self.export_shp_btn.pack(fill="x", pady=2)

        # Clear cache
        self.clear_cache_btn = tb.Button(controls, text="Clear Cache", command=self.clear_cache)
        self.clear_cache_btn.pack(fill="x", pady=(8, 0))

        # Map display
        map_frame = tb.Frame(main, padding=(10, 0, 0, 0))
        map_frame.grid(row=0, column=1, sticky="nsew")
        map_frame.rowconfigure(0, weight=1)
        map_frame.columnconfigure(0, weight=1)
        self.map_label = tb.Label(
            map_frame,
            text="Map will appear here after generation.",
            anchor="center",
            relief="groove",
            padding=10,
        )
        self.map_label.grid(row=0, column=0, sticky="nsew")

    def _labeled_entry(self, parent, label, var, placeholder: Optional[str] = None):
        row = tb.Frame(parent)
        row.pack(fill="x", pady=4)
        tb.Label(row, text=label).pack(side="left")
        entry = tb.Entry(row, textvariable=var)
        entry.pack(side="left", fill="x", expand=True, padx=6)
        if placeholder:
            entry.insert(0, placeholder)
            entry.bind(
                "<FocusIn>",
                lambda e, v=var, p=placeholder: v.set("")
                if v.get() == placeholder
                else None,
            )

    def _labeled_scale(self, parent, label, var):
        tb.Label(parent, text=label).pack(anchor="w", pady=(6, 0))
        scale = tb.Scale(
            parent,
            from_=0,
            to=100,
            orient="horizontal",
            variable=var,
        )
        scale.pack(fill="x", pady=2)

    # ------------------------- UTIL ------------------------- #
    def _get_selected_state_fips(self) -> Optional[str]:
        name = self.state_combo.get()
        return self.state_fips_by_name.get(name)

    def _save_api_key(self):
        api_key = self.api_key_var.get()
        github_token = self.github_token_var.get()
        try:
            with open("config.json", "r") as f:
                config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            config = {}
        config["api_key"] = api_key
        if github_token:
            config["github_token"] = github_token
        elif "github_token" in config:
            del config["github_token"]
        with open("config.json", "w") as f:
            json.dump(config, f)

    def _load_api_key(self):
        try:
            with open("config.json", "r") as f:
                config = json.load(f)
                api_key = config.get("api_key")
                if api_key:
                    self.api_key_var.set(api_key)
                github_token = config.get("github_token")
                if github_token:
                    self.github_token_var.set(github_token)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def _set_progress(self, value: int, text: str = ""):
        self.progress_value.set(max(0, min(100, value)))
        self.progress_text.set(text)

    def _disable_controls(self):
        for widget in [
            self.state_combo,
            self.num_districts_spin,
            self.algorithm_combo,
            self.election_year_combo,
            self.partisan_provider_combo,
            self.generate_btn,
            self.calc_apportion_btn,
            self.coi_button,
            self.clear_cache_btn,
        ]:
            widget.configure(state="disabled")
        self._set_export_state(False)

    def _enable_controls(self):
        self.state_combo.configure(state="readonly" if self.apportionment else "disabled")
        self.num_districts_spin.configure(state="normal" if self.apportionment else "disabled")
        self.algorithm_combo.configure(state="readonly")
        self._update_election_year_control()
        if self.manual_override_var.get():
            self.partisan_provider_combo.configure(state="readonly")
        else:
            self.partisan_provider_combo.configure(state="disabled")
        for widget in [self.generate_btn, self.calc_apportion_btn, self.coi_button, self.clear_cache_btn]:
            widget.configure(state="normal")

    # ------------------------- ACTIONS ------------------------- #
    def clear_cache(self):
        cache_dir = ".cache"
        try:
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
                messagebox.showinfo(
                    "Cache Cleared",
                    f"The cache directory ({cache_dir}) has been cleared successfully.",
                )
            else:
                messagebox.showinfo("Cache Cleared", "No cache directory found to clear.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred while clearing the cache: {e}")

    def upload_coi_file(self):
        file_path = filedialog.askopenfilename(
            title="Upload COI File", filetypes=[("CSV Files", "*.csv")]
        )
        if file_path:
            self.coi_file_path = file_path
            self.coi_label_var.set(os.path.basename(file_path))

    def run_apportionment_calculation(self):
        self._save_api_key()
        api_key = self.api_key_var.get()

        def worker():
            fetcher = DataFetcher(api_key)
            state_populations = fetcher.get_all_states_population_data()
            if not state_populations:
                self.after(
                    0,
                    lambda: messagebox.showerror(
                        "Error",
                        "Failed to fetch population data. Please check the console for details.",
                    ),
                )
                return

            house_size = self.house_size_var.get()
            apportionment = calculate_apportionment(state_populations, house_size)

            def update():
                self.apportionment = apportionment
                names = []
                self.state_fips_by_name = {}
                for state in us.states.STATES:
                    if state.fips in self.apportionment:
                        names.append(state.name)
                        self.state_fips_by_name[state.name] = state.fips
                self.state_combo.configure(values=names, state="readonly")
                if names:
                    self.state_combo.set(names[0])
                self.num_districts_spin.configure(state="normal")
                self.update_num_districts()
                self._refresh_provider_chain()

            self.after(0, update)

        threading.Thread(target=worker, daemon=True).start()

    def update_num_districts(self, *_):
        if not self.apportionment:
            return
        state_fips = self._get_selected_state_fips()
        if state_fips and state_fips in self.apportionment:
            num_districts = self.apportionment[state_fips]
            self.num_districts_var.set(num_districts)
            if num_districts <= 1:
                self.progress_text.set("Need at least 2 districts to run.")
                self.generate_btn.configure(state="disabled")
            else:
                self.generate_btn.configure(state="normal")
        self._refresh_provider_chain()

    def _refresh_provider_chain(self):
        state_fips = self._get_selected_state_fips()
        manual_key = self.manual_provider_key if self.manual_override_var.get() else None
        requested_year = self._selected_year()

        if self.manual_override_var.get():
            available = available_manual_providers(state_fips, requested_year)
            self._populate_manual_provider_combo(available)
            available_keys = [meta.key for meta in available]
            if not available_keys:
                messagebox.showwarning(
                    "No Providers",
                    "No manual providers are available for this state. Reverting to automatic selection.",
                )
                self.manual_override_var.set(False)
                self.manual_provider_key = None
                self.partisan_provider_combo.configure(state="disabled")
                manual_key = None
            elif manual_key not in available_keys:
                self.manual_provider_key = available_keys[0]
                self.partisan_provider_combo.set(available[0].label)
                manual_key = self.manual_provider_key
        else:
            available = available_manual_providers(state_fips, requested_year)
            self._populate_manual_provider_combo(available)

        chain = provider_chain_for_state(state_fips, requested_year, manual_key)
        self.current_provider_chain = chain
        active_meta = chain[0] if chain else None
        self._update_data_quality_panel(active_meta)
        self._update_election_year_control()

    def _populate_manual_provider_combo(self, providers):
        labels = [meta.label for meta in providers]
        self.partisan_provider_combo.configure(values=labels)
        if self.manual_provider_key:
            for meta in providers:
                if meta.key == self.manual_provider_key:
                    self.partisan_provider_combo.set(meta.label)
                    break

    def _update_data_quality_panel(self, metadata, actual=False):
        if metadata is None:
            self.data_resolution_var.set("Resolution: -")
            self.data_recency_var.set("Recency: -")
            self.data_confidence_var.set("Confidence: -")
            self.data_contest_var.set("Contest: -")
            self.data_source_status_var.set("Source: -")
            self.provider_details_text = "No data sources configured."
            return
        resolution_text = metadata.granularity.capitalize()
        self.data_resolution_var.set(f"Resolution: {resolution_text}")
        if metadata.supports_year_selection:
            year = self._selected_year()
            recency = f"{year} general election"
        else:
            recency = metadata.recency_note or "Latest certified data"
        self.data_recency_var.set(f"Recency: {recency}")
        self.data_confidence_var.set(f"Confidence: {metadata.confidence}")
        self.data_contest_var.set(f"Contest: {metadata.label}")
        status_prefix = "In use" if actual else "Planned"
        self.data_source_status_var.set(f"{status_prefix}: {metadata.label}")
        details = []
        for idx, meta in enumerate(self.current_provider_chain, start=1):
            role = "Active" if actual and meta.key == metadata.key else ("Primary" if idx == 1 else "Fallback")
            details.append(
                f"{idx}. {meta.label} - {meta.granularity.capitalize()} · {meta.confidence} ({role})\n   {meta.description}"
            )
        self.provider_details_text = "\n\n".join(details) if details else "No providers available."

    def _show_data_details(self):
        messagebox.showinfo(
            "Partisan Data Sources",
            self.provider_details_text or "No provider information available yet.",
        )

    def _handle_manual_override_toggled(self):
        checked = self.manual_override_var.get()
        self.partisan_provider_combo.configure(state="readonly" if checked else "disabled")
        if not checked:
            self.manual_provider_key = None
        else:
            state_fips = self._get_selected_state_fips()
            requested_year = self._selected_year()
            available = available_manual_providers(state_fips, requested_year)
            if not available:
                messagebox.showwarning(
                    "No Providers", "No manual providers are available for this state."
                )
                self.manual_override_var.set(False)
                self.partisan_provider_combo.configure(state="disabled")
                return
            self.manual_provider_key = available[0].key
            self._populate_manual_provider_combo(available)
            self.partisan_provider_combo.set(available[0].label)
        self._refresh_provider_chain()

    def _handle_manual_provider_changed(self, _event):
        if not self.manual_override_var.get():
            return
        label = self.partisan_provider_combo.get()
        for meta in available_manual_providers(self._get_selected_state_fips(), self._selected_year()):
            if meta.label == label:
                self.manual_provider_key = meta.key
                break
        self._refresh_provider_chain()

    def _update_election_year_control(self):
        active_meta = self.current_provider_chain[0] if self.current_provider_chain else None
        allow = bool(active_meta and active_meta.supports_year_selection and self.state_combo.cget("state") != "disabled")
        self.election_year_combo.configure(state="readonly" if allow else "disabled")

    def _handle_election_year_changed(self, _event):
        self._refresh_provider_chain()

    def _selected_year(self) -> Optional[int]:
        try:
            return int(self.election_year_var.get())
        except ValueError:
            return None

    def _on_state_changed(self, _event):
        self.update_num_districts()

    # ------------------------- REDISTRICTING ------------------------- #
    def run_redistricting(self):
        if not self.apportionment:
            messagebox.showwarning("Missing Data", "Please calculate apportionment first.")
            return
        state_fips = self._get_selected_state_fips()
        if not state_fips:
            messagebox.showwarning("Missing State", "Please select a state.")
            return

        self._save_api_key()
        api_key = self.api_key_var.get()

        self._disable_controls()
        self._set_progress(0, "Fetching data...")

        provider_keys = [meta.key for meta in self.current_provider_chain] or ["county_presidential"]
        active_meta = self.current_provider_chain[0] if self.current_provider_chain else None
        election_year = (
            self._selected_year()
            if active_meta and active_meta.supports_year_selection
            else None
        )
        resolution = "tract" if self.fast_mode_var.get() else "block"

        worker = DataFetcherWorker(
            state_fips,
            api_key,
            election_year=election_year,
            provider_keys=provider_keys,
            resolution=resolution,
            progress_callback=lambda v: self.after(0, lambda: self._set_progress(v, "Fetching data...")),
            finished_callback=lambda df, shp: self.after(0, lambda: self.handle_data_fetched(df, shp)),
            error_callback=lambda msg: self.after(0, lambda: self.handle_data_fetch_error(msg)),
        )

        threading.Thread(target=worker.fetch_data, daemon=True).start()

    def handle_data_fetched(self, census_df, shapefile_path):
        active_meta = None
        if self.current_provider_chain:
            active_meta = self.current_provider_chain[0]
            self.last_applied_provider_meta = active_meta
            self._update_data_quality_panel(active_meta, actual=True)

        self._set_progress(0, "Redistricting...")

        try:
            state_gdf = gpd.read_file(shapefile_path)
            if "GEOID" not in state_gdf.columns:
                if "GEOID20" in state_gdf.columns:
                    state_gdf["GEOID"] = state_gdf["GEOID20"]
                else:
                    raise RuntimeError("Shapefile missing GEOID/GEOID20 field.")
            merged_gdf = state_gdf.merge(census_df, on="GEOID")
            if "partisan_score" not in merged_gdf.columns:
                merged_gdf["partisan_score"] = 0.5
            merged_gdf["partisan_score"] = pd.to_numeric(
                merged_gdf["partisan_score"], errors="coerce"
            )
            fallback = merged_gdf["partisan_score"].mean()
            if pd.isna(fallback):
                fallback = 0.5
            merged_gdf["partisan_score"] = merged_gdf["partisan_score"].fillna(fallback)
        except Exception as exc:
            self.handle_redistricting_error(f"Failed to prepare data: {exc}")
            return

        worker = RedistrictingWorker(
            state_data=merged_gdf,
            num_districts=self.num_districts_var.get(),
            algorithm_name=self.algorithm_var.get(),
            population_equality_weight=self.pop_weight_var.get() / 100.0,
            compactness_weight=self.compactness_var.get() / 100.0,
            vra_compliance=self.vra_var.get(),
            communities_of_interest=self.coi_file_path,
            progress_callback=lambda v: self.after(0, lambda: self._set_progress(v, "Redistricting...")),
            finished_callback=lambda districts: self.after(0, lambda: self.handle_redistricting_finished(districts)),
            error_callback=lambda msg: self.after(0, lambda: self.handle_redistricting_error(msg)),
        )
        threading.Thread(target=worker.run, daemon=True).start()

    def _re_enable_ui_controls(self):
        self._enable_controls()
        self._set_progress(0, "")
        self.update_num_districts()

    def handle_redistricting_finished(self, districts_list):
        all_districts_gdf = gpd.GeoDataFrame()
        for i, district_gdf in enumerate(districts_list):
            district_gdf["district_id"] = i
            all_districts_gdf = pd.concat([all_districts_gdf, district_gdf])

        self.map_generator = MapGenerator(all_districts_gdf)
        map_image_path = self.map_generator.generate_map_image("temp_map.png")
        try:
            self._map_photo = tk.PhotoImage(file=map_image_path)
            self.map_label.configure(image=self._map_photo, text="")
        except Exception:
            # fallback to text if image fails
            self.map_label.configure(text=f"Map generated at {map_image_path}")

        self._re_enable_ui_controls()
        self._set_progress(100, "Done.")
        self._set_export_state(enabled=True)

    def handle_redistricting_error(self, error_message):
        messagebox.showerror("Error", f"Failed to run redistricting: {error_message}")
        self._re_enable_ui_controls()
        self._set_export_state(enabled=False)

    def handle_data_fetch_error(self, error_message):
        messagebox.showerror("Error", f"Failed to fetch data: {error_message}")
        self._re_enable_ui_controls()
        self._set_export_state(enabled=False)

    def _set_export_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        self.export_png_btn.configure(state=state)
        self.export_shp_btn.configure(state=state)

    # ------------------------- EXPORT ------------------------- #
    def export_as_png(self):
        if self.map_generator:
            file_path = filedialog.asksaveasfilename(
                title="Save Map as PNG", defaultextension=".png", filetypes=[("PNG Files", "*.png")]
            )
            if file_path:
                self.map_generator.generate_map_image(file_path)
                messagebox.showinfo("Saved", f"Map saved to {file_path}")

    def export_as_shapefile(self):
        if self.map_generator:
            file_path = filedialog.asksaveasfilename(
                title="Save Districts as Shapefile",
                defaultextension=".shp",
                filetypes=[("Shapefiles", "*.shp")],
            )
            if file_path:
                self.map_generator.export_to_shapefile(file_path)
                messagebox.showinfo("Saved", f"Districts saved to {file_path}")

    # ------------------------- STARTUP ------------------------- #
    def _auto_apportion_on_start(self):
        api_key = self.api_key_var.get()
        if api_key:
            # run after loop starts to keep UI responsive
            self.after(100, self.run_apportionment_calculation)


def main():
    app = MainWindow()
    app.mainloop()


if __name__ == "__main__":
    main()
