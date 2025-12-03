import logging
import os
import random
import shutil
import time
import zipfile
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
import requests
from census import Census

from ..data.partisan_providers import (
    DEFAULT_PARTISAN_YEAR,
    PROVIDER_REGISTRY,
    fetch_scores_for_provider,
)


class DataFetcherWorker:

    CENSUS_FIELDS = ('NAME', 'P1_001N', 'P1_003N', 'P1_004N', 'P1_005N', 'P1_006N', 'P1_007N', 'P1_008N')

    def __init__(
        self,
        state_fips,
        api_key,
        election_year=None,
        provider_keys=None,
        resolution="block",
        progress_callback: Optional[Callable[[int], None]] = None,
        finished_callback: Optional[Callable[[pd.DataFrame, str], None]] = None,
        error_callback: Optional[Callable[[str], None]] = None,
    ):
        self.state_fips = state_fips
        self.api_key = api_key
        self.c = Census(self.api_key)
        self.election_year = election_year or DEFAULT_PARTISAN_YEAR
        self.provider_keys = provider_keys or ["county_presidential"]
        self.active_provider_meta = None
        self.logger = logging.getLogger(__name__)
        self.resolution = resolution  # "block" (default) or "tract"
        self.progress_callback = progress_callback
        self.finished_callback = finished_callback
        self.error_callback = error_callback

    # ------------- event helpers ------------- #
    def _emit_progress(self, value: int):
        if self.progress_callback:
            try:
                self.progress_callback(int(value))
            except Exception:
                pass

    def _emit_finished(self, census_df, shapefile_path):
        if self.finished_callback:
            try:
                self.finished_callback(census_df, shapefile_path)
            except Exception:
                pass

    def _emit_error(self, message: str):
        if self.error_callback:
            try:
                self.error_callback(message)
            except Exception:
                pass

    # ------------- cache helpers ------------- #
    def _cache_paths(self, state_fips):
        cache_dir = ".cache"
        os.makedirs(cache_dir, exist_ok=True)
        base = os.path.join(cache_dir, f"census_{state_fips}_{self.resolution}")
        return f"{base}.csv", f"{base}.parquet"

    def _load_cache(self, state_fips):
        csv_path, parquet_path = self._cache_paths(state_fips)
        if os.path.exists(parquet_path):
            try:
                df = pd.read_parquet(parquet_path)
                self.logger.info(f"Loading census data from cache: {parquet_path}")
                return df
            except Exception as exc:
                self.logger.warning(f"Failed to read parquet cache {parquet_path}: {exc}")
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path, dtype={'GEOID': str, 'county': str})
                self.logger.info(f"Loading census data from cache: {csv_path}")
                return df
            except Exception as exc:
                self.logger.warning(f"Failed to read CSV cache {csv_path}: {exc}")
        return None

    def _save_cache(self, state_fips, df):
        csv_path, parquet_path = self._cache_paths(state_fips)
        try:
            df.to_csv(csv_path, index=False)
        except Exception as exc:
            self.logger.warning(f"Failed to write CSV cache {csv_path}: {exc}")
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception as exc:
            self.logger.warning(f"Failed to write parquet cache {parquet_path}: {exc}")
        self.logger.info(f"Saved census data to cache: {csv_path}")

    def fetch_data(self):
        try:
            census_df = self._get_census_data(self.state_fips)
            shapefile_path = self._get_shapefiles(self.state_fips)

            if census_df is not None and shapefile_path:
                self._emit_finished(census_df, shapefile_path)
            else:
                self._emit_error("Failed to fetch data.")
        except Exception as e:
            self._emit_error(str(e))

    def _get_counties_for_state(self, state_fips):
        try:
            data = self._with_retries(lambda: self.c.pl.get('NAME', {'for': 'county:*', 'in': f'state:{state_fips}'}))
            return [item['county'] for item in data]
        except Exception as e:
            print(f"An error occurred while fetching counties: {e}")
            return None

    def _get_tracts_for_county(self, state_fips, county_fips):
        try:
            data = self._with_retries(
                lambda: self.c.pl.get('NAME', {'for': 'tract:*', 'in': f'state:{state_fips} county:{county_fips}'}))
            return [item['tract'] for item in data]
        except Exception as e:
            print(f"An error occurred while fetching tracts for county {county_fips}: {e}")
            return None

    def _with_retries(self, func, retries=3, base_delay=1.0):
        for attempt in range(1, retries + 1):
            try:
                return func()
            except Exception as exc:
                if attempt == retries:
                    raise
                delay = base_delay * (2 ** (attempt - 1)) + random.random() * 0.2
                self.logger.warning(f"Retrying after error: {exc} (attempt {attempt}/{retries})")
                time.sleep(delay)

    def _get_census_data(self, state_fips):
        cached_df = self._load_cache(state_fips)
        if cached_df is not None:
            if 'partisan_score' not in cached_df.columns:
                cached_df = self._attach_partisan_scores(cached_df, state_fips)
                self._save_cache(state_fips, cached_df)
            return cached_df

        counties = self._get_counties_for_state(state_fips)
        if not counties:
            return None

        all_census_data = []
        num_counties = len(counties)
        for i, county_fips in enumerate(counties):
            tracts = self._get_tracts_for_county(state_fips, county_fips)
            if not tracts:
                continue
            if self.resolution == "tract":
                try:
                    data = self.c.pl.get(self.CENSUS_FIELDS,
                                         {'for': 'tract:*', 'in': f'state:{state_fips} county:{county_fips}'})
                    all_census_data.extend(data)
                except Exception as e:
                    self.logger.warning(f"Tract fetch failed for county {county_fips}: {e}")
                    continue
            else:
                for tract_fips in tracts:
                    try:
                        data = self.c.pl.get(self.CENSUS_FIELDS, {'for': 'block:*',
                                                                  'in': f'state:{state_fips} county:{county_fips} tract:{tract_fips}'})
                        all_census_data.extend(data)
                    except Exception as e:
                        self.logger.warning(f"Block fetch failed for tract {tract_fips} county {county_fips}: {e}")
                        continue
            self._emit_progress(int(((i + 1) / num_counties) * 75))

        if not all_census_data:
            return None

        df = pd.DataFrame(all_census_data)
        if self.resolution == "tract":
            df['GEOID'] = df['state'] + df['county'] + df['tract']
        else:
            df['GEOID'] = df['state'] + df['county'] + df['tract'] + df['block']
        df = self._attach_partisan_scores(df, state_fips)
        self._save_cache(state_fips, df)
        return df

    def _attach_partisan_scores(self, df, state_fips):
        self.active_provider_meta = None
        try:
            for provider_key in self.provider_keys:
                provider_meta = PROVIDER_REGISTRY.get(provider_key)
                if not provider_meta:
                    continue
                county_scores = fetch_scores_for_provider(provider_meta, state_fips, self.election_year)
                if county_scores is None or county_scores.empty:
                    continue
                df['county'] = df['county'].astype(str).str.zfill(3)
                if 'tract' in county_scores.columns:
                    # higher resolution partisan data
                    df['tract'] = df.get('tract', '').astype(str).str.zfill(6)
                    county_scores['tract'] = county_scores['tract'].astype(str).str.zfill(6)
                    df = df.merge(county_scores, on=['county', 'tract'], how='left')
                else:
                    df = df.merge(county_scores, on='county', how='left')
                fallback = county_scores['partisan_score'].mean()
                fallback = 0.5 if pd.isna(fallback) else fallback
                df['partisan_score'] = df['partisan_score'].fillna(fallback)
                self.active_provider_meta = provider_meta
                self.logger.info(f"Attached partisan data from provider '{provider_meta.key}' ({provider_meta.label})")
                return df
            self.logger.warning("No partisan provider produced results; defaulting partisan_score to 0.5.")
            df['partisan_score'] = 0.5
            return df
        except Exception as exc:
            self.logger.warning(f"Unable to attach partisan data: {exc}")
            df['partisan_score'] = 0.5
            return df

    def _get_shapefiles(self, state_fips):
        cache_dir = ".cache"
        suffix = "tract20" if self.resolution == "tract" else "tabblock20"
        base_folder = "TTRACT20" if self.resolution == "tract" else "TABBLOCK20"
        shapefile_dir = os.path.join(cache_dir, f"shapefiles_{state_fips}_{self.resolution}")
        shapefile_base = f"tl_2024_{state_fips}_{suffix}"
        shapefile_path = os.path.join(shapefile_dir, f"{shapefile_base}.shp")
        base_url = f"https://www2.census.gov/geo/tiger/TIGER2024/{base_folder}/"
        filename = f"{shapefile_base}.zip"
        url = f"{base_url}{filename}"

        if os.path.exists(shapefile_dir):
            try:
                response = requests.head(url)
                response.raise_for_status()
                last_modified_header = response.headers.get('Last-Modified')
                if last_modified_header:
                    remote_last_modified = datetime.strptime(last_modified_header, '%a, %d %b %Y %H:%M:%S %Z')
                    local_last_modified = datetime.fromtimestamp(os.path.getmtime(shapefile_dir))
                    if remote_last_modified > local_last_modified:
                        self.logger.info("Remote shapefile is newer. Re-downloading...")
                        shutil.rmtree(shapefile_dir)
                    elif os.path.exists(shapefile_path):
                        self.logger.info(f"Using cached shapefile directory: {shapefile_dir}")
                self._emit_progress(100)
                return shapefile_path
            except requests.RequestException as e:
                self.logger.warning(f"Could not check for newer shapefile, using cache. Error: {e}")
                if os.path.exists(shapefile_path):
                    self._emit_progress(100)
                    return shapefile_path
                # Cache directory exists but shapefile is missing; fall through to re-download.

        try:
            self.logger.info(f"Downloading shapefile from {url}")
            response = self.c.session.get(url)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(shapefile_dir)
            os.remove(filename)
            os.utime(shapefile_dir, None)
            if not os.path.exists(shapefile_path):
                # Fall back to first .shp in the directory if naming changes.
                candidates = [file for file in os.listdir(shapefile_dir) if file.lower().endswith(".shp")]
                if candidates:
                    shapefile_path = os.path.join(shapefile_dir, candidates[0])
            if os.path.exists(shapefile_path):
                self._emit_progress(100)
                return shapefile_path
            self.logger.error("Extracted shapefile missing .shp file.")
            self._emit_progress(100)
            return None
        except Exception as e:
            self.logger.error(f"An error occurred while downloading the shapefile: {e}")
            return None
        except zipfile.BadZipFile:
            self.logger.error("Error: The downloaded file is not a valid zip file.")
            return None
