import os
import zipfile
import pandas as pd
import cudf
import geopandas as gpd
from numba import cuda
from PyQt5.QtCore import QObject, pyqtSignal
from census import Census
import requests
from datetime import datetime
import shutil

class DataFetcherWorker(QObject):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, state_fips, api_key, use_gpu=False):
        super().__init__()
        self.state_fips = state_fips
        self.api_key = api_key
        self.c = Census(self.api_key)
        self.use_gpu = use_gpu and cuda.is_available()

    def fetch_data(self):
        try:
            shapefile_path = self._get_shapefiles(self.state_fips)
            if not shapefile_path:
                self.error.emit("Failed to fetch shapefiles.")
                return

            if self.use_gpu:
                census_df = self._get_census_data_gpu(self.state_fips)
                if census_df is None:
                    self.error.emit("Failed to fetch census data for GPU.")
                    return

                state_gdf = cuspatial.read_shapefile(shapefile_path)
                state_gdf['GEOID'] = state_gdf['GEOID20']
                merged_gdf = state_gdf.merge(census_df, on='GEOID')
                self.finished.emit(merged_gdf)

            else:
                census_df = self._get_census_data_cpu(self.state_fips)
                if census_df is None:
                    self.error.emit("Failed to fetch census data for CPU.")
                    return

                state_gdf = gpd.read_file(shapefile_path)
                state_gdf['GEOID'] = state_gdf['GEOID20']
                merged_gdf = state_gdf.merge(census_df, on='GEOID')
                self.finished.emit(merged_gdf)

        except Exception as e:
            self.error.emit(str(e))

    def _get_counties_for_state(self, state_fips):
        try:
            data = self.c.pl.get('NAME', {'for': 'county:*', 'in': f'state:{state_fips}'})
            return [item['county'] for item in data]
        except Exception as e:
            print(f"An error occurred while fetching counties: {e}")
            return None

    def _get_tracts_for_county(self, state_fips, county_fips):
        try:
            data = self.c.pl.get('NAME', {'for': 'tract:*', 'in': f'state:{state_fips} county:{county_fips}'})
            return [item['tract'] for item in data]
        except Exception as e:
            print(f"An error occurred while fetching tracts for county {county_fips}: {e}")
            return None

    def _get_census_data_cpu(self, state_fips):
        cache_dir = ".cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"census_{state_fips}.csv")

        if os.path.exists(cache_file):
            print(f"Loading census data from cache: {cache_file}")
            return pd.read_csv(cache_file, dtype={'GEOID': str})

        fields = ('NAME', 'P1_001N', 'P1_003N', 'P1_004N', 'P1_005N', 'P1_006N', 'P1_007N', 'P1_008N')

        counties = self._get_counties_for_state(state_fips)
        if not counties:
            return None

        all_census_data = []
        num_counties = len(counties)
        for i, county_fips in enumerate(counties):
            tracts = self._get_tracts_for_county(state_fips, county_fips)
            if not tracts:
                continue
            for tract_fips in tracts:
                try:
                    data = self.c.pl.get(fields, {'for': 'block:*', 'in': f'state:{state_fips} county:{county_fips} tract:{tract_fips}'})
                    all_census_data.extend(data)
                except Exception as e:
                    print(f"An error occurred for tract {tract_fips} in county {county_fips}: {e}")
                    continue
            self.progress.emit(int(((i + 1) / num_counties) * 75))

        if not all_census_data:
            return None

        df = pd.DataFrame(all_census_data)
        df['GEOID'] = df['state'] + df['county'] + df['tract'] + df['block']
        df.to_csv(cache_file, index=False)
        print(f"Saved census data to cache: {cache_file}")
        return df

    def _get_census_data_gpu(self, state_fips):
        cache_dir = ".cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"census_{state_fips}.csv")

        if os.path.exists(cache_file):
            print(f"Loading census data from cache into GPU: {cache_file}")
            return cudf.read_csv(cache_file, dtype={'GEOID': 'str'})

        fields = ('NAME', 'P1_001N', 'P1_003N', 'P1_004N', 'P1_005N', 'P1_006N', 'P1_007N', 'P1_008N')

        counties = self._get_counties_for_state(state_fips)
        if not counties:
            return None

        all_census_data = []
        num_counties = len(counties)
        for i, county_fips in enumerate(counties):
            tracts = self._get_tracts_for_county(state_fips, county_fips)
            if not tracts:
                continue
            for tract_fips in tracts:
                try:
                    data = self.c.pl.get(fields, {'for': 'block:*', 'in': f'state:{state_fips} county:{county_fips} tract:{tract_fips}'})
                    all_census_data.extend(data)
                except Exception as e:
                    print(f"An error occurred for tract {tract_fips} in county {county_fips}: {e}")
                    continue
            self.progress.emit(int(((i + 1) / num_counties) * 75))

        if not all_census_data:
            return None

        df = cudf.DataFrame(all_census_data)
        df['GEOID'] = df['state'] + df['county'] + df['tract'] + df['block']
        df.to_csv(cache_file, index=False)
        print(f"Saved census data to cache: {cache_file}")
        return df

    def _get_shapefiles(self, state_fips):
        cache_dir = ".cache"
        shapefile_dir = os.path.join(cache_dir, f"shapefiles_{state_fips}")
        base_url = "https://www2.census.gov/geo/tiger/TIGER2024/TABBLOCK20/"
        filename = f"tl_2024_{state_fips}_tabblock20.zip"
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
                        print("Remote shapefile is newer. Re-downloading...")
                        shutil.rmtree(shapefile_dir)
                    else:
                        print(f"Using cached shapefile directory: {shapefile_dir}")
                        self.progress.emit(100)
                        return shapefile_dir
            except requests.RequestException as e:
                print(f"Could not check for newer shapefile, using cache. Error: {e}")
                self.progress.emit(100)
                return shapefile_dir

        try:
            print(f"Downloading shapefile from {url}")
            response = self.c.session.get(url)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(shapefile_dir)
            os.remove(filename)
            os.utime(shapefile_dir, None)
            self.progress.emit(100)
            return shapefile_dir
        except Exception as e:
            print(f"An error occurred while downloading the shapefile: {e}")
            return None
        except zipfile.BadZipFile:
            print("Error: The downloaded file is not a valid zip file.")
            return None
