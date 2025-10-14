import requests
import os
import zipfile
import time
import pandas as pd
from PyQt5.QtCore import QObject, pyqtSignal

class DataFetcherWorker(QObject):
    finished = pyqtSignal(pd.DataFrame, str)
    error = pyqtSignal(str)

    def __init__(self, state_fips, api_key):
        super().__init__()
        self.state_fips = state_fips
        self.api_key = api_key

    def fetch_data(self):
        try:
            census_df = self._get_census_data(self.state_fips)
            shapefile_path = self._get_shapefiles(self.state_fips)

            if census_df is not None and shapefile_path:
                self.finished.emit(census_df, shapefile_path)
            else:
                self.error.emit("Failed to fetch data.")
        except Exception as e:
            self.error.emit(str(e))

    def _get_counties_for_state(self, state_fips):
        base_url = "https://api.census.gov/data/2020/dec/pl"
        get_vars = "NAME"
        for_geo = f"&for=county:*&in=state:{state_fips}"
        url = f"{base_url}?get={get_vars}{for_geo}&key={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return [row[2] for row in data[1:]]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching counties: {e}")
            return None

    def _get_census_data(self, state_fips):
        base_url = "https://api.census.gov/data/2020/dec/pl"
        get_vars = "NAME,P1_001N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N,state,county,tract,block"

        counties = self._get_counties_for_state(state_fips)
        if not counties:
            return None

        all_census_data = []
        is_first_request = True

        for county_fips in counties:
            for_geo = f"&for=block:*&in=state:{state_fips}&in=county:{county_fips}"
            url = f"{base_url}?get={get_vars}{for_geo}&key={self.api_key}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                if is_first_request:
                    all_census_data.extend(data)
                    is_first_request = False
                else:
                    all_census_data.extend(data[1:])
            except requests.exceptions.RequestException as e:
                print(f"An error occurred for county {county_fips}: {e}")
                continue
            time.sleep(0.2)

        if not all_census_data:
            return None

        df = pd.DataFrame(all_census_data[1:], columns=all_census_data[0])
        df['GEOID'] = df['state'] + df['county'] + df['tract'] + df['block']
        return df

    def _get_shapefiles(self, state_fips):
        base_url = "https://www2.census.gov/geo/tiger/TIGER2024/TABBLOCK20/"
        filename = f"tl_2024_{state_fips}_tabblock20.zip"
        url = f"{base_url}{filename}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(f"shapefiles_{state_fips}")
            os.remove(filename)
            return f"shapefiles_{state_fips}"
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while downloading the shapefile: {e}")
            return None
        except zipfile.BadZipFile:
            print("Error: The downloaded file is not a valid zip file.")
            return None
