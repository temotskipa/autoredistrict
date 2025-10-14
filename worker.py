import os
import zipfile
import pandas as pd
from PyQt5.QtCore import QObject, pyqtSignal
from census import Census

class DataFetcherWorker(QObject):
    finished = pyqtSignal(pd.DataFrame, str)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, state_fips, api_key):
        super().__init__()
        self.state_fips = state_fips
        self.api_key = api_key
        self.c = Census(self.api_key)

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

    def _get_census_data(self, state_fips):
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
        return df

    def _get_shapefiles(self, state_fips):
        base_url = "https://www2.census.gov/geo/tiger/TIGER2024/TABBLOCK20/"
        filename = f"tl_2024_{state_fips}_tabblock20.zip"
        url = f"{base_url}{filename}"
        try:
            response = self.c.session.get(url)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(f"shapefiles_{state_fips}")
            os.remove(filename)
            self.progress.emit(100)
            return f"shapefiles_{state_fips}"
        except Exception as e:
            print(f"An error occurred while downloading the shapefile: {e}")
            return None
        except zipfile.BadZipFile:
            print("Error: The downloaded file is not a valid zip file.")
            return None
