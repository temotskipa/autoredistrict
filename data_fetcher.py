import requests
import os
import zipfile

class DataFetcher:
    def __init__(self):
        self.api_key = None  # It's good practice to use an API key if required

    def _get_counties_for_state(self, state_fips):
        """
        Retrieves a list of county FIPS codes for a given state.
        """
        base_url = "https://api.census.gov/data/2020/dec/pl"
        get_vars = "NAME"
        for_geo = f"&for=county:*&in=state:{state_fips}"
        url = f"{base_url}?get={get_vars}{for_geo}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            # Extract county FIPS codes, skipping the header
            counties = [row[2] for row in data[1:]]
            return counties
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching counties: {e}")
            return None

    def get_census_data(self, state_fips):
        """
        Fetches the P.L. 94-171 redistricting data for a given state.
        This is done by first getting a list of all counties in the state,
        and then fetching the block-level data for each county.
        """
        base_url = "https://api.census.gov/data/2020/dec/pl"
        get_vars = "NAME,P1_001N,P1_003N,P1_004N,P1_005N,P1_006N,P1_007N,P1_008N"

        counties = self._get_counties_for_state(state_fips)
        if not counties:
            return None

        all_census_data = []
        is_first_request = True

        for county_fips in counties:
            for_geo = f"&for=block:*&in=state:{state_fips}&in=county:{county_fips}"
            url = f"{base_url}?get={get_vars}{for_geo}"

            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if is_first_request:
                    all_census_data.extend(data)
                    is_first_request = False
                else:
                    all_census_data.extend(data[1:])  # Skip header for subsequent requests

            except requests.exceptions.RequestException as e:
                print(f"An error occurred for county {county_fips}: {e}")
                # Decide if we should continue or fail fast
                continue # Continue with the next county

        return all_census_data

    def get_all_states_population_data(self):
        """
        Fetches the total population for all states.
        """
        base_url = "http://api.census.gov/data/2020/dec/pl"
        get_vars = "NAME,P1_001N"
        for_geo = "&for=state:*"
        url = f"{base_url}?get={get_vars}{for_geo}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Convert to a dictionary of FIPS codes to populations
            state_populations = {}
            for row in data[1:]:  # Skip header row
                state_fips = row[2]
                population = int(row[1])
                state_populations[state_fips] = population
            return state_populations
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def get_shapefiles(self, state_fips):
        """
        Downloads and extracts the TIGER/Line shapefile for a given state.
        """
        base_url = "https://www2.census.gov/geo/tiger/TIGER2024/TABBLOCK20/"
        filename = f"tl_2024_{state_fips}_tabblock20.zip"
        url = f"{base_url}{filename}"

        try:
            # Download the file
            response = requests.get(url)
            response.raise_for_status()

            # Save the file to a temporary location
            with open(filename, 'wb') as f:
                f.write(response.content)

            # Extract the shapefile
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(f"shapefiles_{state_fips}")

            # Clean up the downloaded zip file
            os.remove(filename)

            print(f"Shapefiles for state {state_fips} downloaded and extracted.")
            return f"shapefiles_{state_fips}"

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while downloading the shapefile: {e}")
            return None
        except zipfile.BadZipFile:
            print("Error: The downloaded file is not a valid zip file.")
            return None
