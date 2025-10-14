import requests
import os
import zipfile

class DataFetcher:
    def __init__(self):
        self.api_key = None  # It's good practice to use an API key if required

    def get_census_data(self, state_fips):
        """
        Fetches the P.L. 94-171 redistricting data for a given state.
        """
        base_url = "http://api.census.gov/data/2020/dec/pl"

        # Specify the variables to retrieve (e.g., total population)
        # P1_001N is the total population
        get_vars = "NAME,P1_001N"

        # Specify the geography (all blocks in a state)
        for_geo = f"&for=block:*&in=state:{state_fips}"

        # Construct the full URL
        url = f"{base_url}?get={get_vars}{for_geo}"

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

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
