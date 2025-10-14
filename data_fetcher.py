import requests

class DataFetcher:
    def __init__(self, api_key):
        self.api_key = api_key

    def get_all_states_population_data(self):
        """
        Fetches the total population for all states.
        """
        base_url = "http://api.census.gov/data/2020/dec/pl"
        get_vars = "NAME,P1_001N"
        for_geo = "&for=state:*"
        url = f"{base_url}?get={get_vars}{for_geo}&key={self.api_key}"

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
