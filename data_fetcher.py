from census import Census

class DataFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.c = Census(self.api_key)

    def get_all_states_population_data(self):
        """
        Fetches the total population for all states.
        """
        try:
            data = self.c.pl.state(('NAME', 'P1_001N'), Census.ALL)
            state_populations = {item['state']: int(item['P1_001N']) for item in data}
            return state_populations
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
