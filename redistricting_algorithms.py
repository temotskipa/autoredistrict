import geopandas as gpd
import numpy as np
from shapely.geometry import LineString

class RedistrictingAlgorithm:
    def __init__(self, state_data, num_districts, population_equality_weight=1.0, compactness_weight=1.0, vra_compliance=False):
        self.state_data = state_data
        self.num_districts = num_districts
        self.population_equality_weight = population_equality_weight
        self.compactness_weight = compactness_weight
        self.vra_compliance = vra_compliance

    def divide_and_conquer(self):
        """
        Recursively partitions the state into the desired number of districts.
        """

        # Start the recursive partitioning process
        districts = self._recursive_partition(self.state_data, self.num_districts)

        return districts

    def _recursive_partition(self, area_gdf, num_districts_to_create):
        """
        The core recursive logic of the divide and conquer algorithm.
        """
        # Base case: if we only need to create one district, return the area as a list
        if num_districts_to_create <= 1:
            return [area_gdf]

        # Determine the number of districts for each side of the split
        num_districts_1 = num_districts_to_create // 2
        num_districts_2 = num_districts_to_create - num_districts_1

        # Find the best split for the current area
        best_split = self._find_best_split(area_gdf, num_districts_1, num_districts_2)

        # Recursively partition the two new areas
        districts_1 = self._recursive_partition(best_split['part1'], num_districts_1)
        districts_2 = self._recursive_partition(best_split['part2'], num_districts_2)

        return districts_1 + districts_2

    def _find_best_split(self, area_gdf, num_districts_1, num_districts_2):
        """
        Finds the best way to split a given area into two parts.
        """
        best_split = None
        best_score = float('inf')

        total_population = area_gdf['P1_001N'].sum()
        target_pop1 = (total_population / (num_districts_1 + num_districts_2)) * num_districts_1

        for angle in np.linspace(0, 180, 10): # Try 10 different angles for the split line
            rad = np.deg2rad(angle)
            centroid = area_gdf.unary_union.centroid
            line = LineString([
                (centroid.x - np.cos(rad) * 100, centroid.y - np.sin(rad) * 100),
                (centroid.x + np.cos(rad) * 100, centroid.y + np.sin(rad) * 100)
            ])

            # This is a very simplified way to split. A real implementation would be more robust.
            part1 = area_gdf[area_gdf.geometry.centroid.x < centroid.x]
            part2 = area_gdf[area_gdf.geometry.centroid.x >= centroid.x]

            if part1.empty or part2.empty:
                continue

            score = self._calculate_split_score(part1, part2, target_pop1)

            if score < best_score:
                best_score = score
                best_split = {'part1': part1, 'part2': part2}

        return best_split

    def _calculate_split_score(self, part1, part2, target_pop1):
        """
        Calculates a score for a given split based on population balance and compactness.
        """
        pop1 = part1['P1_001N'].sum()
        pop_balance_score = abs(pop1 - target_pop1) / target_pop1

        compactness1 = self._polsby_popper(part1)
        compactness2 = self._polsby_popper(part2)
        compactness_score = 1 - (compactness1 + compactness2) / 2 # We want to maximize compactness, so we minimize 1 - compactness

        return (self.population_equality_weight * pop_balance_score +
                self.compactness_weight * compactness_score)

    def _polsby_popper(self, gdf):
        """
        Calculates the Polsby-Popper compactness score for a GeoDataFrame.
        """
        perimeter = gdf.unary_union.length
        area = gdf.unary_union.area
        return (4 * np.pi * area) / (perimeter ** 2)

    def gerrymander(self):
        """
        Creates gerrymandered districts by concentrating a certain population into one district.
        """
        sorted_blocks = self.state_data.sort_values(by='party', ascending=False)
        total_population = sorted_blocks['P1_001N'].sum()
        ideal_population = total_population / self.num_districts

        districts = []

        # Create the 'packed' district
        packed_district_blocks = []
        packed_population = 0
        for i, block in sorted_blocks.iterrows():
            if packed_population < ideal_population:
                packed_district_blocks.append(block)
                packed_population += block['P1_001N']
            else:
                break

        districts.append(gpd.GeoDataFrame(packed_district_blocks))

        remaining_blocks = sorted_blocks.iloc[len(packed_district_blocks):]

        # Split the remaining blocks into the other districts
        # This is still a simplification, but it produces the correct number of districts
        remaining_districts = np.array_split(remaining_blocks, self.num_districts - 1)

        for district_df in remaining_districts:
            districts.append(gpd.GeoDataFrame(district_df))

        return districts
