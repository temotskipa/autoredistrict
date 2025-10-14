import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString
import multiprocessing
from functools import partial
from PyQt5.QtCore import QObject, pyqtSignal

def _polsby_popper_static(gdf):
    """
    Calculates the Polsby-Popper compactness score for a GeoDataFrame.
    """
    if gdf.empty or gdf.unary_union.area == 0:
        return 0
    perimeter = gdf.unary_union.length
    area = gdf.unary_union.area
    if perimeter == 0:
        return 0
    return (4 * np.pi * area) / (perimeter ** 2)

def _calculate_split_score_static(area_gdf, part1, part2, target_pop1, population_equality_weight, compactness_weight, partisan_weight, vra_compliance, communities_of_interest, coi_weight):
    """
    Calculates a score for a given split based on population balance, compactness, and VRA compliance.
    """
    pop1 = part1['P1_001N'].sum()
    pop_balance_score = abs(pop1 - target_pop1) / target_pop1 if target_pop1 > 0 else 0

    compactness1 = _polsby_popper_static(part1)
    compactness2 = _polsby_popper_static(part2)
    compactness_score = 1 - (compactness1 + compactness2) / 2

    vra_score = 0
    if vra_compliance:
        total_pop_area = area_gdf['P1_001N'].sum()
        minority_pop_area = total_pop_area - area_gdf['P1_003N'].sum()
        minority_percentage_area = minority_pop_area / total_pop_area if total_pop_area > 0 else 0

        total_pop_part1 = part1['P1_001N'].sum()
        minority_pop_part1 = total_pop_part1 - part1['P1_003N'].sum()
        minority_percentage_part1 = minority_pop_part1 / total_pop_part1 if total_pop_part1 > 0 else 0

        total_pop_part2 = part2['P1_001N'].sum()
        minority_pop_part2 = total_pop_part2 - part2['P1_003N'].sum()
        minority_percentage_part2 = minority_pop_part2 / total_pop_part2 if total_pop_part2 > 0 else 0

        if minority_percentage_area > 0.3:
            if minority_percentage_part1 < minority_percentage_area and minority_percentage_part2 < minority_percentage_area:
                vra_score = (minority_percentage_area - (minority_percentage_part1 + minority_percentage_part2) / 2)

    partisan_score = 0
    if partisan_weight > 0:
        party1_part1 = part1[part1['party'] > 0.5].shape[0] / part1.shape[0] if part1.shape[0] > 0 else 0
        party1_part2 = part2[part2['party'] > 0.5].shape[0] / part2.shape[0] if part2.shape[0] > 0 else 0
        partisan_score = 1 - abs(party1_part1 - 0.5) - abs(party1_part2 - 0.5)

    coi_score = 0
    if communities_of_interest:
        coi_blocks_in_area = area_gdf[area_gdf['GEOID'].isin(communities_of_interest)]
        if not coi_blocks_in_area.empty:
            coi_blocks_in_part1 = part1[part1['GEOID'].isin(communities_of_interest)]
            coi_blocks_in_part2 = part2[part2['GEOID'].isin(communities_of_interest)]
            if not coi_blocks_in_part1.empty and not coi_blocks_in_part2.empty:
                coi_score = 1

    return (population_equality_weight * pop_balance_score +
            compactness_weight * compactness_score +
            partisan_weight * partisan_score +
            coi_weight * coi_score +
            vra_score)

def _process_angle(angle, area_gdf, centroid, target_pop1, population_equality_weight, compactness_weight, partisan_weight, vra_compliance, communities_of_interest, coi_weight):
    rad = np.deg2rad(angle)
    c_x, c_y = centroid.x, centroid.y

    # Use geometry centroids to decide which side of the line they fall on
    centroids_coords = area_gdf.geometry.centroid
    side = (centroids_coords.x - c_x) * np.sin(rad) - (centroids_coords.y - c_y) * np.cos(rad) > 0

    part1 = area_gdf[side]
    part2 = area_gdf[~side]

    if part1.empty or part2.empty:
        return float('inf'), None

    score = _calculate_split_score_static(area_gdf, part1, part2, target_pop1, population_equality_weight, compactness_weight, partisan_weight, vra_compliance, communities_of_interest, coi_weight)

    return score, {'part1': part1, 'part2': part2}


class RedistrictingAlgorithm(QObject):
    progress_update = pyqtSignal(int)

    def __init__(self, state_data, num_districts, population_equality_weight=1.0, compactness_weight=1.0, partisan_weight=0.0, vra_compliance=False, communities_of_interest=None, coi_weight=1.0):
        super().__init__()
        self.state_data = state_data
        for col in ['P1_001N', 'P1_003N', 'P1_004N', 'P1_005N', 'P1_006N', 'P1_007N', 'P1_008N']:
            if col in self.state_data.columns:
                self.state_data[col] = pd.to_numeric(self.state_data[col], errors='coerce').fillna(0)
        self.num_districts = num_districts
        self.population_equality_weight = population_equality_weight
        self.compactness_weight = compactness_weight
        self.partisan_weight = partisan_weight
        self.vra_compliance = vra_compliance
        self.communities_of_interest = communities_of_interest
        self.coi_weight = coi_weight

    def divide_and_conquer(self):
        self.partitions_done = 0
        districts = self._recursive_partition(self.state_data, self.num_districts)
        return districts

    def _recursive_partition(self, area_gdf, num_districts_to_create):
        if num_districts_to_create <= 1:
            self.partitions_done += 1
            progress = int((self.partitions_done / self.num_districts) * 100)
            self.progress_update.emit(progress)
            return [area_gdf]

        num_districts_1 = num_districts_to_create // 2
        num_districts_2 = num_districts_to_create - num_districts_1

        best_split = self._find_best_split(area_gdf, num_districts_1, num_districts_2)

        if best_split is None or 'part1' not in best_split or 'part2' not in best_split or best_split['part1'].empty or best_split['part2'].empty:
             # If no valid split is found, return the area as a single district
            return [area_gdf]

        districts_1 = self._recursive_partition(best_split['part1'], num_districts_1)
        districts_2 = self._recursive_partition(best_split['part2'], num_districts_2)

        return districts_1 + districts_2

    def _find_best_split(self, area_gdf, num_districts_1, num_districts_2):
        total_population = area_gdf['P1_001N'].sum()
        if total_population == 0: return None
        target_pop1 = (total_population / (num_districts_1 + num_districts_2)) * num_districts_1

        centroid = area_gdf.unary_union.centroid
        angles = np.linspace(0, 180, 10)

        # Use functools.partial to create a function with most arguments already filled in
        worker_func = partial(_process_angle,
                              area_gdf=area_gdf,
                              centroid=centroid,
                              target_pop1=target_pop1,
                              population_equality_weight=self.population_equality_weight,
                              compactness_weight=self.compactness_weight,
                              partisan_weight=self.partisan_weight,
                              vra_compliance=self.vra_compliance,
                              communities_of_interest=self.communities_of_interest,
                              coi_weight=self.coi_weight)

        with multiprocessing.Pool() as pool:
            results = pool.map(worker_func, angles)

        best_score = float('inf')
        best_split = None
        for score, split in results:
            if score < best_score:
                best_score = score
                best_split = split

        return best_split

    def gerrymander(self):
        self.partitions_done = 0
        self.partisan_weight = 1.0
        districts = self._recursive_partition(self.state_data, self.num_districts)
        return districts
