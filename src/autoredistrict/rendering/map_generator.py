import matplotlib.pyplot as plt
import geopandas as gpd

class MapGenerator:
    def __init__(self, districts_gdf):
        self.districts_gdf = districts_gdf

    def generate_map_image(self, output_path):
        """
        Generates a map image from the districts GeoDataFrame.
        """
        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        self.districts_gdf.plot(ax=ax, cmap='viridis', edgecolor='black')
        ax.set_axis_off()
        plt.savefig(output_path, bbox_inches='tight')
        plt.close(fig)
        return output_path

    def export_to_shapefile(self, output_path):
        """
        Exports the districts GeoDataFrame to a shapefile.
        """
        self.districts_gdf.to_file(output_path, driver='ESRI Shapefile')
        return output_path
