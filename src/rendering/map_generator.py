import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd


class MapGenerator:
    def __init__(self, districts_gdf):
        self.districts_gdf = districts_gdf

    def _dissolved_districts(self) -> gpd.GeoDataFrame:
        """Return district-level polygons with weighted partisan scores when available."""
        gdf = self.districts_gdf
        if "district_id" not in gdf.columns:
            return gdf
        work = gdf.copy()
        if "P1_001N" in work.columns:
            work["__pop"] = pd.to_numeric(work["P1_001N"], errors="coerce").fillna(0)
        else:
            work["__pop"] = 1.0
        if "partisan_score" in work.columns:
            work["__score"] = pd.to_numeric(work["partisan_score"], errors="coerce").fillna(0.5)
        else:
            work["__score"] = 0.5

        dissolved = work.dissolve(by="district_id", aggfunc={"__pop": "sum"})
        weighted_num = (work["__score"] * work["__pop"]).groupby(work["district_id"]).sum()
        weighted_den = work["__pop"].groupby(work["district_id"]).sum().replace({0: pd.NA})
        weighted = (weighted_num / weighted_den).fillna(0.5)
        dissolved["partisan_score"] = weighted.reindex(dissolved.index).values
        dissolved.reset_index(inplace=True)
        return dissolved

    def generate_map_image(self, output_path):
        """
        Generates a map image from the districts GeoDataFrame.
        - If district_id present, dissolve to district polygons.
        - If partisan_score present, shade red/blue by partisan_score (0=R,1=D).
        """
        display_gdf = self._dissolved_districts()

        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        plot_kwargs = {"ax": ax, "edgecolor": "black"}
        if "partisan_score" in display_gdf.columns:
            plot_kwargs.update(
                {
                    "column": "partisan_score",
                    "cmap": "RdBu_r",
                    "vmin": 0,
                    "vmax": 1,
                }
            )
        elif "district_id" in display_gdf.columns:
            plot_kwargs.update({"column": "district_id", "cmap": "tab20"})
        else:
            plot_kwargs.update({"cmap": "viridis"})

        display_gdf.plot(**plot_kwargs)
        ax.set_axis_off()
        plt.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        return output_path

    def export_to_shapefile(self, output_path):
        """
        Exports dissolved district polygons (if available) to a shapefile.
        """
        gdf = self._dissolved_districts().copy()
        # Shapefile field name limit is 10 chars; shorten to avoid warnings.
        rename_map = {}
        for col in list(gdf.columns):
            if len(col) > 10:
                short = col[:10]
                # Ensure uniqueness
                suffix = 1
                while short in rename_map.values() or short in gdf.columns:
                    short = f"{col[:7]}{suffix}"
                    suffix += 1
                rename_map[col] = short
        if rename_map:
            gdf = gdf.rename(columns=rename_map)
        gdf.to_file(output_path, driver="ESRI Shapefile")
        return output_path
