import logging
import os
from typing import Optional
import requests

import pandas as pd
import us


class CountyPresidentialReturnsProvider:
    """
    Handles downloading and caching the Harvard Dataverse county-level presidential returns.
    Provides partisan scores for arbitrary states and election years.
    """

    # Direct download for "countypres_2000-2024.tab" (ID 13256842)
    FILE_URL = "https://dataverse.harvard.edu/api/access/datafile/13256842"
    COUNTY_FILE_LABEL = "countypres_2000-2024.tab"

    def __init__(self, cache_root: str = ".cache"):
        self.cache_dir = os.path.join(cache_root, "partisan")
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_state_scores(self, state_fips: str, election_year: int) -> Optional[pd.DataFrame]:
        """
        Returns a DataFrame with columns ['county', 'partisan_score'] for the requested state/year.
        """
        if not state_fips:
            return None
        state = us.states.lookup(state_fips)
        if not state:
            return None

        dataset_path = self._ensure_dataset_file()
        if not dataset_path:
            return None

        try:
            df = pd.read_csv(
                dataset_path,
                sep="\t",
                dtype={"county_fips": str, "year": int, "state_po": str, "party": str, "candidatevotes": int},
            )
        except Exception as exc:
            print(f"Unable to read county presidential dataset: {exc}")
            return None

        df_state = df[
            (df["year"] == election_year)
            & (df["state_po"] == state.abbr)
            & (df["office"].str.contains("PRESIDENT", case=False, na=False))
            ]
        if df_state.empty:
            return None

        df_state = df_state[df_state["party"].isin(["DEMOCRAT", "REPUBLICAN"])]
        if df_state.empty:
            return None

        pivot = (
            df_state.pivot_table(
                index="county_fips",
                columns="party",
                values="candidatevotes",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        if pivot.empty:
            return None

        if "DEMOCRAT" not in pivot.columns:
            pivot["DEMOCRAT"] = 0
        if "REPUBLICAN" not in pivot.columns:
            pivot["REPUBLICAN"] = 0

        pivot["total_votes"] = pivot["DEMOCRAT"] + pivot["REPUBLICAN"]
        pivot = pivot[pivot["total_votes"] > 0]
        if pivot.empty:
            return None

        # county_fips is 5 digits (e.g. 23001), we need last 3 for 'county' (e.g. 001)
        pivot["county"] = pivot["county_fips"].str[-3:]
        pivot["partisan_score"] = pivot["DEMOCRAT"] / pivot["total_votes"]

        return pivot[["county", "partisan_score"]]

    def _ensure_dataset_file(self) -> Optional[str]:
        """
        Downloads the dataset if necessary and returns the local path.
        """
        cache_path = os.path.join(self.cache_dir, self.COUNTY_FILE_LABEL)
        if os.path.exists(cache_path):
            if os.path.getsize(cache_path) > 0:
                return cache_path
            # Corrupt empty file
            os.remove(cache_path)

        print(f"Downloading partisan data from {self.FILE_URL}...")
        try:
            response = requests.get(self.FILE_URL, stream=True, timeout=120)
            response.raise_for_status()
            with open(cache_path, "wb") as outfile:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        outfile.write(chunk)
            return cache_path
        except Exception as exc:
            print(f"Failed to download partisan data: {exc}")
            return None
