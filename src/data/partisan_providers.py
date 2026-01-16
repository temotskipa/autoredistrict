import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd
import requests
import us
import yaml

from .partisan_data import CountyPresidentialReturnsProvider

AVAILABLE_PARTISAN_YEARS = [2000, 2004, 2008, 2012, 2016, 2020, 2024]
DEFAULT_PARTISAN_YEAR = 2020

# Harvard Dataverse US House 2018 precinct-level dataset
HARVARD_2018_FILE_URL = "https://dataverse.harvard.edu/api/access/datafile/3814252"
HARVARD_2018_CACHE = Path(".cache") / "harvard_house" / "us-house-precinct-2018.zip"


@dataclass(frozen=True)
class ProviderMetadata:
    key: str
    label: str
    granularity: str  # 'precinct', 'county'
    confidence: str  # 'High', 'Medium', 'Low'
    description: str
    supports_year_selection: bool
    available_years: Optional[List[int]]
    granularity_rank: int  # lower is better (precinct < county)
    base_priority: int = 100
    recency_note: str = ""
    fetcher_key: str = ""


_county_returns_provider = CountyPresidentialReturnsProvider()

MEDSL_BASE_URL = "https://dataverse.harvard.edu/api/access/datafile"
MEDSL_STATE_FILES: Dict[str, Dict[str, str]] = {
    "AL": {"id": 6100422, "ext": ".tab"},
    "AK": {"id": 6100421, "ext": ".tab"},
    "AZ": {"id": 6100398, "ext": ".tab"},
    "AR": {"id": 6100441, "ext": ".tab"},
    "CA": {"id": 6970332, "ext": ".csv"},
    "CO": {"id": 6100407, "ext": ".tab"},
    "CT": {"id": 6100396, "ext": ".tab"},
    "DC": {"id": 6100424, "ext": ".tab"},
    "DE": {"id": 6100418, "ext": ".tab"},
    "FL": {"id": 6100417, "ext": ".tab"},
    "GA": {"id": 6100409, "ext": ".tab"},
    "HI": {"id": 6100428, "ext": ".tab"},
    "IA": {"id": 6100439, "ext": ".tab"},
    "ID": {"id": 6100403, "ext": ".tab"},
    "IL": {"id": 6100402, "ext": ".csv"},
    "IN": {"id": 6593272, "ext": ".tab"},
    "KS": {"id": 6100436, "ext": ".tab"},
    "KY": {"id": 6100401, "ext": ".tab"},
    "LA": {"id": 6100448, "ext": ".tab"},
    "MA": {"id": 6100429, "ext": ".tab"},
    "MD": {"id": 6100445, "ext": ".tab"},
    "ME": {"id": 6100411, "ext": ".tab"},
    "MI": {"id": 6100399, "ext": ".tab"},
    "MN": {"id": 6100425, "ext": ".tab"},
    "MO": {"id": 6100413, "ext": ".tab"},
    "MS": {"id": 6100414, "ext": ".tab"},
    "MT": {"id": 6100404, "ext": ".tab"},
    "NC": {"id": 6100444, "ext": ".csv"},
    "ND": {"id": 6100438, "ext": ".tab"},
    "NE": {"id": 6100397, "ext": ".tab"},
    "NH": {"id": 6100419, "ext": ".tab"},
    "NJ": {"id": 6100406, "ext": ".tab"},
    "NM": {"id": 6100416, "ext": ".tab"},
    "NV": {"id": 6100415, "ext": ".tab"},
    "NY": {"id": 6100433, "ext": ".tab"},
    "OH": {"id": 6100400, "ext": ".tab"},
    "OK": {"id": 6100437, "ext": ".tab"},
    "OR": {"id": 10244629, "ext": ".tab"},
    "PA": {"id": 6100405, "ext": ".tab"},
    "RI": {"id": 6100440, "ext": ".tab"},
    "SC": {"id": 6100427, "ext": ".tab"},
    "SD": {"id": 6100420, "ext": ".tab"},
    "TN": {"id": 6100423, "ext": ".tab"},
    "TX": {"id": 6100412, "ext": ".tab"},
    "UT": {"id": 6100430, "ext": ".tab"},
    "VA": {"id": 6100432, "ext": ".tab"},
    "VT": {"id": 6100442, "ext": ".tab"},
    "WA": {"id": 6100434, "ext": ".tab"},
    "WI": {"id": 6100446, "ext": ".tab"},
    "WV": {"id": 6100431, "ext": ".tab"},
    "WY": {"id": 6100426, "ext": ".tab"},
}


def _fetch_county_returns(state_fips: str, election_year: Optional[int]) -> Optional[pd.DataFrame]:
    year = election_year or DEFAULT_PARTISAN_YEAR
    return _county_returns_provider.get_state_scores(state_fips, year)


def _fetch_medsl_state_returns(state_fips: str, _unused_year: Optional[int]) -> Optional[pd.DataFrame]:
    state = us.states.lookup(state_fips)
    if not state:
        return None
    info = MEDSL_STATE_FILES.get(state.abbr)
    if not info:
        return None
    cache_dir = Path(".cache") / "medsl_state" / state.abbr.lower()
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_file = cache_dir / f"{state.abbr.lower()}_{info['id']}{info['ext']}"
    if not local_file.exists():
        url = f"{MEDSL_BASE_URL}/{info['id']}"
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            with open(local_file, "wb") as outfile:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        outfile.write(chunk)
        except requests.RequestException as exc:
            print(f"Failed to download MEDSL state file for {state.abbr}: {exc}")
            return None
    delimiter = "\t" if info['ext'] == ".tab" else ","
    try:
        medsl_df = pd.read_csv(local_file, sep=delimiter, dtype={'county_fips': str})
    except Exception as exc:
        print(f"Unable to parse MEDSL state file for {state.abbr}: {exc}")
        return None
    presidential = medsl_df[
        medsl_df['office'].str.contains('PRESIDENT', case=False, na=False)
    ]
    if 'party_simplified' in presidential.columns:
        party_col = 'party_simplified'
    else:
        party_col = 'party'
    presidential = presidential[presidential[party_col].isin(['DEMOCRAT', 'REPUBLICAN'])]
    if presidential.empty:
        return None
    vote_col = 'votes' if 'votes' in presidential.columns else 'candidatevotes'
    grouped = (
        presidential.groupby(['county_fips', party_col])[vote_col]
        .sum()
        .unstack(fill_value=0)
    )
    if 'DEMOCRAT' not in grouped.columns:
        grouped['DEMOCRAT'] = 0
    if 'REPUBLICAN' not in grouped.columns:
        grouped['REPUBLICAN'] = 0
    grouped['total'] = grouped.sum(axis=1)
    grouped = grouped[grouped['total'] > 0]
    grouped['partisan_score'] = grouped['DEMOCRAT'] / grouped['total']
    result = grouped[['partisan_score']].reset_index()
    result.rename(columns={'county_fips': 'county'}, inplace=True)
    result['county'] = result['county'].astype(str).str.zfill(3)
    return result


def _ensure_harvard_house_zip() -> Optional[Path]:
    HARVARD_2018_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if HARVARD_2018_CACHE.exists():
        return HARVARD_2018_CACHE
    try:
        response = requests.get(HARVARD_2018_FILE_URL, stream=True, timeout=60)
        response.raise_for_status()
        with open(HARVARD_2018_CACHE, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    outfile.write(chunk)
        return HARVARD_2018_CACHE
    except requests.RequestException as exc:
        print(f"Failed to download Harvard 2018 dataset: {exc}")
        return None


def _fetch_harvard_house_2018(state_fips: str, election_year: Optional[int]) -> Optional[pd.DataFrame]:
    if election_year is not None and election_year != 2018:
        return None
    state = us.states.lookup(state_fips)
    if not state:
        return None
    zip_path = _ensure_harvard_house_zip()
    if not zip_path:
        return None
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            with archive.open("national-files/us-house-wide.csv") as file_obj:
                df = pd.read_csv(file_obj, dtype={"fipscode": float})
    except Exception as exc:
        print(f"Unable to read Harvard 2018 dataset: {exc}")
        return None
    df_state = df[df["state"] == state.abbr]
    if df_state.empty:
        return None
    df_state = df_state[pd.notna(df_state["fipscode"])]
    if df_state.empty:
        return None
    df_state["county_fips"] = df_state["fipscode"].astype(int).astype(str).str.zfill(5)
    df_state["county"] = df_state["county_fips"].str[-3:]
    df_state["votes_DEM"] = pd.to_numeric(df_state.get("dem"), errors="coerce").fillna(0)
    df_state["votes_GOP"] = pd.to_numeric(df_state.get("rep"), errors="coerce").fillna(0)
    grouped = (
        df_state.groupby("county")[["votes_DEM", "votes_GOP"]]
        .sum()
        .reset_index()
    )
    grouped["total"] = grouped["votes_DEM"] + grouped["votes_GOP"]
    grouped = grouped[grouped["total"] > 0]
    if grouped.empty:
        return None
    grouped["partisan_score"] = grouped["votes_DEM"] / grouped["total"]
    return grouped[["county", "partisan_score"]]


def parse_precinct_csv(entry, state_fips: str, election_year: Optional[int]) -> Optional[pd.DataFrame]:
    if entry.get("state_fips") and entry["state_fips"] != state_fips:
        return None
    if election_year is not None and entry.get("year") and entry["year"] != election_year:
        return None
    state = us.states.lookup(state_fips)
    if not state:
        return None
    cache_dir = Path(".cache") / "metadata_sources" / entry.get("provider_key", "source")
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_file = cache_dir / Path(entry["url"]).name
    if not local_file.exists():
        try:
            response = requests.get(entry["url"], stream=True, timeout=60)
            response.raise_for_status()
            with open(local_file, "wb") as outfile:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        outfile.write(chunk)
        except requests.RequestException as exc:
            print(f"Failed to fetch metadata provider {entry.get('provider_key')}: {exc}")
            return None
    try:
        df = pd.read_csv(local_file)
    except Exception as exc:
        print(f"Unable to parse metadata provider CSV {entry.get('provider_key')}: {exc}")
        return None
    county_col = entry.get("county_field", "county")
    if county_col not in df.columns:
        return None
    party_col = entry.get("party_field", "party")
    if party_col not in df.columns:
        return None
    vote_fields = entry.get("vote_fields", ["votes"])
    for col in vote_fields:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["total_votes"] = df[vote_fields].sum(axis=1)
    df = df[df["total_votes"] > 0]
    if df.empty:
        return None
    county_lookup = {county.name.upper(): county.fips for county in state.counties}
    df["county_fips"] = df[county_col].astype(str).str.upper().map(county_lookup)
    df = df[pd.notna(df["county_fips"])]
    if df.empty:
        return None
    party_series = df[party_col].astype(str).str.upper()
    dem_token = entry.get("dem_token", "DEM").upper()
    gop_token = entry.get("gop_token", "REP").upper()
    dem_mask = party_series.str.contains(dem_token)
    gop_mask = party_series.str.contains(gop_token)
    dem_votes = df[dem_mask].groupby("county_fips")["total_votes"].sum()
    gop_votes = df[gop_mask].groupby("county_fips")["total_votes"].sum()
    grouped = pd.DataFrame({"votes_DEM": dem_votes, "votes_GOP": gop_votes}).fillna(0)
    grouped = grouped[(grouped["votes_DEM"] + grouped["votes_GOP"]) > 0]
    if grouped.empty:
        return None
    grouped["county"] = grouped.index.astype(str).str[-3:]
    grouped["partisan_score"] = grouped["votes_DEM"] / (grouped["votes_DEM"] + grouped["votes_GOP"])
    return grouped.reset_index(drop=True)[["county", "partisan_score"]]


FETCHER_MAP: Dict[str, Callable[[str, Optional[int]], Optional[pd.DataFrame]]] = {
    "county_presidential": _fetch_county_returns,
    "medsl_state_2020": _fetch_medsl_state_returns,
    "harvard_house_2018": _fetch_harvard_house_2018,
}

PROVIDER_REGISTRY: Dict[str, ProviderMetadata] = {
    "county_presidential": ProviderMetadata(
        key="county_presidential",
        label="County Presidential Returns (Harvard Dataverse)",
        granularity="county",
        confidence="High",
        description="Certified presidential results aggregated to counties (2000-2024).",
        supports_year_selection=True,
        available_years=AVAILABLE_PARTISAN_YEARS,
        granularity_rank=2,
        base_priority=100,
        recency_note="Supports any general election year from 2000 through 2024.",
        fetcher_key="county_presidential",
    ),
    "medsl_state_2020": ProviderMetadata(
        key="medsl_state_2020",
        label="MEDSL State Returns (2020)",
        granularity="county",
        confidence="High",
        description="State-level presidential returns published by MEDSL for 2020, downloaded per state.",
        supports_year_selection=False,
        available_years=[2020],
        granularity_rank=1,
        base_priority=50,
        recency_note="Certified 2020 general election",
        fetcher_key="medsl_state_2020",
    ),
    "harvard_house_2018": ProviderMetadata(
        key="harvard_house_2018",
        label="Harvard Dataverse House Returns (2018)",
        granularity="county",
        confidence="Medium",
        description="County-level US House results from the Harvard Dataverse 2018 general dataset.",
        supports_year_selection=False,
        available_years=[2018],
        granularity_rank=2,
        base_priority=120,
        recency_note="Certified 2018 US House results",
        fetcher_key="harvard_house_2018",
    ),
}

GLOBAL_PROVIDER_KEYS: List[str] = ["county_presidential", "harvard_house_2018"]

METADATA_PARSER_DISPATCH = {
    "precinct_csv": parse_precinct_csv,
}


def _register_metadata_providers():
    for entry in _load_metadata_providers():
        parser_name = entry.get("parser")
        parser = METADATA_PARSER_DISPATCH.get(parser_name)
        if not parser:
            continue
        provider_key = entry["provider_key"]
        available_years = [entry["year"]] if entry.get("year") else None

        def fetcher(state_fips, election_year, entry=entry, parser=parser):
            return parser(entry, state_fips, election_year)

        FETCHER_MAP[provider_key] = fetcher
        PROVIDER_REGISTRY[provider_key] = ProviderMetadata(
            key=provider_key,
            label=f"{entry.get('state')} {entry.get('contest')} Returns ({entry.get('year')})",
            granularity=entry.get("granularity", "county"),
            confidence=entry.get("confidence", "Medium"),
            description=entry.get("description", "Metadata-defined contest source."),
            supports_year_selection=False,
            available_years=available_years,
            granularity_rank=entry.get("granularity_rank", 2),
            base_priority=entry.get("base_priority", 80),
            recency_note=entry.get("recency_note", ""),
            fetcher_key=provider_key,
        )


def _state_specific_provider_keys(state_fips: Optional[str]) -> List[str]:
    if not state_fips:
        return []
    state = us.states.lookup(state_fips)
    if not state:
        return []
    keys: List[str] = []
    if state.abbr in MEDSL_STATE_FILES:
        keys.append("medsl_state_2020")
    for entry in _load_metadata_providers():
        if entry.get("state_fips") == state.fips:
            keys.append(entry.get("provider_key"))
    return keys


def _year_distance(meta: ProviderMetadata, requested_year: Optional[int]) -> int:
    if requested_year is None or not meta.available_years:
        return 0
    return min(abs(year - requested_year) for year in meta.available_years)


def provider_chain_for_state(state_fips: Optional[str], requested_year: Optional[int],
                             manual_override_key: Optional[str] = None) -> List[ProviderMetadata]:
    """
    Returns an ordered list of provider metadata representing the hierarchy to attempt.
    """
    if manual_override_key:
        meta = PROVIDER_REGISTRY.get(manual_override_key)
        return [meta] if meta else []

    candidate_keys = _state_specific_provider_keys(state_fips) + GLOBAL_PROVIDER_KEYS
    candidates: List[tuple] = []
    for key in candidate_keys:
        meta = PROVIDER_REGISTRY.get(key)
        if not meta:
            continue
        year_distance = _year_distance(meta, requested_year)
        score = (meta.granularity_rank, year_distance, meta.base_priority)
        candidates.append((score, meta))

    candidates.sort(key=lambda item: item[0])
    ordered: List[ProviderMetadata] = []
    seen = set()
    for _, meta in candidates:
        if meta.key in seen:
            continue
        seen.add(meta.key)
        ordered.append(meta)

    if not ordered:
        fallback = PROVIDER_REGISTRY.get("county_presidential")
        if fallback:
            ordered.append(fallback)

    return ordered


def get_provider_metadata(key: str) -> Optional[ProviderMetadata]:
    return PROVIDER_REGISTRY.get(key)


def available_manual_providers(state_fips: Optional[str], requested_year: Optional[int]) -> List[ProviderMetadata]:
    """
    Returns all providers applicable to the given state for manual override selection.
    """
    auto_chain = provider_chain_for_state(state_fips, requested_year)
    return auto_chain


def fetch_scores_for_provider(meta: ProviderMetadata, state_fips: str, election_year: Optional[int]) -> Optional[
    pd.DataFrame]:
    fetcher = FETCHER_MAP.get(meta.fetcher_key)
    if not fetcher:
        return None
    return fetcher(state_fips, election_year if meta.supports_year_selection else None)


def allocate_partisan_to_geoid(base_df: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder kept for API compatibility; block/tract allocation handled in DataFetcherWorker.
    """
    return base_df


def _load_metadata_providers():
    metadata_path = Path("data/provider_sources.yaml")
    if not metadata_path.exists():
        return []
    try:
        with open(metadata_path, "r") as file_obj:
            entries = yaml.safe_load(file_obj) or []
    except Exception as exc:
        print(f"Unable to load provider metadata: {exc}")
        return []
    valid_entries = []
    for entry in entries:
        required = ("state", "provider_key", "url")
        if not all(field in entry for field in required):
            continue
        state = us.states.lookup(entry["state"])
        if not state:
            continue
        entry["state_fips"] = state.fips
        valid_entries.append(entry)
    return valid_entries


_register_metadata_providers()
