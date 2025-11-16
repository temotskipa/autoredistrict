#!/usr/bin/env python3
"""Generate provider metadata entries by probing known data sources."""
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Dict, List

import requests
import yaml

ROOT = Path(__file__).resolve().parent.parent
METADATA_PATH = ROOT / "data" / "provider_sources.yaml"
CACHE_DIR = ROOT / ".cache" / "provider_probe"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OPEN_ELECTIONS_REPOS = [
    ("openelections", "openelections-data-ga"),
    ("openelections", "openelections-data-va"),
]
def load_metadata() -> List[Dict]:
    if not METADATA_PATH.exists():
        return []
    with open(METADATA_PATH, "r") as fp:
        return yaml.safe_load(fp) or []


def save_metadata(data: List[Dict]):
    with open(METADATA_PATH, "w") as fp:
        yaml.safe_dump(data, fp, sort_keys=False)
def validate_source(url: str) -> bool:
    try:
        resp = requests.get(url, stream=True, timeout=15)
        resp.raise_for_status()
        size = 0
        for chunk in resp.iter_content(1024 * 1024):
            if not chunk:
                break
            size += len(chunk)
            if size > 1024 * 1024:
                break
        return size > 0
    except requests.RequestException:
        return False


def discover_openelections() -> List[Dict]:
    entries = []
    session = requests.Session()
    for owner, repo in OPEN_ELECTIONS_REPOS:
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/2022"
        try:
            resp = session.get(api_url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException:
            continue
        for item in resp.json():
            name = item.get("name", "")
            if not name.endswith(".csv"):
                continue
            match = re.match(r"(\\d{8})__([a-z]{2})__([a-z_]+)__([a-z_]+).csv", name)
            if not match:
                continue
            date, state, contest_type, granularity = match.groups()
            year = int(date[:4])
            state_abbr = state.upper()
            url = item.get("download_url")
            entries.append({
                "state": state_abbr,
                "contest": contest_type.replace("_", " ").title(),
                "year": year,
                "granularity": "county" if "precinct" in granularity else granularity,
                "confidence": "High",
                "url": url,
                "format": "csv",
                "parser": "precinct_csv" if "precinct" in granularity else "county_csv",
                "county_field": "county",
                "party_field": "party",
                "vote_fields": ["votes"] if "precinct" not in granularity else [
                    "election_day_votes", "advanced_votes", "absentee_by_mail_votes", "provisional_votes"
                ],
                "dem_token": "DEM",
                "gop_token": "REP",
            })
    return entries


def main():
    existing = load_metadata()
    existing_keys = {(item["state"], item["contest"], item.get("year")) for item in existing}

    discovered = discover_openelections()
    new_entries = []
    for src in discovered:
        key = (src["state"], src["contest"], src.get("year"))
        if key in existing_keys:
            continue
        if not validate_source(src["url"]):
            continue
        entry = {
            **src,
            "provider_key": f"{src['state'].lower()}_{src['contest'].lower().replace(' ', '_')}_{src.get('year', '')}",
            "granularity_rank": 1 if src.get("granularity") == "county" else 2,
            "base_priority": 60,
            "recency_note": f"Certified {src.get('year')} contest",
        }
        new_entries.append(entry)

    if not new_entries:
        print("No new entries discovered.")
        return

    combined = existing + new_entries
    save_metadata(combined)
    print(f"Added {len(new_entries)} entries to {METADATA_PATH}")


if __name__ == "__main__":
    main()
