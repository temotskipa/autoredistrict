#!/usr/bin/env python3
"""Generate provider metadata entries by probing known data sources."""
import csv
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

ORG = "openelections"
TARGET_YEARS = [2022]

SESSION = requests.Session()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {TOKEN}"})
def load_metadata() -> List[Dict]:
    if not METADATA_PATH.exists():
        return []
    with open(METADATA_PATH, "r") as fp:
        return yaml.safe_load(fp) or []


def save_metadata(data: List[Dict]):
    with open(METADATA_PATH, "w") as fp:
        yaml.safe_dump(data, fp, sort_keys=False)
def download_metadata(url: str):
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return None
    data = resp.content
    hasher = hashlib.sha256(data)
    size = len(data)
    text = data.decode("utf-8", errors="ignore")
    reader = csv.reader(text.splitlines())
    try:
        header = next(reader)
    except StopIteration:
        return None
    header_lower = [h.strip().lower() for h in header]
    county_field = None
    for candidate in ("county", "county_name", "county_label"):
        if candidate in header_lower:
            county_field = header[header_lower.index(candidate)]
            break
    if not county_field:
        return None
    party_field = None
    for candidate in ("party", "party_simplified", "party_detailed"):
        if candidate in header_lower:
            party_field = header[header_lower.index(candidate)]
            break
    if not party_field:
        return None
    vote_fields = []
    if "votes" in header_lower:
        vote_fields = [header[header_lower.index("votes")]]
    else:
        vote_fields = [col for col in header if col.lower().endswith("_votes")]
    if not vote_fields:
        return None
    return {
        "file_hash": hasher.hexdigest(),
        "file_size": size,
        "county_field": county_field,
        "party_field": party_field,
        "vote_fields": vote_fields,
    }


def list_repos(session):
    url = f"https://api.github.com/orgs/{ORG}/repos?per_page=100"
    while url:
        resp = session.get(url, timeout=15)
        if resp.status_code == 403:
            raise RuntimeError("GitHub rate limit exceeded. Set GITHUB_TOKEN.")
        resp.raise_for_status()
        data = resp.json()
        for repo in data:
            name = repo.get("name", "")
            if not name.startswith("openelections-data-"):
                continue
            suffix = name.rsplit("-", 1)[-1]
            if len(suffix) != 2:
                continue
            yield repo["owner"]["login"], name, suffix.upper()
        url = resp.links.get("next", {}).get("url")


def discover_openelections() -> List[Dict]:
    entries = []
    repo_list = list(list_repos(SESSION))
    for owner, repo, state_abbr in repo_list:
        for year in TARGET_YEARS:
            api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{year}"
            try:
                resp = SESSION.get(api_url, timeout=15)
                if resp.status_code == 404:
                    continue
                if resp.status_code == 403:
                    raise RuntimeError("GitHub rate limit exceeded. Set GITHUB_TOKEN.")
                resp.raise_for_status()
            except requests.RequestException:
                continue
            for item in resp.json():
                name = item.get("name", "")
                if not name.endswith(".csv"):
                    continue
                match = re.match(r"(\\d{8})__([a-z]{2})__([a-z0-9_]+)__([a-z0-9_]+)\.csv", name)
                if not match:
                    continue
                _, state, contest_type, granularity = match.groups()
                if state.upper() != state_abbr:
                    continue
                url = item.get("download_url")
                entry = {
                    "state": state_abbr,
                    "contest": contest_type.replace("_", " ").title(),
                    "year": year,
                    "granularity": "precinct" if granularity == "precinct" else granularity,
                    "confidence": "Medium",
                    "url": url,
                    "format": "csv",
                    "parser": "precinct_csv" if granularity == "precinct" else "county_csv",
                    "dem_token": "DEM",
                    "gop_token": "REP",
                }
                entries.append(entry)
    return entries


def main():
    existing = load_metadata()
    lookup = {(item["state"], item["contest"], item.get("year")): item for item in existing}

    discovered = discover_openelections()
    added = 0
    for src in discovered:
        key = (src["state"], src["contest"], src.get("year"))
        metadata = download_metadata(src["url"])
        if not metadata:
            continue
        entry = {
            **src,
            **metadata,
            "provider_key": f"{src['state'].lower()}_{src['contest'].lower().replace(' ', '_')}_{src.get('year', '')}",
            "granularity_rank": 1,
            "base_priority": 60,
            "recency_note": f"Certified {src.get('year')} contest",
        }
        if key in lookup:
            lookup[key].update(entry)
        else:
            lookup[key] = entry
            added += 1

    if not added:
        print("No new entries discovered.")
        save_metadata(list(lookup.values()))
        return

    combined = sorted(lookup.values(), key=lambda x: (x["state"], x.get("year", 0), x["contest"]))
    save_metadata(combined)
    print(f"Added {added} entries to {METADATA_PATH}")


if __name__ == "__main__":
    main()
