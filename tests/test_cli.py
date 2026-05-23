import json
from pathlib import Path

from src.cli import main


def test_cli_demo_writes_map_and_report_with_seed(tmp_path: Path):
    map_path = tmp_path / "demo.png"
    report_path = tmp_path / "plan.json"

    main(
        [
            "demo",
            "--demo",
            "--mode",
            "demo",
            "--districts",
            "4",
            "--seed",
            "11",
            "--map-out",
            str(map_path),
            "--report-out",
            str(report_path),
            "--quiet",
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert map_path.exists()
    assert report["solver"]["random_seed"] == 11
    assert report["summary"]["district_count"] == 4
    assert report["summary"]["all_contiguous"] is True
