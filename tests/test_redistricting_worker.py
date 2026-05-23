from pathlib import Path

from src.core.utils import is_contiguous
from src.services.redistricting_service import build_demo_dataset
from src.workers.redistricting_worker import RedistrictingWorker


def test_redistricting_worker_uses_graph_solver_and_preserves_coi(tmp_path: Path):
    data = build_demo_dataset(size=4)
    coi_path = tmp_path / "coi.csv"
    coi_path.write_text("GEOID\n0000000\n0000001\n0000002\n", encoding="utf-8")
    progress = []
    finished = []
    errors = []

    worker = RedistrictingWorker(
        data,
        4,
        "Divide and Conquer (Fair)",
        population_equality_weight=1.0,
        compactness_weight=1.0,
        vra_compliance=False,
        communities_of_interest=str(coi_path),
        progress_callback=progress.append,
        finished_callback=finished.append,
        error_callback=errors.append,
    )

    worker.run()

    assert errors == []
    assert progress[-1] == 100
    assert len(finished) == 1
    districts = finished[0]
    assert len(districts) == 4
    assert all(is_contiguous(district) for district in districts)

    coi_districts = {
        district_id
        for district_id, district in enumerate(districts)
        if set(district["GEOID"]) & {"0000000", "0000001", "0000002"}
    }
    assert len(coi_districts) == 1
