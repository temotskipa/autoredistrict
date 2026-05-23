from fastapi.testclient import TestClient

from src.api import app


client = TestClient(app)


def test_health_endpoint():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_web_ui_static_contract():
    index = client.get("/")
    app_js = client.get("/static/app.js")

    assert index.status_code == 200
    assert app_js.status_code == 200
    assert 'id="plan-form"' in index.text
    assert 'name="mode"' in index.text
    assert 'name="algorithm"' in index.text
    assert 'name="random_seed"' in index.text
    assert 'id="map-image"' in index.text
    assert 'id="downloads"' in index.text
    assert "optionalInteger" in app_js.text
    assert 'placeholder = "Auto"' in app_js.text
    assert '"/api/plans/state"' in app_js.text


def test_demo_plan_endpoint():
    response = client.post(
        "/api/plans/demo",
        json={"districts": 4, "algorithm": "fair", "grid_size": 4, "random_seed": 3},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["district_count"] == 4
    assert body["solver"]["random_seed"] == 3
    assert body["urls"]["map_png"].endswith("/map.png")
    assert body["urls"]["assignment_csv"].endswith("/assignment.csv")
    assert body["urls"]["metrics_csv"].endswith("/metrics.csv")
    assert len(body["metrics"]) == 4


def test_demo_plan_artifacts_and_report_are_served():
    response = client.post(
        "/api/plans/demo",
        json={"districts": 4, "algorithm": "fair", "grid_size": 4, "random_seed": 5},
    )
    body = response.json()

    report = client.get(f"/api/plans/{body['plan_id']}")
    assert report.status_code == 200
    assert report.json()["plan_id"] == body["plan_id"]

    artifact_expectations = {
        "map_png": "image/png",
        "districts_geojson": "application/geo+json",
        "assignment_csv": "text/csv",
        "metrics_csv": "text/csv",
        "report_json": "application/json",
    }
    for key, expected_type in artifact_expectations.items():
        artifact = client.get(body["urls"][key])
        assert artifact.status_code == 200
        assert artifact.headers["content-type"].startswith(expected_type)


def test_missing_plan_returns_404():
    response = client.get("/api/plans/not-a-plan")

    assert response.status_code == 404


def test_state_plan_endpoint_requires_api_key():
    response = client.post(
        "/api/plans/state",
        json={"state": "ME", "districts": 2, "algorithm": "fair"},
    )

    assert response.status_code == 422
