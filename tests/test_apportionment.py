from src.core.apportionment import APPORTIONMENT_2020, get_apportioned_seats


def test_apportionment_2020_has_50_states_summing_to_435():
    assert len(APPORTIONMENT_2020) == 50
    assert sum(APPORTIONMENT_2020.values()) == 435
    assert all(isinstance(code, str) and len(code) == 2 and code.isdigit() for code in APPORTIONMENT_2020)
    assert all(seats >= 1 for seats in APPORTIONMENT_2020.values())


def test_get_apportioned_seats_returns_official_2020_counts():
    assert get_apportioned_seats("30") == 2  # Montana
    assert get_apportioned_seats("44") == 2  # Rhode Island
    assert get_apportioned_seats("01") == 7  # Alabama
    assert get_apportioned_seats("56") == 1  # Wyoming
    assert get_apportioned_seats("06") == 52  # California
    assert get_apportioned_seats("48") == 38  # Texas


def test_get_apportioned_seats_returns_none_outside_table():
    assert get_apportioned_seats("11") is None  # District of Columbia
    assert get_apportioned_seats("72") is None  # Puerto Rico
    assert get_apportioned_seats("00") is None  # Demo dataset
    assert get_apportioned_seats(None) is None
    assert get_apportioned_seats("") is None
