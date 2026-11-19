import math
from typing import Optional

# Official 2020 Census House apportionment announced by the U.S. Census Bureau
# on April 26, 2021 (effective for the 2022 elections), keyed by 2-digit FIPS.
APPORTIONMENT_2020: dict[str, int] = {
    "01": 7,   # Alabama
    "02": 1,   # Alaska
    "04": 9,   # Arizona
    "05": 4,   # Arkansas
    "06": 52,  # California
    "08": 8,   # Colorado
    "09": 5,   # Connecticut
    "10": 1,   # Delaware
    "12": 28,  # Florida
    "13": 14,  # Georgia
    "15": 2,   # Hawaii
    "16": 2,   # Idaho
    "17": 17,  # Illinois
    "18": 9,   # Indiana
    "19": 4,   # Iowa
    "20": 4,   # Kansas
    "21": 6,   # Kentucky
    "22": 6,   # Louisiana
    "23": 2,   # Maine
    "24": 8,   # Maryland
    "25": 9,   # Massachusetts
    "26": 13,  # Michigan
    "27": 8,   # Minnesota
    "28": 4,   # Mississippi
    "29": 8,   # Missouri
    "30": 2,   # Montana
    "31": 3,   # Nebraska
    "32": 4,   # Nevada
    "33": 2,   # New Hampshire
    "34": 12,  # New Jersey
    "35": 3,   # New Mexico
    "36": 26,  # New York
    "37": 14,  # North Carolina
    "38": 1,   # North Dakota
    "39": 15,  # Ohio
    "40": 5,   # Oklahoma
    "41": 6,   # Oregon
    "42": 17,  # Pennsylvania
    "44": 2,   # Rhode Island
    "45": 7,   # South Carolina
    "46": 1,   # South Dakota
    "47": 9,   # Tennessee
    "48": 38,  # Texas
    "49": 4,   # Utah
    "50": 1,   # Vermont
    "51": 11,  # Virginia
    "53": 10,  # Washington
    "54": 2,   # West Virginia
    "55": 8,   # Wisconsin
    "56": 1,   # Wyoming
}


def get_apportioned_seats(state_fips: Optional[str]) -> Optional[int]:
    """Return official 2020 Census House seats for a 2-digit state FIPS, or None."""
    if not state_fips:
        return None
    normalized = str(state_fips).strip()
    if normalized.isdigit():
        normalized = normalized.zfill(2)
    return APPORTIONMENT_2020.get(normalized)


def calculate_apportionment(state_populations, house_size):
    """
    Calculates the apportionment of House seats to states using the Huntington-Hill method.

    Args:
        state_populations (dict): A dictionary where keys are state FIPS codes and values are populations.
        house_size (int): The total number of seats in the House of Representatives.

    Returns:
        dict: A dictionary where keys are state FIPS codes and values are the number of apportioned seats.
    """
    num_states = len(state_populations)
    if house_size < num_states:
        raise ValueError("House size must be at least the number of states.")

    # Initial allocation: each state gets one seat
    seats = {state: 1 for state in state_populations}
    remaining_seats = house_size - num_states

    # Allocate remaining seats using priority values
    for _ in range(remaining_seats):
        priorities = {}
        for state, pop in state_populations.items():
            n = seats[state]
            priority = pop / math.sqrt(n * (n + 1))
            priorities[state] = priority

        # Find the state with the highest priority
        next_seat_state = max(priorities, key=priorities.get)
        seats[next_seat_state] += 1

    return seats
