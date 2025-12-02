import math

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
