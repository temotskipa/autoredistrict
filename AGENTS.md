# Agent Guidance

This document provides guidance for AI agents working on this project.

## Project Overview

This project is a congressional redistricting application that automatically generates district maps based on user-provided criteria. The application is built using Python, with PyQt5 for the GUI and GeoPandas for geographic data manipulation.

## Key Modules

-   **`main.py`:** The main entry point of the application. It contains the `MainWindow` class, which defines the GUI and orchestrates the overall workflow.
-   **`data_fetcher.py`:** This module is responsible for fetching census data and shapefiles from the US Census Bureau.
-   **`redistricting_algorithms.py`:** This module contains the implementation of the redistricting algorithms, including the "Divide and Conquer" algorithm and the gerrymandering algorithm.
-   **`map_generator.py`:** This module is responsible for generating map images and exporting district data to shapefiles.

## Development Guidelines

-   **Code Style:** Please follow the PEP 8 style guide for Python code.
-   **Modularity:** Keep the code modular and well-organized. Each module should have a clear and specific purpose.
-   **Error Handling:** Implement robust error handling, especially for network requests and file operations.
-   **Testing:** While there are no automated tests at the moment, it is highly encouraged to add them in the future.

## Future Enhancements

-   **More Sophisticated Algorithms:** The current gerrymandering algorithm is a simplified placeholder. It could be replaced with a more advanced implementation of "packing and cracking."
-   **VRA Compliance Logic:** The VRA compliance is currently a boolean flag. The actual logic for ensuring VRA compliance needs to be implemented in the redistricting algorithms.
-   **Additional Criteria:** The application could be extended to support other redistricting criteria, such as preserving communities of interest.
-   **Expanded State List:** The state selection dropdown currently only lists California. This should be expanded to include all US states.
-   **Performance Optimization:** For larger states, the redistricting process can be slow. The performance of the algorithms could be optimized.
