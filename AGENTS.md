# Agent Guidance

This document provides guidance for AI agents working on this project.

## Project Overview

This project is a congressional redistricting application that automatically generates district maps based on user-provided criteria. The application is built using Python, with PyQt5 for the GUI and GeoPandas for geographic data manipulation.

## Key Modules

-   **`main.py`:** The main entry point of the application. It contains the `MainWindow` class, which defines the GUI and orchestrates the overall workflow.
-   **`data_fetcher.py`:** This module is responsible for fetching census data and shapefiles from the US Census Bureau.
-   **`redistricting_algorithms.py`:** This module contains the implementation of the redistricting algorithms, including the "Divide and Conquer" algorithm and the gerrymandering algorithm.
-   **`map_generator.py`:** This module is responsible for generating map images and exporting district data to shapefiles.
-   **`apportionment.py`:** This module contains the logic for the Huntington-Hill apportionment method.

## Development Guidelines

-   **Code Style:** Please follow the PEP 8 style guide for Python code.
-   **Modularity:** Keep the code modular and well-organized. Each module should have a clear and specific purpose.
-   **Error Handling:** Implement robust error handling, especially for network requests and file operations.
-   **Testing:** While there are no automated tests at the moment, it is highly encouraged to add them in the future.

## Implemented Enhancements

-   **Huntington-Hill Apportionment:** The application now allows users to set the total size of the U.S. House of Representatives and uses the Huntington-Hill method to apportion districts to each state.
-   **VRA Compliance Logic:** The VRA compliance logic has been implemented in the redistricting algorithms to prevent the "cracking" of minority communities.
-   **Enhanced Gerrymandering Algorithm:** The gerrymandering algorithm has been enhanced to use a partisan score to create a partisan advantage.
-   **Support for Communities of Interest:** The application now supports the preservation of communities of interest by allowing users to upload a CSV file of GEOIDs.
-   **Expanded State List:** The state selection dropdown is now populated with a complete list of US states.
-   **Performance Optimization:** The redistricting algorithm has been parallelized to improve performance on multi-core systems.
