# Agent Guidance

This document provides guidance for AI agents working on this project.

## Project Overview

This project is a congressional redistricting application that automatically generates district maps based on user-provided criteria. The application is built using Python, with PyQt5 for the GUI. It has been refactored to support both high-performance GPU acceleration and an efficiently parallelized CPU fallback.

## Architectural Overview

The application now features a dual-path execution model for its core data processing and redistricting logic. A "Use GPU Acceleration" checkbox in the UI controls which path is taken.

-   **GPU Path:** Utilizes the NVIDIA RAPIDS suite (`cudf`, `cuspatial`) for massively parallel data manipulation and geometric calculations on a compatible NVIDIA GPU. This is the preferred path for maximum performance.
-   **CPU Path:** For systems without a compatible GPU, the application falls back to a highly parallelized CPU implementation. This path uses `dask` to efficiently distribute work across multiple CPU cores and `numba` to JIT-compile critical functions.
-   **Hybrid Calculations:** To ensure geographic accuracy, some calculations, like the Polsby-Popper compactness score, use a hybrid approach. Data is temporarily transferred from the GPU to the CPU to leverage the precise `unary_union` operation from `geopandas`.

## Key Modules

-   **`main.py`:** The main entry point. It contains the `MainWindow` class, which defines the GUI, handles user input (including the GPU/CPU selection), and orchestrates the overall workflow.
-   **`worker.py`:** Contains the `DataFetcherWorker`, which runs in a `QThread`. It is responsible for fetching and processing census and shapefile data. It has separate internal logic to handle data loading into `cudf` (for GPU) or `pandas`/`geopandas` (for CPU).
-   **`redistricting_algorithms.py`:** This is the core module. It contains the `RedistrictingAlgorithm` class with dual implementations for both CPU and GPU (`_cpu` and `_gpu` suffixed methods).
    -   The **GPU implementation** uses `cudf`, `cuspatial`, and a hybrid approach for compactness.
    -   The **CPU implementation** uses `geopandas` and `dask` for parallel execution.
-   **`redistricting_worker.py`:** Contains the `RedistrictingWorker`, which runs the `RedistrictingAlgorithm` in a `QThread`, passing the user's `use_gpu` selection to it.
-   **`data_fetcher.py` & `apportionment.py`:** These modules handle the initial national-level apportionment calculations, which are CPU-based.
-   **`map_generator.py`:** This module generates map images from the final `geopandas` GeoDataFrame.

## Development Guidelines

-   **Code Style:** Please follow the PEP 8 style guide for Python code.
-   **Modularity:** The dual-path architecture should be maintained. Keep GPU and CPU logic separate where possible within `redistricting_algorithms.py`.
-   **Error Handling:** Implement robust error handling, especially for GPU memory management and data transfers.
-   **Dependencies:** The project has significant new dependencies. The GPU path requires a specific CUDA version and the RAPIDS libraries. The CPU path requires `dask`. See `requirements.txt` for details.

## Implemented Enhancements

-   **GPU Acceleration:** The entire data processing and redistricting pipeline has been refactored to run on NVIDIA GPUs using `cudf` and `cuspatial`, offering a significant performance increase.
-   **Enhanced CPU Parallelism:** The CPU fallback path has been upgraded from `multiprocessing` to `dask`, providing more efficient and scalable multi-core performance.
-   **Hybrid Accuracy Model:** A hybrid CPU/GPU approach is used for critical geometric calculations to ensure accuracy is not sacrificed for performance.
-   **Huntington-Hill Apportionment:** The application now allows users to set the total size of the U.S. House of Representatives and uses the Huntington-Hill method to apportion districts to each state.
-   **VRA Compliance Logic:** The VRA compliance logic has been implemented in the redistricting algorithms to prevent the "cracking" of minority communities.
-   **Support for Communities of Interest:** The application now supports the preservation of communities of interest by allowing users to upload a CSV file of GEOIDs.

## API Usage Notes

-   **Census API for Block-Level Data:** When fetching block-level data from the Census API, it is required to specify both the state and county FIPS codes.
