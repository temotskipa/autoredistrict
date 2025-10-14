# Congressional Redistricting Application

This application allows users to automatically generate congressional district maps for any US state. It has been refactored for high-performance computing, offering both GPU-accelerated and enhanced multi-core CPU execution paths.

## Features

-   **High-Performance Redistricting:**
    -   **GPU Acceleration:** For systems with a compatible NVIDIA GPU, the application uses the RAPIDS suite (`cudf`, `cuspatial`) to deliver massive performance gains.
    -   **Enhanced CPU Parallelism:** For systems without a GPU, the application falls back to a highly parallelized CPU mode using `dask` to ensure efficient use of all available CPU cores.
-   **Automatic Redistricting:** Generate congressional district maps based on user-defined criteria.
-   **Multiple Algorithms:** Choose between a "fair" redistricting algorithm (Divide and Conquer) and a gerrymandering algorithm.
-   **Customizable Criteria:** Adjust parameters like population equality and compactness.
-   **VRA Compliance:** Enable or disable Voting Rights Act compliance.
-   **Data Fetching:** Automatically fetches the latest census data and shapefiles.
-   **Export Options:** Export the generated maps as PNG images or as shapefiles for use in GIS software.

## Setup and Installation

### Prerequisites

-   Python 3.8+
-   For **GPU Acceleration**:
    -   An NVIDIA GPU with CUDA compute capability 7.0+ (e.g., Volta, Turing, Ampere, Ada Lovelace architecture).
    -   A compatible NVIDIA driver.
    -   A Linux environment (or WSL2 on Windows).

### Installation Steps

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Install the dependencies:**
    The application has different dependencies depending on your hardware. All necessary packages are listed in `requirements.txt`.
    ```bash
    # This will install CPU, GPU, and common dependencies.
    # The RAPIDS libraries (cudf, cuspatial) will be installed from the NVIDIA PyPI index.
    pip install -r requirements.txt
    ```
    **Note:** The RAPIDS libraries are large. The installation may take some time. If you do not have a compatible GPU, the installation will still succeed, but the application will only run in CPU mode.

## How to Run

To run the application, execute the `main.py` file:
```bash
python main.py
```

-   The application will automatically detect if you have a compatible GPU.
-   Use the **"Use GPU Acceleration"** checkbox to switch between the high-performance GPU path and the parallelized CPU path.

This will open the main application window, where you can select a state, choose an algorithm, and generate a district map.
