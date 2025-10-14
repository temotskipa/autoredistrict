# Congressional Redistricting Application

This application allows users to automatically generate congressional district maps for any US state. It provides options for different redistricting algorithms and criteria, and it fetches the latest census data to ensure accuracy.

## Features

-   **Automatic Redistricting:** Generate congressional district maps based on user-defined criteria.
-   **Multiple Algorithms:** Choose between a "fair" redistricting algorithm (Divide and Conquer) and a gerrymandering algorithm.
-   **Customizable Criteria:** Adjust parameters like population equality and compactness.
-   **VRA Compliance:** Enable or disable Voting Rights Act compliance.
-   **Data Fetching:** Automatically fetches the latest census data and shapefiles.
-   **Progress Feedback:** A progress bar provides real-time feedback during the map generation process.
-   **Export Options:** Export the generated maps as PNG images or as shapefiles for use in GIS software.

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Install the dependencies:**
    Make sure you have Python 3 installed. Then, run the following command to install the necessary libraries:
    ```bash
    pip install -r requirements.txt
    ```

## How to Run

To run the application, execute the `main.py` file:
```bash
python main.py
```

This will open the main application window, where you can select a state, choose an algorithm, and generate a district map.
