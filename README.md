# Employees Data ETL

## Overview

The Employees Data ETL (Extract, Transform, Load) is a Python script designed to process employee data stored in .dat files, perform data cleaning and transformation, and save the results in a CSV file. The script utilizes threading to concurrently read and process multiple input files for improved performance.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Dependencies](#dependencies)
- [License](#license)

## Installation

1. Clone the repository to your local machine:

   ```bash
   git clone https://github.com/souravsingh308/file-handling-dat_to_csv

Navigate to the project directory:

Crete virtual env

Install the required dependencies:

pip install -r requirements.txt

Usage
To run the Employees Data ETL script, execute the following command in the terminal:

python file_processor.py
This will initiate the ETL process, concurrently reading and processing data from .dat files in the specified input folder.

Configuration
You can customize the script behavior by modifying the constants and parameters in the code:

INPUT_FOLDER: Path to the input folder containing .dat files.
OUTPUT_FOLDER: Path to the output folder for saving the processed data.
Ensure that the input folder contains the required .dat files, and the output folder is correctly set for saving the processed data.

Dependencies
The script relies on the following Python libraries:

pandas
concurrent.futures
These dependencies are listed in the requirements.txt file and can be installed using the provided installation instructions.
