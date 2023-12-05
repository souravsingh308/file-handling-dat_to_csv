import os
import concurrent.futures
import pandas as pd
from os import walk

from constants import INPUT_FOLDER, OUTPUT_FOLDER


class EmployeesDataETL:
    """
    Class for ETL (Extract, Transform, Load) operations on employee data.

    Attributes:
        input_folder (str): Path to the input folder containing .dat files.
        output_folder (str): Path to the output folder for saving the processed data.
        data (pd.DataFrame): DataFrame to store the processed employee data.
    """

    def __init__(self, input_folder: str = INPUT_FOLDER, output_folder: str = OUTPUT_FOLDER) -> None:
        """
        Initializes the EmployeesDataETL instance.

        Parameters:
            input_folder (str): Path to the input folder containing .dat files.
            output_folder (str): Path to the output folder for saving the processed data.
        """
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.data = None

    def read_file(self, file):
        """
        Reads data from a given file and appends it to the frames list.

        Parameters:
            file (str): Name of the file to be read.
        """
        return pd.read_csv(f'{self.input_folder}/{file}', sep='\t')

    def process_files_concurrently(self):
        """
        Processes multiple files concurrently using threading.
        """
        frames = []
        files = next(walk(self.input_folder), (None, None, []))[2]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Use a list comprehension to submit tasks to the executor
            futures = [executor.submit(self.read_file, file) for file in files]

            # Wait for all tasks to complete
            concurrent.futures.wait(futures)

            # Extract results from completed tasks
            frames = [future.result() for future in futures]

        self.data = pd.concat(frames)

    def clean_data(self):
        """
        Cleans the data by dropping NaN values, removing duplicates, and filling NaNs with zero.
        """
        self.data.dropna(axis=1, inplace=True)
        self.data.drop_duplicates(['id'], inplace=True)
        self.data.fillna(0, inplace=True)

    def process_data(self):
        """
        Processes the data by calculating gross salary, second-highest salary, and average salary.
        """
        self.data['gross_salary'] = self.data['basic_salary'] + self.data['allowances']
        second_highest_salary = self.get_second_highest_salary()
        average_salary = self.get_average_salary()
        self.data.loc[len(self.data.index)] = [
                                                  f"Second Highest Salary = {second_highest_salary}",
                                                  f"Average Salary = {average_salary}"] + [None for i in
                                                                                           range(self.data.shape[1] - 2)
                                                                                           ]

    def get_average_salary(self) -> float:
        """
        Calculates and returns the average salary.

        Returns:
            float: Average salary.
        """
        average_salary = round(self.data['gross_salary'].mean(), 2)
        return average_salary

    def get_second_highest_salary(self) -> float:
        """
        Retrieves and returns the second-highest salary.

        Returns:
            float: Second-highest salary.
        """
        second_highest_salary = self.data['gross_salary'].nlargest(2).iloc[-1]
        return second_highest_salary

    def write_to_csv(self):
        """
        Writes the processed data to a CSV file.

        Returns:
            str: Result of the CSV write operation.
        """
        output_file_path = f'{self.output_folder}/RESULT.csv'
        try:
            res = self.data.to_csv(output_file_path, index=False)
        except OSError:
            if not os.path.exists(OUTPUT_FOLDER):
                print('Cannot save file into a non-existent directory')
                os.makedirs(OUTPUT_FOLDER)
            res = self.data.to_csv(output_file_path, index=False)
        return res

    def run(self):
        """
        Runs the complete ETL process.
        """
        self.process_files_concurrently()
        self.clean_data()
        self.process_data()
        self.write_to_csv()
        print('\n Task completed')


if __name__ == "__main__":
    EmployeesDataETL().run()
