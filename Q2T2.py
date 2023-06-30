import multiprocessing
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

# Reference: for this assignment I used some parts of the code from comp6231 labs

# set the global variables here
number_of_rows = 6311871
number_of_processes = multiprocessing.cpu_count()


def map_tasks(reading_info: list, data: str = 'C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2'
                                              '/Combined_Flights_2021.csv'):
    df = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    df = df[df.iloc[:, 5] == True]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.year == 2021]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.month == 11]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.day <= 30]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.day >= 20]
    return df.iloc[:, 5].count()


def reduce_task(mapping_output: list):
    all_diverted_flights = 0
    for out in tqdm(mapping_output):
        all_diverted_flights += out
    print(f'diverted flights: {all_diverted_flights}')


def compute_multiprocessing():
    def distribute_rows(n_rows: int, n_processes):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        for _ in range(1, n_processes - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows

        reading_info.append([None, skip_rows])
        return reading_info

    processes = number_of_processes
    p = Pool(processes=processes)
    result = p.map(map_tasks, distribute_rows(n_rows=number_of_rows//processes, n_processes=processes))
    reduce_task(result)
    p.close()
    p.join()


if __name__ == '__main__':
    print('Q2: How many flights were diverted between the period of '
          '20th-30th November 2021? (T2. Multi-processing)')
    compute_multiprocessing()