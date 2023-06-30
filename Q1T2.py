import multiprocessing
import time
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
    df = df[df.iloc[:, 4] == True]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.year == 2021]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.month == 9]
    return df.iloc[:, 1].value_counts()


def reduce_task(mapping_output: list):
    reduce_out = {}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value
    max_cancelled_flight_num = max(reduce_out.values())
    max_cancelled_flight_airline = [key for key, value in reduce_out.items() if value == max(reduce_out.values())]
    print(f'{max_cancelled_flight_airline} Airline with '
          f'{max_cancelled_flight_num} cancelled flights.')


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
    # start_time = time.time()
    print('Q1: Which Airline had the most canceled flights in September 2021? (T2. Multi-processing)')
    compute_multiprocessing()
    end_time = time.time()
    # print("Time taken for multi-processing is: " + str(end_time - start_time))