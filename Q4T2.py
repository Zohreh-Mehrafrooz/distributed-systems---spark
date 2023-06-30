import multiprocessing
import time

import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

number_of_rows = 6311871
number_of_processes = 4 # multiprocessing.cpu_count()


def map_tasks(reading_info: list, data: str = 'C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2'
                                              '/Combined_Flights_2021.csv'):
    df = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    df = df[df.iloc[:, 7].isnull()]
    list_of_dates = df.iloc[:, 0].values.tolist()
    return list_of_dates


def reduce_task(mapping_output: list):
    reduce_out = []
    for out in tqdm(mapping_output):
        reduce_out.extend(out)
    print(f'Dates with missing DepTime : {set(reduce_out)}')
    print(f'Number of the list of dates : {len(reduce_out)}')
    print(f'Number of the set of dates : {len(set(reduce_out))}')


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
    start_time = time.time()
    print('Q4: For which date was departure time (DepTime) not recorded/went missing? (T2. Multi-processing)')
    compute_multiprocessing()
    end_time = time.time()
    print("Time taken for multi-processing is: " + str(end_time - start_time))