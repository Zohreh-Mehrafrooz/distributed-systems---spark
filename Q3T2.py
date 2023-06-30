import multiprocessing
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

# Reference: for this assignment I used some parts of the code from comp6231 labs

# set the global variables here
number_of_rows = 6311871
number_of_processes = 4 # multiprocessing.cpu_count()


def map_tasks(reading_info: list, data: str = 'C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2'
                                              '/Combined_Flights_2021.csv'):
    df = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    df = df[df.iloc[:, 34] == 'Nashville, TN']
    df = df[df.iloc[:, 42] == 'Chicago, IL']
    average_airtime = df.iloc[:, 12].mean()
    return average_airtime


def reduce_task(mapping_output: list):
    average_airtime_all = 0
    for out in tqdm(mapping_output):
        average_airtime_all += out
    print(f'average airtime: {average_airtime_all/number_of_processes}.')


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
    print('Q3: What was the average airtime for flights that were flying'
          ' from Nashville to Chicago? (T2. Multi-processing)')
    compute_multiprocessing()