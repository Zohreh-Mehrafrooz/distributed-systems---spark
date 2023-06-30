from threading import Thread, Lock
import pandas as pd

# Reference: for this assignment I used some parts of the code from comp6231 labs

# set the global variables here
number_of_rows = 6311871
num_of_threads = 5
reduce_out = {}


def q1_parallel(n_rows: int, skip_rows: int, lock):
    df = pd.read_csv('C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2/Combined_Flights_2021.csv',
                     nrows=n_rows, skiprows=skip_rows, header=None)
    df = df[df.iloc[:, 4] == True]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.year == 2021]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.month == 9]
    df = df.iloc[:, 1].value_counts()
    lock.acquire()
    for key, value in df.to_dict().items():
        if key in reduce_out:
            reduce_out[key] = reduce_out.get(key) + value
        else:
            reduce_out[key] = value
    lock.release()



def thread_function():
    def distribute_rows(n_rows: int, num_threads: int):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows
        for _ in range(1, num_of_threads - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows
        reading_info.append([None, skip_rows])
        return reading_info

    lock = Lock()
    reading_info_fill = distribute_rows(number_of_rows//num_of_threads, num_of_threads)
    thread_handle = []
    for j in range(0, num_of_threads):
        t = Thread(target=q1_parallel,
                   args=(reading_info_fill[j][0], reading_info_fill[j][1], lock))
        thread_handle.append(t)
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()

    max_cancelled_flight_num = max(reduce_out.values())
    max_cancelled_flight_airline = [key for key, value in reduce_out.items() if value == max(reduce_out.values())]
    print(f'{max_cancelled_flight_airline} Airline with '
          f'{max_cancelled_flight_num} cancelled flights.')


if __name__ == "__main__":
    print('Q1: Which Airline had the most canceled flights in September 2021? (T1. Multi-threading)')
    thread_function()
