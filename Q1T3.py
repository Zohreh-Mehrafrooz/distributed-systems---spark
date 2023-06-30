import time
import pandas as pd
from mpi4py import MPI

# Reference: for this assignment I used some parts of the code from comp6231 labs

number_of_rows = 6311871

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

dataset = 'C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2/Combined_Flights_2021.csv'

if rank == 0:
    start_time = time.time()
    """
    Master worker (with rank 0) is responsible for distributes the workload evenly 
    between slave workers.
    """
    def distribute_rows(n_rows: int, n_workers):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        for _ in range(1, n_workers - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows

        reading_info.append([None, skip_rows])
        return reading_info

    slave_workers = size - 1
    chunk_distribution = distribute_rows(n_rows=number_of_rows//slave_workers, n_workers=slave_workers)

    # distribute tasks to slaves
    for worker in range(1, size):
        chunk_to_process = worker-1
        comm.send(chunk_distribution[chunk_to_process], dest=worker)

    # receive and aggregate results from slave
    results = []
    for worker in (range(1, size)):  # receive
        result = comm.recv(source=worker)
        results.append(result)
        print(f'received from Worker slave {worker}')

    out = {}
    for r in results:
        for key, value in r.to_dict().items():
            if key in out:
                out[key] = out[key] + value
            else:
                out[key] = value

    max_cancelled_flight_num = max(out.values())
    max_cancelled_flight_airline = [key for key, value in out.items() if value == max(out.values())]
    print(f'{max_cancelled_flight_airline} Airline with '
          f'{max_cancelled_flight_num} cancelled flights.')

    # end_time = time.time()
    # print("Time taken for MPI is: " + str(end_time - start_time))

elif rank > 0:
    chunk_to_process = comm.recv()
    print(f'Worker {rank} is assigned chunk info {chunk_to_process} {dataset}')
    df = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
    df = df[df.iloc[:, 4] == True]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.year == 2021]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.month == 9]
    result = df.iloc[:, 1].value_counts()
    print(f'Worker slave {rank} is done. Sending back to master')
    comm.send(result, dest=0)