# Zohreh_Mehrafrooz_40218210_A4
import os
from itertools import permutations

from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import year, month, dayofmonth, to_date


def restaurant_shift_coworkers(worker_shifts: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the worker_shifts.txt. Performs a series of MapReduce operations via
    PySpark to calculate the number of shifts worked together by each pair of co-workers. Returns the results as an RDD
    sorted by the number of shifts worked together THEN by the names of co-workers in a DESCENDING order.
    :param worker_shifts: RDD object of the contents of worker_shifts.txt.
    :return: RDD of pairs of co-workers and the number of shifts they worked together sorted in a DESCENDING order by
             the number of shifts then by the names of co-workers.
             Example output: [(('Shreya Chmela', 'Fabian Henderson'), 3),
                              (('Fabian Henderson', 'Shreya Chmela'), 3),
                              (('Shreya Chmela', 'Leila Jager'), 2),
                              (('Leila Jager', 'Shreya Chmela'), 2)]
    """
    # type of the output: (date, str)
    # by this map, I swap (Shirley Marion,2022-08-13) pair as (2022-08-13,Shirley Marion)
    worker_shifts_swap = worker_shifts.map(lambda x: (x.split(',')[1], x.split(',')[0]))

    # Transformation join(), output type is (date, (str, str))
    # by this transformation, I reached (2022-08-13,(Shirley Marion,Brenda Schantz))
    worker_shifts_join = worker_shifts_swap.join(worker_shifts_swap)

    # type of the output: ((str, str), date)
    # by this map, I swap (2022-08-13,(Shirley Marion,Brenda Schantz)) to ((Shirley Marion,Brenda Schantz), 2022-08-13)
    worker_shifts_join_swap = worker_shifts_join.map(lambda x: (x[1], x[0]))

    # type of the output: ((str, str), date), delete the pairs that include each person with himself
    # filter to delete pairs like ((Shirley Marion,Shirley Marion), 2022-08-13)
    workers_shifts_pair = worker_shifts_join_swap.filter(lambda x: x[0][0] != x[0][1])

    # type of the output: ((str, str), int)
    # map each date for every pair of workers to 1, ex.((Shirley Marion,Brenda Schantz), 1)
    workers_shifts_count = workers_shifts_pair.mapValues(lambda x: 1)

    # type of the output: ((str, str), int)
    # reduce pairs based on key which is a pair of workers
    workers_shifts_count_all = workers_shifts_count.reduceByKey(lambda x, y: x + y)

    # # type of the output: ((str, str), int)
    # sort shift workers based on their names and number of shifts
    workers_shifts_count_all_sorted = workers_shifts_count_all.sortBy(lambda x: x[0][1], ascending=False).\
        sortBy(lambda x: x[0][0], ascending=False).sortBy(lambda x: x[1], ascending=False)

    # print(workers_shifts_count_all_sorted.collect())

    return workers_shifts_count_all_sorted


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    cancelled_flights = flights.filter((flights.Cancelled == "true") &
                                       (year(flights.FlightDate) == 2021) & (month(flights.FlightDate) == 9))
    cancelled_flights_airline = cancelled_flights.groupBy('Airline').count()
    cancelled_flights_airline_sorted = cancelled_flights_airline.sort('count', ascending=False)
    # print(cancelled_flights_airline_sorted.collect()[0])
    return cancelled_flights_airline_sorted.collect()[0][0]


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of 
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    # raise NotImplementedError('Your Implementation Here.')
    diverted_flights = flights.filter((flights.Diverted == "true") & (year(flights.FlightDate) == 2021) &
                                      (month(flights.FlightDate) == 11) & (20 <= dayofmonth(flights.FlightDate)) &
                                       (dayofmonth(flights.FlightDate) <= 30))
    return diverted_flights.count()


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    """
    # raise NotImplementedError('Your Implementation Here.')
    avg_airtime_flights = flights.filter((flights.OriginCityName == 'Nashville, TN') &
                                         (flights.DestCityName == 'Chicago, IL'))
    return avg_airtime_flights.agg({'AirTime': 'mean'}).collect()[0][0]


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is 
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing. 
    """
    missing_departure_time = flights.filter(flights.DepTime.isNull())
    missing_departure_time_date = missing_departure_time.withColumn("date_type", to_date('FlightDate'))
    return missing_departure_time_date.select('date_type').distinct().count()


def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext('local[*]')
    spark = SparkSession.builder.getOrCreate()

    print('########################## Problem 1 ########################')
    # problem 1: restaurant shift coworkers with Spark and MapReduce 
    # read the file
    worker_shifts = sc.textFile('worker_shifts.txt')

    sorted_num_coworking_shifts = restaurant_shift_coworkers(worker_shifts)

    # print the most, least, and average number of shifts together
    sorted_num_coworking_shifts.persist()
    print('Co-Workers with most shifts together:', sorted_num_coworking_shifts.first())
    print('Co-Workers with least shifts together:', sorted_num_coworking_shifts.sortBy(lambda x: (x[1], x[0])).first())
    print('Avg. No. of Shared Shifts:',
          sorted_num_coworking_shifts.map(lambda x: x[1]).reduce(lambda x,y: x+y)/sorted_num_coworking_shifts.count())

    print('########################## Problem 2 ########################')
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv('Combined_Flights_2021.csv', header=True, inferSchema=True)
    # print(flights.take(10))
    # print(flights.columns)

    print('Q1:', air_flights_most_canceled_flights(flights), 'had the most canceled flights in September 2021.')

    print('Q2:', air_flights_diverted_flights(flights),
          'flights were diverted between the period of 20th-30thm November 2021.')

    print('Q3:', air_flights_avg_airtime(flights), 'is the average airtime for flights that were flying from '
                                                   'Nashville to Chicago.')

    print('Q4:', air_flights_missing_departure_time(flights), 'unique dates where departure time (DepTime) was '
                                                              'not recorded.')


if __name__ == '__main__':
    main()
