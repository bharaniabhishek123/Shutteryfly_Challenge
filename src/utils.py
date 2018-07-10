""" Utilities for Solution2_Program and NaiveSolution_Wrong
    To Convert timestamp into week number
    Sort the input data with event time
"""

import dateutil.parser
import logging


def getWeekNum(event_time):

    """
    To get the week number from event time

    Args:
        event_time:str
        Timestamp read from input file of format : %Y-%m-%dT%H:%M:%S.%fZ
    Returns:
        weekNum : int
        Week number of the year (Sunday as the first day of the week) as a zero padded decimal number.
        All days in a new year preceding the first Sunday are considered to be in week 0.
    """
    try :
        date_obj = dateutil.parser.parse(event_time).date()
        weekNum = date_obj.strftime('%U')  # weekNum is string, we need to cast to int
        return int(weekNum)
    except ValueError:
        logging.debug("Invalid datetime format for event_time : %s", event_time)



def presort(data):

    """
    To sort the input data based on event time
    Assumption : Since there is no guaranteed order for key and event time. We are sorting the input json data based on event_time.

    Args:
        data: json
        input data in json format read from input file
    Returns:
        sorted data : json
        output data in json format sorted based on event time ascending.

    """
    # Using sorted built-in sorting algorithm with run time complexity of  O(n log n)
    # This can slowdown the performance for huge dataset

    logging.info('Sorting Input data ...')
    return sorted(data, key = lambda v: (v['event_time']))


def doAggregation(D):
    """
    This function is a helper function called from TopXSimpleLTVCustomers to calculate
    revenuePerVisit : float,visitsPerWeek : float, LTV : float, 52(revenuePerVisit * visitsPerWeek) * 10

    Args:
        D: defaultdict DataStructure

    Returns:
        D: default dict DataStructure updated with revenuePerVisit, visitsPerWeek, LTV

    Note:
    Some Customer may not have revenue > 0.00 : make revenuePerVisit to 0.00
    Some Customer may not have distinctWeeks > 0 : make visitsPerWeek to 0.00
   """

    n = 2  # n digits precision after decimal point
    logging.info('Generating Aggregates to get revenuePerVisit, visitsPerWeek and LTV')
    for customerId in D.keys():

        revenue = sum(D[customerId]['orders'].values())
        if revenue > 0.00:

            D[customerId]['revenuePerVisit'] = round(revenue / D[customerId]['visitCount'], n)
        else:
            D[customerId]['revenuePerVisit'] = 0.00


        # if D[customerId]['revenue'] > 0.00:
        #
        #     D[customerId]['revenuePerVisit'] = round(D[customerId]['revenue'] / D[customerId]['visitCount'], n)
        # else:
        #     D[customerId]['revenuePerVisit'] = 0.00

        distinctWeeks = len(D[customerId]['visits'])

        if distinctWeeks > 0:
            D[customerId]['visitsPerWeek'] = round(D[customerId]['visitCount'] / distinctWeeks, n)
        else:
            D[customerId]['visitsPerWeek'] = 0.00

        lifespan = 10.00

        D[customerId]['LTV'] = round(
            52 * (D[customerId]['revenuePerVisit'] * D[customerId]['visitsPerWeek']) * lifespan, n)

    return D
