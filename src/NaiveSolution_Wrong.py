# Copyright 2018 Shutterfly Inc

import os
import json
from collections import defaultdict
import time
from src.utils import getWeekNum, presort, doAggregation
import sys
import logging

""" 
D : Data Structure will be a default dictionary with structure as below :
{
customerId (string): {
            visitCount : int , Count of visits
            visits : dict {} , week number and counts of each week number. week starts at Sunday and end at Saturday
            orders : dict {} , orderId as key and amount as value
            lastName : string, customer last name (optional) 
            city : string, customer city (optional)
            state : string, customer state (optional)
            revenuePerVisit : float, 
            visitsPerWeek : float,
            LTV : float, 52(revenuePerVisit * visitsPerWeek) * 10 
}
optional :  may or may not be present in input file
"""
def ingest(e ,D) :

    """
    For every event e, this function updates the DataStructure D

    Args:
        e: dict
        Each event is of type dict and is read from json input file.
        D : defaltdict
    Returns:
        D : defaultdict, updated defaultdict with each events.

    D : Data Structure will be a default dictionary with structure as below :
    {
    customerId (string): {
                visitCount : int , Count of visits
                visits : dict {} , week number and counts of each week number. week starts at Sunday and end at Saturday
                orders : dict {} , orderId as key and amount as value
                pages : [] , list of all page_id visited
                images : [] , list of all image_id uploaded
                lastName : string, customer last name (optional)
                city : string, customer city (optional)
                state : string, customer state (optional)
                revenuePerVisit : float,
                visitsPerWeek : float,
                LTV : float, 52(revenuePerVisit * visitsPerWeek) * 10
    }
    optional :  may or may not be present in input file
    """


    if e['type'] == 'CUSTOMER':

        """
        case  verb = new and customer id not in D : Insert
        case  verb = new and customer id in D : Update , event after previous site_visit event
        case  verb = update and customer id in D : Update
        case  verb = update and customer id not in D : exception
        """

        weekNum = getWeekNum(e['event_time'])

        lastName, city, state = checkNonMandatory(e)

        if e['verb'] == 'NEW' and e['key'] not in D:
            # Insert
            D[e['key']] = {'visitCount':1, 'visits':{weekNum:1}, 'orders':{},'pages':[],'images': [],'revenue':0.00, 'lastName': lastName, 'city': city, 'state': state}

        elif e['verb'] == 'NEW' and e['key'] in D :
            # update , event after previous site_visit event
            D[e['key']]['lastName'] = lastName
            D[e['key']]['city'] = city
            D[e['key']]['state'] = state

            D[e['key']]['visitCount'] += 1

            if weekNum not in D[e['key']].setdefault('visits',{}) :
                D[e['key']]['visits'][weekNum] = 1
            else:
                D[e['key']]['visits'][weekNum] += 1

        elif e['verb'] == 'UPDATE' and e['key'] in D:
            # update
            D[e['key']]['lastName'] = lastName
            D[e['key']]['city'] = city
            D[e['key']]['state'] = state

            D[e['key']]['visitCount'] += 1

            if weekNum not in D[e['key']].setdefault('visits',{}) :
                D[e['key']]['visits'][weekNum] = 1
            else:
                D[e['key']]['visits'][weekNum] += 1

        elif e['verb'] == 'UPDATE' and e['key'] not in D :
            # exception
            logging.debug('UPDATE CUSTOMER without previous customer_id : %s', e['key'])

    elif e['type'] == 'SITE_VISIT':

        """
        Assumptions : SITE VISIT  event can occur before CUSTOMER event as it was not clear from description                      
                      Events with same page_id will be treated as duplicate as key will be unique. 
        """

        weekNum = getWeekNum(e['event_time'])

        if e['customer_id'] not in D:
            # first visit for the customer_id

            D[e['customer_id']] = {'visitCount': 1, 'visits': {weekNum:1}, 'orders':{},'images': [],'pages':[e['key']],'revenue': 0.00, 'lastName': None, 'city': None, 'state': None}

        elif e['customer_id'] in D and e['key'] in D[e['customer_id']]['pages']:
            # page_id is duplicate

            logging.debug('NEW SITE_VISIT with existing page_id :%s', e['key'])

        elif e['customer_id'] in D and e['key'] not in D[e['customer_id']]['pages']:
            # page_id is new for the customer

            D[e['customer_id']]['pages'].append(e['key'])
            D[e['customer_id']]['visitCount'] += 1

            if weekNum not in D[e['customer_id']].setdefault('visits',{}) :
                D[e['customer_id']]['visits'][weekNum] = 1
            else:
                D[e['customer_id']]['visits'][weekNum] += 1

    elif e['type'] == 'IMAGE':

        """
        Assumption : UPLOAD IMAGE event cannot occur before SITE VISIT or CUSTOMER event. 
                     We are not processing such events.
        
        case : customer uploaded different image i.e for same customer id there is one more image_id (new image)
        case : Events with same image_id will be treated as duplicate. Since image_id (key) will be unique.  
            
        """

        weekNum = getWeekNum(e['event_time'])

        if e['customer_id'] not in D :

            logging.debug('UPLOAD IMAGE event without SITE_VISIT/CUSTOMER event for customer_id : %s', e['customer_id'])

        elif e['customer_id'] in D and e['key'] not in D[e['customer_id']]['images']:
            # new image uploaded by customer
            D[e['customer_id']]['images'].append(e['key'])

            D[e['customer_id']]['visitCount'] += 1

            if weekNum not in  D[e['customer_id']].setdefault('visits',{}) :
                D[e['customer_id']]['visits'][weekNum] = 1
            else:
                D[e['customer_id']]['visits'][weekNum] += 1

        elif e['customer_id'] in D and e['key'] in D[e['customer_id']]['images']:
            # image_id is duplicate

            logging.debug('UPLOAD IMAGE with existing image_id :%s', e['key'])

    elif e['type'] == 'ORDER':

        """
         case verb = new and order id not in D : Insert
         case verb = new and order id in D : exception
         case verb = update and order id in D : Update
         case verb = update and order id not in D : exception
         
         Limitation : Update ORDER event should not update customer_id, as key of D is customer_id.  
        """
        try:
            # in case order amount is having junk data it will be captured in valueError

            order_amount = float(e['total_amount'].replace(" USD", ""))

            weekNum = getWeekNum(e['event_time'])

            if e['customer_id'] not in D:
                logging.debug('ORDER event without customer_id : %s', e['customer_id'])

            elif e['verb'] == 'NEW' and e['key'] not in D[e['customer_id']]['orders']:
                # Insert
                if e['customer_id'] not in D:
                    # exception, direct order without customer id
                    logging.debug('NEW ORDER without customer_id : %s',e['customer_id'])
                else:
                    D[e['customer_id']]['orders'][e['key']] = order_amount
                    D[e['customer_id']]['visitCount'] += 1

                    if weekNum not in D[e['customer_id']].setdefault('visits', {}):
                        D[e['customer_id']]['visits'][weekNum] = 1
                    else:
                        D[e['customer_id']]['visits'][weekNum] += 1

            elif e['verb'] == 'NEW' and e['key'] in D[e['customer_id']]['orders'] :
                # exception, order id is key
                logging.debug('NEW ORDER with existing order_id :%s', e['key'])

            elif e['verb'] == 'UPDATE' and e['key'] in D[e['customer_id']]['orders'] :
                # Update
                D[e['customer_id']]['orders'][e['key']]=order_amount
                D[e['customer_id']]['visitCount'] += 1

                if weekNum not in  D[e['customer_id']].setdefault('visits',{}) :
                    D[e['customer_id']]['visits'][weekNum] = 1
                else:
                    D[e['customer_id']]['visits'][weekNum] += 1

            elif e['verb'] == 'UPDATE' and e['key'] not in D[e['customer_id']]['orders']:
                # exception, no existing order to update
                logging.debug('UPDATE ORDER without previous order_id : %s', e['key'])
        except ValueError:
            logging.debug('Invalid total_amount for order_id : %s', e['key'])

    else:
        return D

def checkNonMandatory(e):

    """
    Returns lastName, city, state from CUSTOMER events incase of missing data
    Args:
        e : event dictionary

    Returns:
        result : lastName, city, state

    Note:
    incase any of the nonmandatory fields are missing we will take it as None
    """

    lastName, city, state = "","",""

    try :
        lastName = e['last_name']
    except KeyError:
        lastName = None

    try:
        city = e['city']
    except KeyError:
        city = None

    try:
        state= e['state']
    except KeyError:
        state = None

    return lastName, city, state

def readInput(input_dir):

    # Check the input data dir exists
    if not os.path.exists(input_dir):
        raise Exception("Input dir does not exist: %s", input_dir)

    # check if input dir contain txt files
    if not os.listdir(input_dir):
        logging.error("Could not read file from input dir %s",input_dir)
        sys.exit('Input directory is empty')

    # D DataStructure of type defaultdict
    D = defaultdict(dict)

    ingest_tic = time.time()

    for file in os.listdir(input_dir):

        if file.endswith(".txt"):
            logging.info('Reading Input File %s...', os.path.join(input_dir,file))
        try:
            with open(os.path.join(input_dir,file),'r') as f:

                raw_data = json.load(f)

                sorted_data = presort(raw_data)

                logging.info('Ingesting the events from Input File %s...', os.path.join(input_dir,file))

                for event in sorted_data:
                    # print(event)
                    ingest(event, D)

                totalEvents = len(sorted_data)
        except IOError :
            logging.error("Could not read file %s from input dir", file)



        f.close()
    # print(D)
    ingest_toc = time.time()
    ingestion_time = ingest_toc - ingest_tic

    logging.info("Data Ingestion Completed Successfully...")
    logging.info("----------Ingestion Summary----------")
    logging.info("----------Total Number of Events Processed  : %d", totalEvents)
    logging.info("----------Total Distinct CustomerId Ingested: %d", len(D))
    logging.info("----------Ingestion Time (in secs)          : %f", ingestion_time)

    return D



def TopXSimpleLTVCustomers(x,D):

    """
    Returns the top x customers with the highest Simple Lifetime Value from data D


    Args:
        x : int
        D: Data Structure

    Returns:
        result : dict
        output results for each customer id with LTV value.
        dict {customerId: LTV}

    Note:
    User may ask for x greater than number of items in Data Structure. we will handle this by min(x, len(D))
    """

    LTV_tic = time.time()
    D = doAggregation(D)

    x = min(x, len(D))

    topx = sorted(D.items(),key=lambda x:x[1]['LTV'],reverse=True)[:x]

    result = {custmerId[0]: custmerId[1]['LTV'] for custmerId in topx}

    # print(result)
    # logging.info('final results %s...', result)

    LTV_toc = time.time()
    LTV_time = LTV_toc - LTV_tic
    logging.info('Total Time to get the top x customers : %f', LTV_time)
    return result


def main():

    tic = time.time()


    MAIN_DIR = os.path.relpath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # relative path of the main directory
    input_dir = os.path.join(MAIN_DIR, "input")  # relative path of input dir
    output_dir = os.path.join(MAIN_DIR, "output")  # relative path of output dir

    logging.basicConfig(level=logging.DEBUG)

    file_handler = logging.FileHandler(os.path.join(output_dir, "log.txt"))
    logging.getLogger().addHandler(file_handler)

    logging.info('Started main')

    D = readInput(input_dir) # Generate DataStructure D
    x=36

    topXCustomersLTV =TopXSimpleLTVCustomers(x,D)

    fname = "output.txt"

    logging.info('Writing the results into the output directory..')

    with open(os.path.join(output_dir,fname),'w') as f:
        f.write(json.dumps(topXCustomersLTV))

    f.close()

    # Run Time

    toc = time.time()
    run_time = toc - tic

    logging.info('Total run time : %f', run_time)



if __name__ == '__main__':

    main()
