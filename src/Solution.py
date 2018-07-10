# Copyright 2018 Shutterfly Inc

"""Final Solution for code challenge"""

import os
import json
from collections import defaultdict,namedtuple
import time
from src.utils import getWeekNum, presort, doAggregation
import sys
import logging


def readInput(input_dir):

    # Check the input data dir exists
    if not os.path.exists(input_dir):
        raise Exception("Input dir does not exist: %s", input_dir)

    # check if input dir contain files
    if not os.listdir(input_dir):
        logging.error("Could not read file from input dir %s",input_dir)
        sys.exit('Input directory is empty')


    # D is DataStructure of type defaultdict
    # D = {'customer':{},'site_visit':{},'images':{},'order':{}}
    D = {'customer': defaultdict(), 'site_visit': defaultdict(), 'image': defaultdict(), 'order': defaultdict()} # better performance

    sumEvents = 0

    ingest_tic = time.time()

    # res = []
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
                    Ingest(event, D)
                    sumEvents += 1

        except IOError :
            logging.error("Could not read file %s from input dir", file)

        f.close()
    # print("D after Ingestion",D)
    ingest_toc = time.time()

    logging.info('Data Ingestion Completed...')
    ingestion_time = ingest_toc - ingest_tic
    logging.info("Data Ingestion Completed Successfully...")
    logging.info("----------Ingestion Summary----------")
    logging.info("----------Total Number of Events Processed  : %d", sumEvents)
    logging.info("----------Total Distinct CustomerId Ingested: %d", len(D['customer']))
    logging.info("----------Ingestion Time (in secs)          : %f", ingestion_time)

    return D


def Ingest(e ,D) :

    """
    For every event e, this function updates the DataStructure D
    Args:
        e: dict
        Each event is of type dict and is read from json input file.
        D: {'customer': defaultdict(), 'site_visit': defaultdict(), 'image': defaultdict(), 'order': defaultdict()}
    Returns:
        D updated with each event type information.
          Data Structure will be as below :
            {
                    'customer'   : { customer_id(key) : [lastnm ,city,state] }
                    'site_visit' : { page_id(key) : { {'customer_id': customer_id},{'event_time': [event_time]}}}
                    'image' : { image_id(key) : { 'customer_id':customer_id}  } # Not needed
                    'order' : { order_id(key) : [ {'customer_id' : customer_id}, {'order_id':total_amount}, {'event_time' :[event_time]}]}
            }
    """

    if e['type'] == 'CUSTOMER':

        """        case  verb = new and customer id not in D : Insert
                   case  verb = update and customer id in D : Update
                   case  verb = new and customer id in D : exception
                   case  verb = update and customer id not in D : exception
      
        Assumption : Each customer_id is Unique as Key is unique
        """
        lastName, city, state = checkNonMandatory(e)

        if e['verb'] == 'NEW' and e['key'] not in D['customer']:
            # Insert
            D['customer'][e['key']] = [lastName,city,state]

        elif e['verb'] == 'UPDATE' and e['key'] in D['customer']:
            # update
            D['customer'][e['key']] = [lastName, city, state]

        elif e['verb'] == 'NEW' and e['key'] in D['customer']:
            # exception
            logging.debug('NEW CUSTOMER with existing customer_id :%s', e['key'])

        elif e['verb'] == 'UPDATE' and e['key'] not in D['customer']:
            # exception
            logging.debug('UPDATE CUSTOMER without Key : %s', e['key'])

    elif e['type'] == 'SITE_VISIT':

        """
        Assumptions : Assumption : Each page_id is Unique as Key is unique
        """

        # 'site_visit': {page_id(key): [ {'customer_id': customer_id} ]}

        if e['key'] not in D['site_visit']:
            # Insert
            D['site_visit'][e['key']] = []
            D['site_visit'][e['key']].append({'customer_id':e['customer_id']})
            D['site_visit'][e['key']].append({'event_time': e['event_time']})

        elif e['key'] in D['site_visit']:
            # page_id Key is duplicate
            logging.debug('NEW SITE_VISIT with existing page_id :%s', e['key'])

    elif e['type'] == 'IMAGE':

        """        
         Assumption : Each image_id is Unique as Key is unique
        """
        # 'image': {image_id(key): [ {'customer_id': customer_id}] }

        if e['key'] not in D['image']:
            D['image'][e['key']] = []
            D['image'][e['key']].append({'customer_id':e['customer_id']})

        elif e['key'] in D['image']:
            # image_id key is duplicate
            logging.debug('UPLOAD IMAGE with existing image_id :%s', e['key'])

    elif e['type'] == 'ORDER':

        """
         case verb = new and order id not in D : Insert
         case verb = update and order id in D : Update
         case verb = new and order id in D : exception
         case verb = update and order id not in D : exception

         Assumption : Each order_id is Unique as Key is unique         
        """
        # 'order': defaultdict( { order_id(key): [ {'customer_id': customer_id}, {'order_amount': total_amount} ] } )
        try: # to handle junk order amount
            order_amount = float(e['total_amount'].replace(" USD", ""))

            if e['verb'] == 'NEW' and e['key'] not in D['order']:
                # Insert
                D['order'][e['key']] = []
                D['order'][e['key']].append({'customer_id':e['customer_id']}) # customer id
                D['order'][e['key']].append({'order_amount':order_amount}) # order amount

            elif e['verb'] == 'UPDATE' and e['key'] in D['order'] :
                # Update
                D['order'][e['key']][0]['customer_id'] = e['customer_id']
                D['order'][e['key']][1]['order_amount'] = order_amount

            elif e['verb'] == 'NEW' and e['key'] in D['order'] :
                # exception
                logging.debug('NEW ORDER with existing order_id :%s', e['key'])

            elif e['verb'] == 'UPDATE' and e['key'] not in D['order']:
                # exception
                logging.debug('UPDATE ORDER without Key : %s', e['key'])

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
    try:
        lastName = e['last_name']
    except KeyError:
        lastName = None

    try:
        city = e['adr_city']
    except KeyError:
        city = None

    try:
        state= e['adr_state']
    except KeyError:
        state = None

    return lastName, city, state

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

        Input  D : Data Structure will be as below :
        {
            'customer'   : { customer_id(key) : [lastnm ,city,state] }
            'site_visit' : { page_id(key) : { {'customer_id': customer_id},{'event_time': [event_time]}}}
            'image' : { image_id(key) : { 'customer_id':customer_id}  } # Not needed
            'order' : { order_id(key) : [ {'customer_id' : customer_id}, {'order_id':total_amount}, {'event_time' :[event_time]}]}
        }

        Output D : Data Structure will be as below :
        {
            'customer'   : { customer_id(key) : [lastnm ,city, state,revenuePerVisit,visitsPerWeek,LTV] }
            'site_visit' : { page_id(key) : { {'customer_id': customer_id},{'event_time': [event_time]}}}
            'image' : { image_id(key) : { 'customer_id':customer_id}  } # Not needed
            'order' : { order_id(key) : [ {'customer_id' : customer_id}, {'order_id':total_amount}, {'event_time' :[event_time]}]}
        }

    Note: User may ask for x greater than number of items in Data Structure. we will handle this by min(x, len(D))
    """

    LTV_tic = time.time()

    x = min(x, len(D))

    for customerId in D['customer'].keys() :

        site_visit_event_time =  [D['site_visit'][page_id][1]['event_time'] for page_id in D['site_visit'] if D['site_visit'][page_id][0]['customer_id'] == customerId]

        visitsWeekwise = {}

        for event_time in site_visit_event_time:

            weeknm = getWeekNum(event_time) # for a given timestamp it will get the week num

            if weeknm in visitsWeekwise:
                visitsWeekwise[weeknm] +=1
            else:
                visitsWeekwise[weeknm] = 1

        visitCount = sum(visitsWeekwise.values())

        distinctWeeks = len(visitsWeekwise)

        revenue = sum([D['order'][order_id][1]['order_amount'] for order_id in D['order'] if D['order'][order_id][0]['customer_id'] == customerId])

        n =2  #2 decimal precision

        if visitCount > 0.00: # to handle divide by 0
            revenuePerVisit = revenue / float(visitCount)
            D['customer'][customerId].append(round(revenuePerVisit,n))

        else:
            revenuePerVisit = 0.00
            D['customer'][customerId].append(revenuePerVisit)

        if distinctWeeks > 0: # to handle divide by 0
            visitsPerWeek = visitCount / float(distinctWeeks)
            D['customer'][customerId].append(round(visitsPerWeek, n))

        else:
            visitsPerWeek = 0.00
            D['customer'][customerId].append(visitsPerWeek)

        lifespan = 10.00
        LTV = round(52 * (D['customer'][customerId][3] * D['customer'][customerId][4]) * lifespan, n)

        D['customer'][customerId].append(LTV) # index of LTV will be 5

    # print("D after Aggregation",D)

    topx_tuple = sorted(D['customer'].items(), key=lambda x:x[1][5], reverse=True)[:x]

    result = dict(topx_tuple) # converting list of tuples to dict

    LTV_toc = time.time()
    LTV_time = LTV_toc - LTV_tic

    logging.info('final results %s...', result)
    logging.info("TopXSimpleLTVCustomers Completed Successfully...")
    logging.info("----------TopXSimpleLTVCustomers Summary----------")
    logging.info("----------Time for get Top %d customers(in secs): %f",x ,LTV_time)

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
    x=20

    topXCustomers =TopXSimpleLTVCustomers(x,D)

    fname = "output.txt"

    logging.info('Writing the results into the output directory..')

    with open(os.path.join(output_dir,fname),'w') as f:
        f.write(json.dumps(topXCustomers))
    f.close()

    # Run Time

    toc = time.time()
    run_time = toc - tic

    logging.info('Total run time : %f', run_time)



if __name__ == '__main__':

    main()
