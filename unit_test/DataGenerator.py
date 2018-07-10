

import random, string

from random import randrange
import datetime
import os
import json
from collections import defaultdict

def generateCustomer_id():

    customer_id = ''.join(random.choices(string.ascii_letters.lower() + string.digits, k=12))
    return customer_id


def generateEvent1():
    event = random.randint(1, 2)
    if event == 1:
        return "CUSTOMER"
    else:
        return "SITE_VISIT"

def generateEvent2():
    event = random.randint(3, 4)
    if event == 3:
        return "IMAGE"
    else:
        return "ORDER"

def generateTimeStamp(start,count):
    current = start
    format = '%Y-%m-%dT%H:%M:%S.%fZ'

    while count >= 0:
        curr = datetime.datetime.strptime(current, format) + datetime.timedelta(milliseconds=randrange(60)) # days=randrange(60),
        yield curr
        count-=1

def generateNewUpdate():
    event = random.randint(1, 2)
    if event == 1:
        return "NEW"
    else:
        return "UPDATE"




def generateAmount():
    return str(round(random.uniform(10, 10000), 2)) + " USD"


def DataGenerator():

    MAIN_DIR = os.path.relpath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # relative path of the main directory
    input_dir = os.path.join(MAIN_DIR, "input")  # relative path of input dir


    fname = "inputgen.txt"

    start = "2018-01-14T12:46:46.384Z"
    format = '%Y-%m-%dT%H:%M:%S.%fZ'

    i = 0
    result = list()
    while i <= 50:
        customer_id = generateCustomer_id()
        j = i * 10
        k = i * 10
        customerFirst = True
        orderFirst = True
        while j > 0:

            event_time = next(generateTimeStamp(start, 0)).strftime(format)
            start = event_time

            event_type = generateEvent1()


            if event_type == 'CUSTOMER':

                if customerFirst: # first event should always be NEW for a customer_id
                    e = {'type': "CUSTOMER", 'verb': "NEW", "key": customer_id, "event_time": event_time,
                         "last_name": "Michael", "adr_city": "Rockville", "adr_state": "MD"}
                    customerFirst = False
                else:
                    verb = generateNewUpdate()
                    if verb == 'NEW':
                        customer_id = generateCustomer_id()
                        e = {'type': "CUSTOMER", 'verb': verb, "key": customer_id, "event_time": event_time,
                             "last_name": "Michael", "adr_city": "Rockville", "adr_state": "MD"}
                    else:
                        e = {'type': "CUSTOMER", 'verb': verb, "key": customer_id, "event_time": event_time,
                             "last_name": "Michael", "adr_city": "Rockville", "adr_state": "MD"}

            elif event_type == 'SITE_VISIT':

                e = {"type": "SITE_VISIT", "verb": "NEW", "key": generateCustomer_id(), "event_time": event_time,
                     "customer_id": customer_id, "tags": {"some key": "some value"}}

            result.append(e)
            j -= 1

        while k > 0 :

            event_time = next(generateTimeStamp(start, 0)).strftime(format)
            start = event_time

            event_type = generateEvent2()

            if event_type == 'IMAGE':

                e = {"type": "IMAGE", "verb": "UPLOAD", "key": generateCustomer_id(), "event_time": event_time,
                     "customer_id": customer_id, "camera_make": "Canon", "camera_model": "EOS 80D"}

            elif event_type == 'ORDER':

                if orderFirst: # first event should always as NEW for a order_id
                    e = {"type": "ORDER", "verb": "NEW", "key": generateCustomer_id(),"event_time": event_time, "customer_id": customer_id, "total_amount": generateAmount()}
                    orderFirst = False

                else:
                    verb = generateNewUpdate()
                    if verb == 'UPDATE' :
                        # fetch the previous oder_id
                        prev_order_id = [e['key'] for e in result if e['type'] == 'ORDER' and e['customer_id'] == customer_id and e["verb"] == "NEW"]
                        if prev_order_id:
                            e = {"type": "ORDER", "verb": verb, "key": prev_order_id[0], "event_time": event_time,"customer_id": customer_id, "total_amount": generateAmount()}
                        else: # there is no previous order id to update
                            continue
                    else:
                        e = {"type": "ORDER", "verb": generateNewUpdate(), "key": generateCustomer_id(), "event_time": event_time, "customer_id": customer_id, "total_amount": generateAmount()}

            result.append(e)
            k -= 1
        i += 1


    with open(os.path.join(input_dir,fname),'w') as f:

        print('Number of Test Records Generated %d', len(result))

        f.write(json.dumps(result))

    f.close()


if __name__ == '__main__':

    DataGenerator()
