# Solution 

## Assumptions
* *Assuming the order of  as type (site visit/customer) -> image upload -> order and since key and event_time are received with  no guaranteed order. We first sort the input data based on event_time.*  

* *Each Key (customer_id,site_id,order_id) is Unique.For example SITE_VISIT events will have unique page_id, IMAGE event will have unique image_id. second occurence of page_id/image_id will be an exception while for customer_id/order_id the second occurence should come as update else it will be an exception, all these exception will be recorded in log file*      

* *Assumption for CUSTOMER Event that NEW event will come first and then UPDATE event since file is sorted by event time. we handled this exception gracefully and all customer_id for which UPDATE event precedes the NEW event will be recorded in the log file.*   

* *Event Time is format = '%Y-%m-%dT%H:%M:%S.%fZ'*  
  No Time Conversion is expected. However we can extend this to convert to EST or other time.  

* *Some customers will only have SITE_VISIT and CUSTOMER event, no order/images*  
  We will default revenuePerVisit for such customers  as 0.00 and calculate visitsPerWeek assuming they may have orders in future.  
  
* *Please note that the timeframe for this calculation should come from D.*   
 To use the timeframe from data that was ingested into D to calculate the LTV to find start  start and end dates of your LTV calculation.
So, D may contain more than 1 year worth of data for a customerID. In other words the date range can be very wide or narrow and may span more than or less than 52 weeks. We should be counting the number of visits week wise.      
visits = ['2017-01-02','2017-01-09','2018-01-01','2018-01-02','2018-05-02','2018-06-03']  
visits_weekwise = [1,2,1,1,18,22]   
Total site visits = 6 and distinct weeks are 4   
number of site visits per week  = 6/4 = 1.50  

## Data Structure Used
    Input to Ingestion:                 Updated D from Ingestion/Input to TopXSimpleLTVCustomers          
    D = {                               D = {                                                     
        'customer': defaultdict(),          'customer':{customer_id(key):[lastnm ,city,state]}
        'site_visit': defaultdict(),        'site_visit':{page_id(key):{{'customer_id': customer_id},{'event_time': [event_time]}}}
        'image': defaultdict(),             'image':{image_id(key):{'customer_id':customer_id}  } # Not needed
        'order': defaultdict()             'order':{order_id(key):[{'customer_id':customer_id}, {'order_id':total_amount}, {'event_time' :[event_time]}]}
        }                                   }


## Ingestion Summary  
----------Ingestion Summary----------  
----------Total Number of Events Processed  : 1252500  
----------Total Distinct CustomerId Ingested: 500  
----------Ingestion Time (in secs)          : 28.689338  

## TopXSimpleLTVCustomers Sumary
----------Time for get Top 50 customers(in secs): 178.478944


## First Naive Solution 
###Assumptions (later confirmed with shutterfly team that below assumptions are wrong) 
* *UPDATE on ORDER events: assumption is customer_id will not be updated, we are incrementing the visit count and updating total amount*  
* *UPDATE on CUSTOMER events:we are incrementing the visit count and updating last_name etc.*  
* *Visit count should be incremented for all event types*  
This is critical assumption. However, we can change it to count VISIT only when event type is SITE_VISIT.  
  
## Naive Solution Performance 
* *Total execution time for 1.25 million events.*    
  Total Number of Events Processed      : 1252535    events  
  Ingestion Time (in secs)              : 156.128368 secs    
  Total Time to get the top x customers : 0.002979   secs  
  Total run time                        : 156.161186 secs   
  To replicate the same results, please generate the synthetic test data using DataGenerator.py inside unit_test folder.
      
#Shutterfly Customer Lifetime Value

One way to analyze acquisition strategy and estimate marketing cost is to calculate the Lifetime Value (“LTV”) of a customer. Simply speaking, LTV is the projected revenue that customer will generate during their lifetime.

A simple LTV can be calculated using the following equation: `52(a) x t`. Where `a` is the average customer value per week (customer expenditures per visit (USD) x number of site visits per week) and `t` is the average customer lifespan. The average lifespan for Shutterfly is 10 years.  

## Code Requirements

1. Solve this problem using SQL. Create a relational data model , and use this data model to write SQL that will return the Customer Lifetime Value for all customers.

2. Write a program that ingests event data and implements one analytic method, below. You are expected to write clean, well-documented and well-tested code. Be sure to think about performance - what the performance characteristic of the code is and how it could be improved in the future.

You may use one of the following OO languages: Java, Python, Scala.

### Ingest(e, D)
Given event e, update data D

### TopXSimpleLTVCustomers(x, D)
Return the top x customers with the highest Simple Lifetime Value from data D. 

Please note that the timeframe for this calculation should come from D. That is, use the data that was ingested into D to calculate the LTV to frame the start and end dates of your LTV calculation. You should not be using external data (in particular "now") for this calculation.

## Events

Please use the following sample events the Data Warehouse collects from Shutterfly’s public sites. All events have a `key` and `event_time`, but are received with no guaranteed order and with fluctuating frequency.

See `sample_input` directory for a sample of each event.

### Customer
* type *
  * CUSTOMER
* verb *
  * NEW
  * UPDATE
* Additional Data
  * key(customer_id) *
  * event_time *
  * last_name
  * adr_city
  * adr_state

### Site Visit
* type *
  * SITE_VISIT
* verb *
  * NEW
* Additional Data
  * key(page_id) *
  * event_time *
  * customer_id *
  * tags (array of name/value properties)

### Image Upload
* type *
  * IMAGE
* verb *
  * UPLOAD
* Additional Data
  * key(image_id) *
  * event_time *
  * customer_id *
  * camera_make
  * camera_model

### Order
* type *
  * ORDER
* verb *
  * NEW
  * UPDATE
* Additional Data
  * key(order_id) *
  * event_time *
  * customer_id *
  * total_amount *

\* represents required data

## Directory structure
This project has the following directories, and it is expected that your code will follow the same structure.

```
- README.md
- src (your source will live here)
- input (input file you create as proof of functionality lives here)
- output (output file with the result of TopXSimpleLTVCustomers(10, D). Include the calculated LTV.)
- sample_input (one event of each type for visualization purposes)
```

## Reference
https://blog.kissmetrics.com/how-to-calculate-lifetime-value

## Submission
Once you have completed your code, submit a link to a Github repo that contains your source code to <mailto:ccdwh@shutterfly.com>.


  
  
## FAQ

* *Do I need to create a private repo?*

  No, you do not need to pay for a private repo - public is fine.

* *Where does the data structure passed into the methods come from?*

  Some caller will pass you the data structure to use. You will define an in-memory data structure to use for the purposes of this challenge, but in some theoretical future refactor of this code there could be a different data structure implementation.

* *What does an event look like?*

  Please see sample events in the sample_input directory. These are intentionally just a single example for you to visualize the data, your code should be tested with more events than these.

* *How should I handle data that [is missing (OR) looks like (OR) has] particular case X?*

  You are welcome to protect your code against any particular data or handle data in any particular way, but it is a best practice to gracefully handle unexpected input. You should document any critical design decisions or assumptions.

* *You are requiring ingesting of events that are not used. What am I missing?*

  The analytic method `TopXSimpleLTVCustomers` is only one potential method of looking at the data ingested. There could be other methods to do other functions. You are still required to ingest events even if they are not consumed as part of this challenge.

* *May I use pre-built libraries?*

  You may, but you should not need to use anything exotic. Fairly standard libraries such as parsing functionality is fine. If you do use libraries that need to be separately downloaded or referenced to use the program, please document.

* *May I use Big Data technology XYZ?*

  Probably not. The purpose of this coding challenge is to ensure you have a solid foundation in coding skills. Additionally, your code must be easy to be reviewed and tested, so you should not expect the reviewer to have your specific technology installed and available. In the end, you must be sure that your resulting output is sufficient to showcase your code.

* *How long may I work on this?*

  The rough expectation is that the challenge will take a couple/few real time hours and can therefore be due in 48 calendar hours. That said, it is more important to be well built than timely so if you have other commitments and cannot quickly accomplish the challenge, then you can take more time.

* *What is the point of dealing with site visits? Couldn't I just add up the order totals and multiply?*

  Correct, dividing revenue by site visits just to multiply back out is not necessary to get to the result. That being said, for the purposes of this exercise you should assume that revenue / visit and visits / week are important metrics to the business and should be available from the data - even if you choose to not implement your revenue calculation using them.

* *How do I handle missing data? What if a user doesn't come back?*

  You should consider your data set (whatever data set you use to test) as complete for that timeframe. So if user A has order events in week 1 and order events in the following week 2, but user B has only order events in week 1, then user B will have see a lower average revenue as his revenue is averaged with zero revenue in week 2.

* *What constitutes a week?*

  You can calculate whatever you want for a week, but it is probably easier to consider a week as running from Sunday - Saturday, as the US default calendar does.

If you have further questions you can email <mailto:ccdwh@shutterfly.com>.
