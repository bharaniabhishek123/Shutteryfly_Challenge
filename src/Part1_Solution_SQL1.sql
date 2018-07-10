--Note : Tried and tested below sql on Oracle
with
   orders(Order_Id,event_date,customer_id,total_amount) as(
	select '68d84e5d1b43','2018-01-20','96f55c7d8010',1250.00 from dual union all
	select '68d84e5d1e53','2018-01-26','96f55c7d8010',2000.00 from dual union all
	select '68d84e5d1b54','2018-01-27','96f55c7d8005',2000.00 from dual union all
	select '68d84e5d2b54','2018-01-28','96f55c7d8001',1250.00 from dual union all
	select '68d84e5d2b55','2018-02-12','96f55c7d8004',50.00 from dual union all
	select '68d84e5d1a43','2018-01-06','96f55c7d8010',15.00 from dual union all
	select '68d84e5d1a44','2018-01-07','96f55c7d8005',1250.00 from dual union all
	select '68d84e5d1a53','2018-01-11','96f55c7d8010',2000.00 from dual
	),
    site_visit(page_id,event_date,customer_id) as(
	select 'ac05e815512g','2018-01-22', '96f55c7d8005' from dual union all
	select 'ac05e815513f','2018-01-26', '96f55c7d8010' from dual union all
	select 'ac05e816513h','2018-02-22', '96f55c7d8004' from dual union all
	select 'ac05e815502f','2018-01-06', '96f55c7d8010' from dual union all
	select 'ac05e815502g','2018-01-09', '96f55c7d8005' from dual union all
	select 'ac05e815503f','2018-01-11', '96f55c7d8010' from dual union all
	select 'ac05e815503g','2018-01-12', '96f55c7d8005' from dual union all
	select 'ac05e815512f','2018-01-31', '96f55c7d8010' from dual union all
	select 'ac05e815513g','2018-01-26', '96f55c7d8005' from dual union all
	select 'ac05e816513g','2018-01-21', '96f55c7d8001' from dual
	),
   customer(Customer_Id,event_date,lastname,city,state) as(
	select '96f55c7d8005','2018-01-26','Michael', 'Rockville','MD' from dual union all
	select '96f55c7d8001','2018-01-2000T12:46:12.384Z','Jennifer', 'Middletown','VA' from dual union all
	select '96f55c7d8004','2018-02-28T12:56:12.384Z','Bill', 'Washington','DC' from dual union all
	select '96f55c7d8010','2018-01-06T12:45:52.041Z','Andy', 'Middletown','VA' from dual
	) -- Table are created and sql will start from next line
   SELECT
    CUST.CUSTOMER_ID,
    LASTNAME,
    CITY,
    STATE,
    NVL(ORD_VISITS.LTV,0),
    ORD_VISITS.RANK
  FROM CUSTOMER CUST
    JOIN
        (SELECT
        FINAL_CUST.CUSTOMER_ID,
        TOTAL_VISITS,
        VISITS_PER_WEEK,
        TOTAL_AMOUNT,
        DENSE_RANK() OVER (ORDER BY (52 * (NVL(TOTAL_AMOUNT,0)/TOTAL_VISITS) * VISITS_PER_WEEK *10) DESC) AS RANK,
        (52 * (TOTAL_AMOUNT/TOTAL_VISITS) * VISITS_PER_WEEK *10) AS LTV
        FROM
        (
            ( SELECT
                CUST.CUSTOMER_ID,
                SUM(VISIT_COUNT) AS TOTAL_VISITS,
                COUNT(DISTINCT (WEEKNM)) AS DISTINCT_WEEKS,
                SUM(VISIT_COUNT) / COUNT(DISTINCT (WEEKNM)) AS VISITS_PER_WEEK
              FROM
                ( SELECT
                        CUSTOMER_ID,
                        TO_CHAR(TO_DATE(EVENT_DATE,'YYYY-MM-DD'),'IW') AS WEEKNM,
                        COUNT(*) AS VISIT_COUNT
                    FROM SITE_VISIT
                   GROUP BY CUSTOMER_ID,TO_CHAR(TO_DATE(EVENT_DATE,'YYYY-MM-DD'),'IW')
                 ) CUST
              GROUP BY CUST.CUSTOMER_ID
            ) FINAL_CUST
            LEFT JOIN --Order may not be present
            ( SELECT
                   CUSTOMER_ID,
                   SUM(TOTAL_AMOUNT) AS TOTAL_AMOUNT
              FROM ORDERS
              GROUP BY CUSTOMER_ID
             ) ORD ON ORD.CUSTOMER_ID = FINAL_CUST.CUSTOMER_ID
        )
    ) ORD_VISITS ON CUST.CUSTOMER_ID = ORD_VISITS.CUSTOMER_ID
   WHERE ORD_VISITS.RANK <=4 -- change this to top x
      ;