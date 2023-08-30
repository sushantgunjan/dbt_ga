{{ config(materialized='table')}}
with cte AS (
  SELECT 
    cast(PARSE_DATE("%Y%m%d", event_date) as date) as date_of_event , 
    (select value.int_value FROM
                                        UNNEST (event_params)
                                                     WHERE
                                                           key = 'ga_session_id') as session_ids
    ,max(user_id) over(PARTITION by user_pseudo_id) as nd_user
    ,
  * FROM `analytics_364078656.events_*`
),
CTE2 as

(Select 
coalesce(max(user_id) OVER (PARTITION by  user_pseudo_id, session_ids),nd_user)  as defined_user_id
,*
from cte),
CTE5 as 
-- where cast(date_of_event as date) >= '2023-08-03'
(Select *
-- (select value.string_value FROM UNNEST (event_params)
--                                                      WHERE
--                                                            key = 'page_location') as pl
from CTE2
-- where user_pseudo_id = '153981139.1691422313'
-- where defined_user_id is null
-- and date_of_event > '2023-08-02'
-- group by user_pseudo_id
-- order by cn desc
)

Select 
case when user_id is not null and user_id != defined_user_id then user_id else defined_user_id end as derived_user_id,
*
Except(defined_user_id, nd_user)
from CTE5
where 1=1
and (select value.string_value FROM UNNEST (event_params)
                                                     WHERE
                                                           key = 'page_location') not like '%test%'
-- Select
--  user_pseudo_id, count(distinct user_id) as cn
-- -- user_pseudo_id, count(user_pseudo_id) as cn 
-- from   
-- where (select value.string_value FROM UNNEST (event_params)
--                                                      WHERE
--                                                            key = 'page_location') not like '%test%'
--  and user_id is not null 
--  group by   user_pseudo_id   
--  having  cn > 1   
--  order by cn desc            
-- -- where user_pseudo_id  in (Select user_pseudo_id from CTE5)
-- group by user_pseudo_id
-- having cn > 10
-- order by cn desc

-- Select
--  distinct user_id as cn
-- -- -- user_pseudo_id, count(user_pseudo_id) as cn 
-- from `analytics_364078656.events_*`
-- where user_pseudo_id = '1524057878.1691558965'


-- Select distinct user_id from `analytics_364078656.events_*`
-- where user_pseudo_id in ('1892396032.1687409859','612910934.1691635851','491189293.1687452855'  )




