With CTE As 
(
Select 
    event_date
    ,event_timestamp
    ,event_name
    ,(select value.string_value from unnest(event_params) where key = 'page_location') as page_location
    ,(select value.string_value from unnest(event_params) where key = 'campaign') as campaign
    ,(select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id
    ,(select value.int_value from unnest(event_params) where key = 'engagement_time_msec') as engagement_time_msec
    ,device.category as device_category
    ,user_id
    ,user_pseudo_id
FROM `masai-ga-saket.analytics_364078656.events_*`
),
Quiz as 
(Select 
      user_id
      ,count(distinct event_date) as `#days_quiz_attempted`
from CTE
where lower(page_location) like '%quiz%' and user_id is not null
group by user_id),
prepare as 
(Select 
      user_id
      ,count(distinct event_date) as `#days_prepared`
      ,count(distinct lower(page_location)) as number_questions
from CTE
where lower(page_location) like '%prepare%' and user_id is not null
group by user_id),
page_views as 
(Select 
      user_id
      ,count(distinct event_date) as `#days_platform_visited`
      ,count(event_name) as `#events_triggered`
      ,count(distinct ga_session_id ) as `#sessions`
      ,sum(engagement_time_msec)/1000 as total_time_spent_in_sec
      ,max(TIMESTAMP_MICROS(event_timestamp)) as last_activity_date
from CTE
where user_id is not null
group by user_id),

job_apply as 
(Select 
      user_id
      ,count(distinct event_date) as `#days_click_on_job_apply`
      ,count(distinct lower(page_location)) as job_apply_click
from CTE
where event_name = 'Jobs - Apply Now'  and user_id is not null
group by user_id),

Last_activity as 
(select 
    user_id
    ,campaign as is_last_active
    ,device_category as device
from 
(select 
    user_id
    ,row_number() over(partition by user_id order by engagement_time_msec desc) as_rank
    ,campaign
    ,device_category
from CTE
where user_id is not null
)C
where as_rank = 1)

Select 
    page_views.*
    ,`#days_quiz_attempted`
    ,`#days_prepared`
    ,number_questions
    ,`#days_click_on_job_apply`
    ,job_apply_click
    ,device as last_device
from  page_views
left join quiz
on page_views.user_id = quiz.user_id
left join prepare 
on page_views.user_id = prepare.user_id
left join job_apply
on page_views.user_id = job_apply.user_id
left join Last_activity
on page_views.user_id = Last_activity.user_id






