With CTE As 
(
Select 
    event_date
    ,event_timestamp
    ,cast(TIMESTAMP_MICROS(event_timestamp) as date) as event_dates
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
      ,event_dates
      ,count(distinct event_dates) as `#days_quiz_attempted`
from CTE
where lower(page_location) like '%quiz%' and user_id is not null
group by user_id, event_dates),
prepare as 
(Select 
      user_id
      ,event_dates
      ,count(distinct event_dates) as `#days_prepared`
      ,count(distinct lower(page_location)) as number_questions
from CTE
where lower(page_location) like '%prepare%' and user_id is not null
group by user_id, event_dates),
page_views as 
(Select 
      user_id
      ,event_dates
      ,count(distinct event_dates) as `#days_platform_visited`
      ,count(event_name) as `#events_triggered`
      ,count(distinct ga_session_id ) as `#sessions`
      ,sum(engagement_time_msec)/1000 as total_time_spent_in_sec
      ,max(TIMESTAMP_MICROS(event_timestamp)) as last_activity_date
from CTE
where user_id is not null
group by user_id,event_dates),

job_apply as 
(Select 
      user_id
      ,event_dates
      ,count(distinct event_dates) as `#days_click_on_job_apply`
      ,count(distinct lower(page_location)) as job_apply_click
from CTE
where event_name = 'Jobs - Apply Now'  and user_id is not null
group by user_id, event_dates),

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
    page_views.user_id, 
     page_views.event_dates, 
     coalesce(max(page_views.`#days_platform_visited`),0) as `#days_platform_visited`, 
     coalesce(max(page_views.`#events_triggered`),0) as `#events_triggered`,
     coalesce(max(page_views.`#sessions`),0) as `#sessions`,
     coalesce(max(page_views.total_time_spent_in_sec) ,0) as total_time_spent_in_sec
    ,coalesce(max(`#days_quiz_attempted`),0) as `#days_quiz_attempted`
    ,coalesce(max(`#days_prepared`),0) as `#days_prepared`
    ,coalesce(max(number_questions),0) as number_questions
    ,coalesce(max(`#days_click_on_job_apply`),0) as `#days_click_on_job_apply`
    ,coalesce(max(job_apply_click),0) as job_apply_click
    -- ,device as last_device
from  page_views
left join quiz
on page_views.user_id = quiz.user_id
left join prepare 
on page_views.user_id = prepare.user_id
left join job_apply
on page_views.user_id = job_apply.user_id
-- left join Last_activity
-- on page_views.user_id = Last_activity.user_id
group by page_views.user_id, 
     page_views.event_dates
    --  page_views.`#days_platform_visited`, 
    --  page_views.`#events_triggered`,
    --  page_views.`#sessions`
    --  page_views.total_time_spent_in_sec