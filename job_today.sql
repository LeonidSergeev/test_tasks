with data as 
(
    select *
    from entity.test_data
    where platform not in ('Web','Unknown')
),

conversion_data as 
(
select count(distinct id) as n
    ,platform
    ,variation_name
    ,action_type
    from data
    where action_type in ('Chat','Registration','Application update')
    group by platform
    ,variation_name
    ,action_type
),

conversion_final as 
(
    select sum(case when action_type = 'Registration' then n end ) as registration_n
    ,sum(case when action_type = 'Application update' then n end ) as application_n
    ,sum(case when action_type = 'Chat' then n end ) as chat_n
    ,application_n/Registration_n::float * 100 as reg_application_conversion
    ,chat_n/Registration_n::float * 100 as reg_chat_conversion
    ,platform
    ,variation_name
    from conversion_data
    group by platform
    ,variation_name
),

average_chats as 
(
    select 
    id
    ,sum(actions_cnt) as chats
    ,platform
    ,variation_name
    ,action_type
    from data
    where action_type = 'Chat'
    group by platform
    ,variation_name
    ,action_type
    ,id
),

average_data_final as 
(
    select 
    median(chats) as median_chats
    --,avg(chats)::float as average_chats
    ,platform
    ,variation_name
    ,'median_chats_per_host' as metrics
    from average_chats
    group by platform
    ,variation_name
),

min_date_chats as 
(
    select 
    id
    ,min(action_date) as min_action_date
    ,platform
    ,variation_name
    ,action_type
    from data
    where action_type = 'Chat'
    group by platform
    ,variation_name
    ,action_type
    ,id 
),

data_for_average_days as 
(
select min_date_chats.id
    ,min_action_date
    ,min_date_chats.platform as platform
    ,min_date_chats.variation_name as variation_name
    ,min_date_chats.action_type as action_type
    ,date_diff
from min_date_chats 
left join data
on min_date_chats.id = data.id
    and min_date_chats.action_type = data.action_type
    and min_date_chats.min_action_date = data.action_date
),

average_days_final as 
(
    select 
    avg(date_diff)::float as average_diff
    --,median(date_diff) as median_diff
    ,platform
    ,variation_name
    ,'average_day_diff' as metrics
    from data_for_average_days
    group by platform
    ,variation_name
),

retention as 
(
    select 
    count(distinct case when date_diff = 0 then id end) as day0
    ,count(distinct case when date_diff >= 1 then id end) as day1
    ,count(distinct case when date_diff >= 7 then id end) as day7
    ,count(distinct case when date_diff >= 15 then id end) as day15
    ,day1/day0::float * 100 as day_1_retention
    ,day7/day0::float * 100 as day_7_retention
    ,day15/day0::float * 100 as day_15_retention
    ,platform
    ,variation_name
    from data 
    where action_type = 'Coming online'
    group by platform,variation_name
)


select *  
from average_data_final
    
union all 

select * 
from average_days_final
    
union all 
    
select reg_application_conversion
,platform
,variation_name
,'reg_application_conversion' as metrics
from conversion_final
    
union all 
    
select reg_chat_conversion
,platform
,variation_name
,'reg_chat_conversion' as metrics
from conversion_final

union all 
    
select day_1_retention
,platform
,variation_name
,'day_1_retention' as metrics
from retention

union all 
    
select day_7_retention
,platform
,variation_name
,'day_7_retention' as metrics
from retention

union all 
    
select day_15_retention
,platform
,variation_name
,'day_15_retention' as metrics
from retention