-- Analytics mart: number of valid users per city.
select
    city,
    count(*) as user_count
from {{ ref('stg_users') }}
group by city
order by user_count desc
