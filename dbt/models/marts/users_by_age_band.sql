-- Analytics mart: distribution of users across age bands.
select
    age_band,
    count(*) as user_count
from {{ ref('stg_users') }}
group by age_band
order by age_band
