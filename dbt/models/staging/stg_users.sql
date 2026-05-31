-- Silver layer: lightly enriches the raw cleaned users with derived attributes
-- (email domain, age band) used by the downstream analytics marts.
with source as (

    select * from {{ source('raw', 'users') }}

),

enriched as (

    select
        user_id,
        name,
        email,
        lower(split_part(email, '@', 2)) as email_domain,
        phone,
        zip_code,
        age,
        case
            when age between 18 and 29 then '18-29'
            when age between 30 and 44 then '30-44'
            when age between 45 and 64 then '45-64'
            else '65+'
        end as age_band,
        city
    from source

)

select * from enriched
