{{ 
    config(
        materialized='table'
    ) 
}}

with questions_data as (
    select * from {{ ref('fact_questions') }}
),
tags_data as (
    select creation_date,
    service,
    SPLIT(TRIM(tags, '|'), '|') tags,
    view_count
    from questions_data
)
    select 
    {{ dbt.date_trunc("month", "creation_date") }} as creation_month, 
    service, 
    tag,
    count(*) question_count,
    sum(view_count) view_count
    from tags_data, unnest(tags) tag
    group by 1,2,3
