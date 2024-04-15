{{
    config(
        materialized='table'
    )
}}

with ai_questions as (
    select *,
        'ai' as service
    from {{ ref('stg_ai_posts') }}
    where post_type = 'Question'
),
datascience_questions as(
    select *,
        'datascience' as service
    from {{ ref('stg_datascience_posts') }}
    where post_type = 'Question'
),
questions_unioned as (
    select * from ai_questions
    union all
    select * from datascience_questions
)
select *
from questions_unioned