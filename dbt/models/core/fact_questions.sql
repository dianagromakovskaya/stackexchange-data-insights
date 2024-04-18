{{
    config(
        materialized='table'
    )
}}

with ai_questions as (
    select *,
       concat('https://ai.stackexchange.com/questions/', id) question_link,
        'ai' as service
    from {{ ref('stg_ai_posts') }}
    where post_type = 'Question'
),
datascience_questions as(
    select *,
        concat('https://datascience.stackexchange.com/questions/', id) question_link,
        'datascience' as service
    from {{ ref('stg_datascience_posts') }}
    where post_type = 'Question'
),
genai_questions as (
    select *,
        concat('https://genai.stackexchange.com/questions/', id) question_link,
        'genai' as service
    from {{ ref('stg_genai_posts') }}
    where post_type = 'Question'
),
questions_unioned as (
    select * from ai_questions
    union all
    select * from datascience_questions
    union all
    select * from genai_questions
)
select *
from questions_unioned