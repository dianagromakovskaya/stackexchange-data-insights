{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('staging', 'ai_posts') }}

),

renamed as (

    select
        id,
        post_type_id,
        {{ get_post_type_description(post_type_id) }} post_type,
        accepted_answer_id,
        creation_date,
        score,
        view_count,
        body,
        owner_user_id,
        last_editor_user_id,
        last_edit_date,
        last_activity_date,
        title,
        tags,
        answer_count,
        comment_count,
        content_license,
        parent_id,
        closed_date,
        favorite_count,
        community_owned_date,
        last_editor_display_name,
        owner_display_name

    from source

)

select * from renamed
where  {{ dbt.date_trunc("month", "creation_date") }} >= '2023-08-01'

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}