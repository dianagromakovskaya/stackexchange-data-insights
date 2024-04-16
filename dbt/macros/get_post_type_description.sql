{#
    This macro returns the description of the post_type
    https://data.stackexchange.com/stackoverflow/query/36599/show-all-types
#}

{% macro get_post_type_description(post_type_id) -%}

    case {{ dbt.safe_cast("post_type_id", api.Column.translate_type("integer")) }}  
        when 1 then 'Question'
        when 2 then 'Answer'
        when 3 then 'Wiki'
        when 4 then 'TagWikiExcerpt'
        when 5 then 'TagWiki'
        when 6 then 'ModeratorNomination'
        when 7 then 'WikiPlaceholder'
        when 8 then 'PrivilegeWiki'
        else 'EMPTY'
    end

{%- endmacro %}