{% macro fix_date_cols(relation, except_col=[], prefix='value') -%}

    {%- set all_cols = adapter.get_columns_in_relation(relation) -%}
    {%- set include_cols = [] %}

     {%- for col in all_cols %}
        {%- if col.column not in except_col %}
            {%- do include_cols.append(col.column) -%}
        {%- endif %}
    {%- endfor %}

    select
        {%- for col in except_col %}
            {{col}},
        {%- endfor %}
        {%- for col in include_cols %}
            "{{col}}" as {{prefix~'_'~col|replace('/', '_')}}
            {%- if not loop.last -%}
            ,
            {%- endif -%}
        {%- endfor %}
    from {{relation}}

{%- endmacro %}