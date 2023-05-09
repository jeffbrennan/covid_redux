{% macro fix_col_names(relation) -%}

    {%- set all_cols = adapter.get_columns_in_relation(relation) -%}
    {%- set include_cols = [] %}

    {%- for col in all_cols %}
        {%- do include_cols.append(col.column) -%}
    {%- endfor %}


    select
        {%- for col in include_cols %}
            "{{col}}" as {{modules.re.sub('[^a-zA-Z0-9_]', '', col|lower|replace('-','_')|replace(' ','_')|replace('\n', '_')|replace('+', '_plus'))|replace('__', '_')}}
            {%- if not loop.last -%}
            ,
            {%- endif -%}
        {%- endfor %}
    from {{relation}}

{%- endmacro %}