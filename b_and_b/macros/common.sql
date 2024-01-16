{%- macro generate_id(fields) -%}

        MD5(
            cast(
                {%- for field in fields -%}
                    coalesce(cast({{ field }} as STRING), '-') 
                {%- if not loop.last %}|| '-' ||{% endif -%}
                {%- endfor -%}
            as STRING)
        )
        
{%- endmacro -%}