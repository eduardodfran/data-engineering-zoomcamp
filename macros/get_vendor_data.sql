{% macro get_vendor_names(vendor_id) -%}
case
    when {{ vendor_id }} = 1 then 'Creative Mobile Technologies'
    when {{ vendor_id }} = 2 then 'Verifone Inc.'
    when {{ vendor_id }} = 3 then 'Unknown Vendor'
    else 'Unknown Vendor'
end as vendor_name
{%- endmacro %}