{% macro reverse_balance_build_table_from_stream(config, sql) %}

{% set unique_key_cols = config['unique_key_cols'] %}
{% set unique_key_hash = snapshot_hash_arguments(unique_key_cols) %}
{% set unique_key_col_name = config['unique_key_col_name']|default('unique_key', true) %}

{% set additive_measures_cols = config['additive_measures_cols'] %}
{% set other_cols = config['other_cols'] %}

{% set as_at_col_name = config['as_at_col_name']|default('as_at_tmstmp', true) %}

{% set fact_id_hash = snapshot_hash_arguments(unique_key_cols + [as_at_col_name, 'case when LoadType in (\'Delete\', \'Reverse\') then \'Y\' else \'N\' end']) %}
{% set fact_id_col_name = config['fact_id_col_name']|default('fact_id', true) %}


with raw_data as
(
 {{ sql }}
)
,stg_data as (
select 
{{ unique_key_hash }} as {{ unique_key_col_name }},
{% for c in unique_key_cols %} 
 {{ c }}, 
{% endfor %}
--
{% for c in other_cols %} 
 {{ c }}, 
{% endfor %}
--
{% for c in additive_measures_cols %} 
  case when metadata$action='DELETE'  then -1 else 1 end * {{ c }} as {{ c }},
{% endfor %}
--
case when metadata$action='INSERT' and metadata$isupdate=FALSE then 'Insert' 
     when metadata$action='INSERT' and metadata$isupdate=TRUE then 'Update' 
     when metadata$action='DELETE' and metadata$isupdate=FALSE then 'Delete'
     when metadata$action='DELETE' and metadata$isupdate=TRUE then 'Reverse'  
end LoadType,
CURRENT_TIMESTAMP() {{ as_at_col_name }}
from raw_data 
)
select
{{ fact_id_hash }} as {{ fact_id_col_name }},
case when LoadType in ('Delete', 'Reverse') then 'Y' else 'N' end as isReverse,
{{ unique_key_hash }} as {{ unique_key_col_name }},
{% for c in unique_key_cols %} 
 {{ c }}, 
{% endfor %}
--
{% for c in other_cols %} 
 {{ c }}, 
{% endfor %}
--
{% for c in additive_measures_cols %} 
  {{ c }}, 
{% endfor %}
{{ as_at_col_name }},
LoadType
from stg_data 

{% endmacro %}

{% macro reverse_balance_insert_from_stream(target, config, sql) %}

{# sql to load new data is the same as to create table #}
{% set new_data_sql = dbt_reverse_balance.reverse_balance_build_table_from_stream(config = config, sql = model['compiled_code']) %}

insert into {{ target }}
{{ new_data_sql }}
;
{% endmacro %}