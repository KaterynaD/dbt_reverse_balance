{% macro reverse_balance_build_table(config, sql, create_flg, target) %}



{% set unique_key_cols = config['unique_key_cols'] %}
{% set unique_key_hash = snapshot_hash_arguments(unique_key_cols) %}
{% set unique_key_col_name = config['unique_key_col_name']|default('unique_key', true) %}

{% set additive_measures_cols = config['additive_measures_cols'] %}
{% set other_cols = config['other_cols'] %}

{% set as_at_col_name = config['as_at_col_name'] %}

{# adding stg. to each unique key column #}
{% set stg_unique_key_cols = [] %}
{% set f_unique_key_cols = [] %}

{% for item in unique_key_cols %}
  {% do stg_unique_key_cols.append('stg.'~item) %}
  {% do f_unique_key_cols.append('f.'~item) %}
{% endfor %}

{% set fact_id_hash = snapshot_hash_arguments(stg_unique_key_cols + ['stg.'~as_at_col_name, 'stg.is_reverse']) %}

{% set fact_id_col_name = config['fact_id_col_name']|default('fact_id', true) %}

{% set fact_id_hash_reversed = snapshot_hash_arguments(f_unique_key_cols + ['max(f.'~as_at_col_name~')', '\'N\'']) %}

{% set fact_id_hash_replaced = snapshot_hash_arguments(stg_unique_key_cols +  ['stg.reversed'~as_at_col_name, '\'N\'']) %}

{% set fact_id_hash_reversed2 = snapshot_hash_arguments(stg_unique_key_cols+ ['stg.'~as_at_col_name, '\'Y\'']) %}

{% set RecordStatus_col_name = config['RecordStatus_col_name'] %}

{% set ReverseFlg_col_name = config['ReverseFlg_col_name'] %}

{% set LoadType_col_name = config['LoadType_col_name'] %}

with raw_data as
(
 {{ sql }}
)
,data as (
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
{{ c }},
{% endfor %}
{{ as_at_col_name }},
--
/*We may have more then 1 change in the batch. We need to add reverse data from the batch itself*/
{% for c in additive_measures_cols %} 
-lag({{ c }}) over (partition by  {% for uc in unique_key_cols %}  {{ uc }} {% if not loop.last %},{% endif %} {% endfor %} order by {{ as_at_col_name }}) prev_{{ c }},
{% endfor %}
--
lag({{ as_at_col_name }}) over (partition by  {% for uc in unique_key_cols %}  {{ uc }} {% if not loop.last %},{% endif %}  {% endfor %} order by {{ as_at_col_name }}) prev_{{ as_at_col_name }},
--
{% for c in other_cols %} 
lag({{ c }}) over (partition by  {% for uc in unique_key_cols %}  {{ uc }} {% if not loop.last %},{% endif %} {% endfor %} order by {{ as_at_col_name }}) prev_{{ c }} {% if not loop.last %},{% endif %}
{% endfor %}

--
from raw_data
)
,stg_data as (
/*reverse data from the staging if any*/
select 
{{ unique_key_col_name }},
{% for c in unique_key_cols %} 
 {{ c }}, 
{% endfor %}
--
{% for c in other_cols %} 
 prev_{{ c }} as {{ c }}, 
{% endfor %}
--
{% for c in additive_measures_cols %} 
prev_{{ c }}  as {{ c }},
{% endfor %}
--
'Y'is_reverse,
{{ as_at_col_name }},
prev_{{ as_at_col_name }} reversed{{ as_at_col_name }},
case when {% for c in additive_measures_cols %} prev_{{ c }} is null  {% if not loop.last %} and {% endif %} {% endfor %} then 'N' else 'Y' end  HasReversedRecord
from data stg
where 
{% for c in additive_measures_cols %} 
prev_{{ c }} is not null {% if not loop.last %} or {% endif %} 
{% endfor %}
--
union all
--
/*forward data from the stage*/
select 
{{ unique_key_col_name }},
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
--
'N' is_reverse,
{{ as_at_col_name }},
null reversed{{ as_at_col_name }},
case when {% for c in additive_measures_cols %} prev_{{ c }} is null  {% if not loop.last %} and {% endif %} {% endfor %} then 'N' else 'Y' end HasReversedRecord
from data
)
{% if create_flg %}

,reverse_data as (
/*Placeholder while the table to lookup does not exists*/
select
null {{ unique_key_col_name }},
null {{ fact_id_col_name }}
where 1=2
)
{% else %}

,reverse_data as (
select
f.{{ unique_key_col_name }},
{{ fact_id_hash_reversed }} as {{ fact_id_col_name }}
from stg_data stg
join {{ target }} f
on  stg.{{ unique_key_col_name }}  = f.{{ unique_key_col_name }}
and stg.{{ as_at_col_name }}>=f.{{ as_at_col_name }}
where f.{{ fact_id_col_name }}_replaced is null
group by f.{{ unique_key_col_name }}, {% for c in unique_key_cols %} f.{{ c }} {% if not loop.last %} , {% endif %}{% endfor %}
)

{% endif %}

/*forward balance data - new and changes, can contain reverse data from the batch - multiple updates during the day*/
select
{{ fact_id_hash }} as {{ fact_id_col_name }},

{% if ReverseFlg_col_name|length > 0 %}
stg.is_reverse {{ ReverseFlg_col_name }},
{% endif %}

case when stg.is_reverse='Y' then {{ fact_id_hash_replaced }} end {{ fact_id_col_name }}_replaced,
--
stg.{{ unique_key_col_name }},
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
--
{{ as_at_col_name }}

{% if LoadType_col_name|length > 0 %}
, case when r.{{ fact_id_col_name }} is null then 'Insert' else 'Update' end ::varchar(10) {{ LoadType_col_name }}
{% endif %}

{% if RecordStatus_col_name|length > 0 %}
 , case when stg.is_reverse='Y' then 'Closed' else 'Active' end {{ RecordStatus_col_name }}
{% endif %}
from stg_data stg
{% if LoadType_col_name|length > 0 %}
 left outer join reverse_data r
 on stg.{{ unique_key_col_name }}  = r.{{ unique_key_col_name }}
{% endif %}

{% if not create_flg %}



--
union all
--
/*reverse balance data - old*/
select
{{ fact_id_hash_reversed2 }} as {{ fact_id_col_name }},
{% if ReverseFlg_col_name|length > 0 %}
'Y' {{ ReverseFlg_col_name }},
{% endif %}
f.{{ fact_id_col_name }} as {{ fact_id_col_name }}_replaced,
--
f.{{ unique_key_col_name }},
{% for c in unique_key_cols %} 
 f.{{ c }}, 
{% endfor %}
--
{% for c in other_cols %} 
f.{{ c }}, 
{% endfor %}
--
{% for c in additive_measures_cols %} 
-f.{{ c }} as {{ c }},
{% endfor %}
--
stg.{{ as_at_col_name }}

{% if LoadType_col_name|length > 0 %}
, case 
 when 
  /*soft condition can be: =0 or <0 or isDelete='Y' or isDelete=TRUE*/
 {% for c in additive_measures_cols %} stg.{{ c }} = 0   {% if not loop.last %} and {% endif %} {% endfor %}
 then 'Delete'  
 else 'Reverse' 
 end ::varchar(10) {{ LoadType_col_name }}
{% endif %}

{% if RecordStatus_col_name|length > 0 %}
 , 'Closed'  {{ RecordStatus_col_name }}
{% endif %}
from reverse_data r
join {{ target }} f
on r.{{ fact_id_col_name }} = f.{{ fact_id_col_name }}
join stg_data stg
on r.{{ unique_key_col_name }} = stg.{{ unique_key_col_name }}
where HasReversedRecord='N'

{% endif %}



{% endmacro %}

{% macro reverse_balance_insert(target, config, sql) %}

{% set RecordStatus_col_name = config['RecordStatus_col_name'] %}
{% set fact_id_col_name = config['fact_id_col_name']|default('fact_id', true) %}
{# sql to load new data is the same as to create table #}
{% set new_data_sql = dbt_reverse_balance.reverse_balance_build_table(config = config, sql = model['compiled_code'],create_flg=false, target=target) %}

insert into {{ target }}
{{ new_data_sql }}
;

{% if RecordStatus_col_name|length > 0 %}
update {{ target }}
set RecordStatus = 'Closed'
from {{ target }} c
where {{ target }}.{{ fact_id_col_name }}=c.{{ fact_id_col_name }}_replaced;
  {% endif %}
{% endmacro %}