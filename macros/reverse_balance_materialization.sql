{% materialization reverse_balance_from_stream, adapter='snowflake', supported_languages=['sql']%}


  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

 
  {% set config = model['config'] %}

  {% set unique_key_cols = config['unique_key_cols'] %}
   {% if unique_key_cols|length == 0 %}
   {{ exceptions.raise_compiler_error('Required parameter "unique_key_cols" is not set!') }}
  {% endif %}

  {% set additive_measures_cols = config['additive_measures_cols'] %}
  {% if additive_measures_cols|length == 0 %}
   {{ exceptions.raise_compiler_error('Required parameter "additive_measures_cols" is not set!') }}
  {% endif %}



  
  {% set grant_config = config.get('grants') %}

  {% set target_table = model.get('alias', model.get('name')) %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') %}

  {% if not target_relation.is_table %}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {% endif %}

  



  {{ run_hooks(pre_hooks) }}

  {% if not target_relation_exists %}
  
   {% set build_sql = dbt_reverse_balance.reverse_balance_build_table_from_stream(config = config, sql = model['compiled_code']) %}
   {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

   {% set final_sql = dbt_reverse_balance.reverse_balance_insert_from_stream(target = target_relation, config = config, sql = model['compiled_code']) %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  
  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% materialization reverse_balance, default%}


  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}


  {% set config = model['config'] %}

  {% set unique_key_cols = config['unique_key_cols'] %}
  {% if unique_key_cols|length == 0 %}
   {{ exceptions.raise_compiler_error('Required parameter "unique_key_cols" is not set!') }}
  {% endif %}

  {% set additive_measures_cols = config['additive_measures_cols'] %}
  {% if additive_measures_cols|length == 0 %}
   {{ exceptions.raise_compiler_error('Required parameter "additive_measures_cols" is not set!') }}
  {% endif %}

  {% set as_at_col_name = config['as_at_col_name'] %}
  {% if as_at_col_name|length == 0 %}
   {{ exceptions.raise_compiler_error('Required parameter "as_at_col_name" is not set!') }}
  {% endif %}
  
   

  

  {% set grant_config = config.get('grants') %}

  {% set target_table = model.get('alias', model.get('name')) %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') %}

  {% if not target_relation.is_table %}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {% endif %}

    



  {{ run_hooks(pre_hooks) }}

  {% if not target_relation_exists %}

   {% set build_sql = dbt_reverse_balance.reverse_balance_build_table(config = config, sql = model['compiled_code'],create_flg=true, target = target_relation) %}
   {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

   {% set final_sql = dbt_reverse_balance.reverse_balance_insert(target = target_relation, config = config, sql = model['compiled_code']) %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  
  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}