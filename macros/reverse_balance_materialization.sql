{% materialization reverse_balance_from_stream, adapter='snowflake', supported_languages=['sql']%}



  

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

 
  {% set config = model['config'] %}


  
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