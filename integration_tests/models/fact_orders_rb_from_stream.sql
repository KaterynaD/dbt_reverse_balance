{{ config(
   materialized='reverse_balance_from_stream',
   
   unique_key_cols=['order_id','part_id'],
   unique_key_col_name='ordr_unique_key',

   additive_measures_cols=['quantity'],

   other_cols=['OrderDate']


) }}
 select * from {{ target.database }}.{{ target.schema }}.strm_orders