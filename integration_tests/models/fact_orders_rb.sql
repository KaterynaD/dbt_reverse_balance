{{ config(
   materialized='reverse_balance',
   
   unique_key_cols=['order_id','part_id'],
   unique_key_col_name='ordr_unique_key',

   as_at_col_name='BookDate',

   additive_measures_cols=['quantity'],

   loaddate='LoadDate',

   other_cols=['OrderDate','LineStatus'],

   RecordStatus_col_name='RecordStatus',

   fact_id_col_name='fact_orders_id',

   ReverseFlg_col_name='is_reverse',

   LoadType_col_name='LoadType'


) }}

select * from {{ source('dwh', 'stg_orders') }}
where loaddate='{{  var('loaddate') }}'