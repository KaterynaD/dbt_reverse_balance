dbt run-operation s1_create_table_and_stream

dbt run-operation s2_1st_day_operations

dbt run --select fact_orders_rb_from_stream

dbt run-operation s3_2nd_day_operations

dbt run --select fact_orders_rb_from_stream

dbt run-operation s4_3rd_day_operations

dbt run --select fact_orders_rb_from_stream

dbt run-operation s5_4th_day_operations

dbt run --select fact_orders_rb_from_stream


dbt run-operation s6_5_6_days_operations

dbt run --select fact_orders_rb_from_stream