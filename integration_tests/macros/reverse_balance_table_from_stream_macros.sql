{% macro s1_create_table_and_stream() %}

{% set source_table_and_stream_creation %}

create  or replace table {{ target.database }}.{{ target.schema }}.orders (
OrderDate datetime,
order_id numeric(10,0),
part_id numeric (10,0),
quantity numeric (10,0)
);

create  or replace stream {{ target.database }}.{{ target.schema }}.strm_orders on table {{ target.database }}.{{ target.schema }}.orders;

drop table if exists {{ target.database }}.{{ target.schema }}.fact_orders_rb_from_stream;
{% endset %}

{% do run_query(source_table_and_stream_creation) %}



{% endmacro %}

{% macro s2_1st_day_operations() %}

{% set operations_day1 %}

insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-02',121,1,10);


insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-02',121,2,15);
--
insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-02',122,1,100);


insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-02',122,3,25);
--
insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-02',123,1,12);


insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-02',123,2,115);
{% endset %}

{% do run_query(operations_day1) %}



{% endmacro %}


{% macro s3_2nd_day_operations() %}

{% set operations_day2 %}

delete from {{ target.database }}.{{ target.schema }}.orders
where order_id=122
and part_id=3;
--
update {{ target.database }}.{{ target.schema }}.orders
set quantity=15
where order_id=123
and part_id=2;
--
insert into {{ target.database }}.{{ target.schema }}.orders
values('2024-12-03',124,4,45);

{% endset %}

{% do run_query(operations_day2) %}



{% endmacro %}


{% macro s4_3rd_day_operations() %}

{% set operations_day3 %}

delete from {{ target.database }}.{{ target.schema }}.orders
where order_id=124
and part_id=4;

{% endset %}

{% do run_query(operations_day3) %}



{% endmacro %}


{% macro s5_4th_day_operations() %}

{% set operations_day4 %}

update {{ target.database }}.{{ target.schema }}.orders
set quantity=10
where order_id=123
and part_id=2;

{% endset %}

{% do run_query(operations_day4) %}



{% endmacro %}



{% macro s6_5_6_days_operations() %}

{% set operations_days_5_6 %}

update {{ target.database }}.{{ target.schema }}.orders
set quantity=120
where order_id=122
and part_id=1;

update {{ target.database }}.{{ target.schema }}.orders
set quantity=20
where order_id=122
and part_id=1;

{% endset %}

{% do run_query(operations_days_5_6) %}



{% endmacro %}