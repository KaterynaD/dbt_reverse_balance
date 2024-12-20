
# This project is used for testing reverse_balance dbt Custom Materialization

To run the project add your specific integration_tests profile.

dbt_utils and dbt_reverse_balance are included in packages.yml

Run

```
dbt deps
```

Staging data and etalon tests data are provided in seeds folder as csv files.

Before testing run

```
dbt seed
```

## Reverse Balance table from a Snowflake stream

It requires DML operations to populate the stream. That's why the source table can not be created once from a seed file.
Instead run the shell file (make them executable first if needed):

```
chmod +x reverse_balance_from_stream.sh
./reverse_balance_from_stream.sh
```

The shell file runs dbt macros to create teh source table and the corresponding stream, then performs inserts/updates/deletes imitating daily users operations.
In between, there are dbt fact_orders_rb_from_stream model runs to imitate dbt nightly scheduled jobs.


To test the target table run:


```
dbt test --select fact_orders_rb_from_stream
```