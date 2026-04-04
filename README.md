# mkpipe-loader-influxdb

InfluxDB loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into InfluxDB as time-series data points using `influxdb-client` write API.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  influxdb_target:
    variant: influxdb
    host: localhost
    port: 8086
    database: my_bucket
    api_key: my-influx-token
    extra:
      org: my-org
      time_column: _time         # column to use as InfluxDB timestamp (default: _time)
      tag_columns: [host, region] # columns written as InfluxDB tags
      field_columns: [cpu, mem]   # columns written as fields (all non-tag, non-time if omitted)
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_influxdb
    source: pg_source
    destination: influxdb_target
    tables:
      - name: public.metrics
        target_name: cpu_usage
        replication_method: full
        batchsize: 5000
```

---

## Write Strategy

Control how data is written to InfluxDB:

```yaml
      - name: public.metrics
        target_name: cpu_usage
        write_strategy: upsert       # append | replace | upsert
```

| Strategy | InfluxDB Behavior |
|---|---|
| `append` | Write data points (default for incremental) |
| `replace` | Delete the measurement (all time range), then write (default for full) |
| `upsert` | Same as `append` — InfluxDB naturally deduplicates by timestamp + tag set. Writing a point with the same timestamp and tags overwrites the existing point. |

> **Note:** InfluxDB uses timestamp + tag set as a natural primary key. `write_key` is not required — deduplication happens automatically.

---

## Column Mapping

Rows from the DataFrame are written as InfluxDB line protocol points:

| DataFrame column | InfluxDB role | How to configure |
|---|---|---|
| `time_column` value | Point timestamp | `extra.time_column` (default: `_time`) |
| `tag_columns` values | Tags (indexed) | `extra.tag_columns: [col1, col2]` |
| All other columns | Fields (values) | Default; or restrict via `extra.field_columns` |

Auto-added columns (`etl_time`, `mkpipe_id`) are excluded from fields.

---

## Write Throughput

`batchsize` controls how many rows are sent per write API call:

```yaml
      - name: public.metrics
        target_name: cpu_usage
        replication_method: full
        batchsize: 10000    # default: 10000 points per write call
```

### Performance Notes

- Write throughput is limited by the InfluxDB server and network. Larger `batchsize` reduces round-trips.
- 5,000–10,000 points per batch is a safe default for most deployments.
- All data is collected on the Spark driver (`df.collect()`) before writing — not suitable for datasets larger than available driver memory.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | InfluxDB measurement name to write into |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `batchsize` | int | `10000` | Points per write API call |
| `write_strategy` | string | — | `append`, `replace`, `upsert` |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |

### Extra Connection Parameters

| Key | Default | Description |
|---|---|---|
| `org` | `""` | InfluxDB organization name |
| `time_column` | `_time` | Column used as the point timestamp |
| `tag_columns` | `[]` | Columns written as InfluxDB tags (indexed, string values) |
| `field_columns` | `[]` | Columns written as fields (if empty, all non-tag/non-time columns) |
