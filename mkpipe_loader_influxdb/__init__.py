import gc
from datetime import datetime

from mkpipe.exceptions import ConfigError, LoadError
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig, WriteStrategy
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.strategy import resolve_write_strategy
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class InfluxDBLoader(BaseLoader, variant='influxdb'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host or 'localhost'
        self.port = connection.port or 8086
        self.token = connection.api_key or connection.oauth_token
        self.org = connection.extra.get('org', '')
        self.bucket = connection.database

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        strategy = resolve_write_strategy(table, data)

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_strategy': strategy.value,
        })

        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS

            url = f'http://{self.host}:{self.port}'
            client = InfluxDBClient(url=url, token=self.token, org=self.org)

            match strategy:
                case WriteStrategy.REPLACE:
                    delete_api = client.delete_api()
                    delete_api.delete(
                        start='1970-01-01T00:00:00Z',
                        stop='2099-12-31T23:59:59Z',
                        predicate=f'_measurement="{target_name}"',
                        bucket=self.bucket,
                        org=self.org,
                    )
                    logger.info({'table': target_name, 'status': 'measurement_deleted'})
                case WriteStrategy.APPEND | WriteStrategy.UPSERT:
                    pass
                case _:
                    raise ConfigError(
                        f"InfluxDB loader does not support write_strategy: {strategy.value}"
                    )

            write_api = client.write_api(write_options=SYNCHRONOUS)

            tag_columns = self.connection.extra.get('tag_columns', [])
            field_columns = self.connection.extra.get('field_columns', [])
            time_column = self.connection.extra.get('time_column', '_time')

            rows = [row.asDict(recursive=True) for row in df.collect()]
            batchsize = table.batchsize or 10000

            for i in range(0, len(rows), batchsize):
                batch = rows[i:i + batchsize]
                points = []
                for row in batch:
                    tags = {k: str(row[k]) for k in tag_columns if k in row and row[k] is not None}
                    time_val = row.get(time_column)

                    fields = {}
                    if field_columns:
                        fields = {k: row[k] for k in field_columns if k in row and row[k] is not None}
                    else:
                        skip = set(tag_columns) | {time_column, '_measurement', 'etl_time', 'mkpipe_id'}
                        fields = {k: v for k, v in row.items() if k not in skip and v is not None}

                    if not fields:
                        continue

                    point = {
                        'measurement': target_name,
                        'tags': tags,
                        'fields': fields,
                    }
                    if time_val:
                        point['time'] = str(time_val)
                    points.append(point)

                if points:
                    write_api.write(bucket=self.bucket, org=self.org, record=points)

            write_api.close()
            client.close()
        except (ConfigError, LoadError):
            raise
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        df.unpersist()
        gc.collect()

        logger.info({
            'table': target_name,
            'status': 'loaded',
            'rows': len(rows),
        })
