import os
from dataclasses import dataclass

@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic_name: str = 'postgres.public.users'
    group_id: str = 'cdc-consumer-group'
    auto_offset_reset: str = 'earliest'

@dataclass(frozen=True)
class PostgresConfig:
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = 5432
    database: str = 'sourcedb'
    user: str = 'postgres'
    password: str = 'postgres'

@dataclass(frozen=True)
class MySQLConfig:
    host: str = os.getenv('MYSQL_HOST', 'localhost')
    port: int = 3306
    database: str = 'targetdb'
    user: str = 'mysql'
    password: str = 'mysql'

@dataclass(frozen=True)
class DebeziumConfig:
    connect_url: str = 'http://localhost:8083'
    connector_name: str = 'postgres-connector'
