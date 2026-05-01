SET 'execution.checkpointing.interval' = '5 s';
SET 'pipeline.name' = 'icestream-it-job';

CREATE CATALOG icestream_cat WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'http://rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin',
  'client.region' = 'us-east-1'
);

CREATE TEMPORARY TABLE kafka_src (
  id BIGINT,
  name STRING,
  ts BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'icestream-input',
  'properties.bootstrap.servers' = 'kafka:9093',
  'properties.group.id' = 'icestream-flink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

INSERT INTO icestream_cat.db.events /*+ OPTIONS('upsert-enabled'='true') */
SELECT id, name, ts FROM kafka_src;
