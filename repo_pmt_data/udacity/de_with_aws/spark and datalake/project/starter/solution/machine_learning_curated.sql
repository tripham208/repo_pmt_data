CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`machine_learning_curated`
(
    `sensorReadingTime`  bigint,
    `serialNumber`       string,
    `distanceFromObject` bigint,
    `user`      string,
    `x`         float,
    `y`         float,
    `z`         float
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '1'
        ) LOCATION 's3://pmt-bucket-us-east-2/stedi/step_trainer/curated/'
    TBLPROPERTIES ('has_encrypted_data' = 'false');