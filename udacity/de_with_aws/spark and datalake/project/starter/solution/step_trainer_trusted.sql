CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_trusted`
(
    `sensorReadingTime`  bigint,
    `serialNumber`       string,
    `distanceFromObject` bigint
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '1'
        ) LOCATION 's3://pmt-bucket-us-east-2/stedi/step_trainer/trusted/'
    TBLPROPERTIES ('has_encrypted_data' = 'false');