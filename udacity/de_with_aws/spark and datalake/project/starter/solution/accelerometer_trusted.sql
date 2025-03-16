CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_trusted`
(
    `user`      string,
    `timeStamp` bigint,
    `x`         float,
    `y`         float,
    `z`         float
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '1'
        ) LOCATION 's3://pmt-bucket-us-east-2/stedi/accelerometer/trusted/'
    TBLPROPERTIES ('has_encrypted_data' = 'false');