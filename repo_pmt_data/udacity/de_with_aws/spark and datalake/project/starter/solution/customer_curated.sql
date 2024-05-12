CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_curated`
(
    `customerName`              string,
    `email`                     string,
    `phone`                     string,
    `birthDay`                  string,
    `serialNumber`              string,
    `registrationDate`          bigint,
    `lastUpdateDate`            bigint,
    `shareWithResearchAsOfDate` bigint,
    `shareWithPublicAsOfDate`   bigint
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '1'
        ) LOCATION 's3://pmt-bucket-us-east-2/stedi/customer/curated/'
    TBLPROPERTIES ('has_encrypted_data' = 'false');