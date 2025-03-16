CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing`
(
    `sensorReadingTime`  bigint,
    `serialNumber`       string,
    `distanceFromObject` bigint
)
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '1'
        ) LOCATION 's3://pmt-bucket-us-east-2/stedi/step_trainer/landing/'
    TBLPROPERTIES ('has_encrypted_data' = 'false');