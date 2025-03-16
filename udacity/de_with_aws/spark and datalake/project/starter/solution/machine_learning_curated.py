import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1715524543007 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                        table_name="accelerometer_trusted",
                                                                                        transformation_ctx="accelerometer_trusted_node1715524543007")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1715524540940 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                       table_name="step_trainer_trusted",
                                                                                       transformation_ctx="step_trainer_trusted_node1715524540940")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct
        `sensorReadingTime`  ,
    `serialNumber`       ,
    `distanceFromObject` ,
    `user`      ,
    `x`         ,
    `y`         ,
    `z`         

from at inner join stt on at.timestamp = stt.sensorReadingTime
'''
SQLQuery_node1715525789328 = sparkSqlQuery(glueContext, query=SqlQuery0,
                                           mapping={"at": accelerometer_trusted_node1715524543007,
                                                    "stt": step_trainer_trusted_node1715524540940},
                                           transformation_ctx="SQLQuery_node1715525789328")

# Script generated for node Amazon S3
AmazonS3_node1715524650585 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1715525789328,
                                                                          connection_type="s3", format="glueparquet",
                                                                          connection_options={
                                                                              "path": "s3://pmt-bucket-us-east-2/stedi/step_trainer/curated/",
                                                                              "partitionKeys": []},
                                                                          format_options={"compression": "snappy"},
                                                                          transformation_ctx="AmazonS3_node1715524650585")

job.commit()
