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

# Script generated for node customer_curated
customer_curated_node1715524543007 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                   table_name="customer_curated",
                                                                                   transformation_ctx="customer_curated_node1715524543007")

# Script generated for node step_trainer_landing
step_trainer_landing_node1715524540940 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                       table_name="step_trainer_landing",
                                                                                       transformation_ctx="step_trainer_landing_node1715524540940")

# Script generated for node step_trainer_landing
step_trainer_landing_node1715530084476 = ApplyMapping.apply(frame=step_trainer_landing_node1715524540940, mappings=[
    ("sensorreadingtime", "long", "sensorreadingtime", "long"),
    ("serialnumber", "string", "right_serialnumber", "string"),
    ("distancefromobject", "long", "distancefromobject", "int")],
                                                            transformation_ctx="step_trainer_landing_node1715530084476")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct
    `sensorReadingTime`  ,
    `serialNumber`       ,
    `distanceFromObject` 

from cc inner join stl on cc.serialNumber = stl.right_serialnumber
'''
SQLQuery_node1715525789328 = sparkSqlQuery(glueContext, query=SqlQuery0,
                                           mapping={"cc": customer_curated_node1715524543007,
                                                    "stl": step_trainer_landing_node1715530084476},
                                           transformation_ctx="SQLQuery_node1715525789328")

# Script generated for node Amazon S3
AmazonS3_node1715524650585 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1715525789328,
                                                                          connection_type="s3", format="glueparquet",
                                                                          connection_options={
                                                                              "path": "s3://pmt-bucket-us-east-2/stedi/step_trainer/trusted/",
                                                                              "partitionKeys": []},
                                                                          format_options={"compression": "snappy"},
                                                                          transformation_ctx="AmazonS3_node1715524650585")

job.commit()
