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

# Script generated for node customer_trusted
customer_trusted_node1715524543007 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                   table_name="customer_trusted",
                                                                                   transformation_ctx="customer_trusted_node1715524543007")

# Script generated for node accekerineter_landing
accekerineter_landing_node1715524540940 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                        table_name="accelerometer_landing",
                                                                                        transformation_ctx="accekerineter_landing_node1715524540940")

# Script generated for node Join
Join_node1715524559952 = Join.apply(frame1=customer_trusted_node1715524543007,
                                    frame2=accekerineter_landing_node1715524540940, keys1=["email"], keys2=["user"],
                                    transformation_ctx="Join_node1715524559952")

# Script generated for node SQL Query
SqlQuery0 = '''
select user,timestamp,cast(x as float),cast(y as float),cast(z as float) from myDataSource
'''
SQLQuery_node1715525789328 = sparkSqlQuery(glueContext, query=SqlQuery0,
                                           mapping={"myDataSource": Join_node1715524559952},
                                           transformation_ctx="SQLQuery_node1715525789328")

# Script generated for node Amazon S3
AmazonS3_node1715524650585 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1715525789328,
                                                                          connection_type="s3", format="glueparquet",
                                                                          connection_options={
                                                                              "path": "s3://pmt-bucket-us-east-2/stedi/accelerometer/trusted/",
                                                                              "partitionKeys": []},
                                                                          format_options={"compression": "snappy"},
                                                                          transformation_ctx="AmazonS3_node1715524650585")

job.commit()
