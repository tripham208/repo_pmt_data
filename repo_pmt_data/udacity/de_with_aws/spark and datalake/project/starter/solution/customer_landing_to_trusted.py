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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1715521746885 = glueContext.create_dynamic_frame.from_catalog(database="stedi",
                                                                                     table_name="customer_landing",
                                                                                     transformation_ctx="AWSGlueDataCatalog_node1715521746885")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate <> 0

'''
SQLQuery_node1715522416724 = sparkSqlQuery(glueContext, query=SqlQuery0,
                                           mapping={"myDataSource": AWSGlueDataCatalog_node1715521746885},
                                           transformation_ctx="SQLQuery_node1715522416724")

# Script generated for node Amazon S3
AmazonS3_node1715521903392 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1715522416724,
                                                                          connection_type="s3", format="glueparquet",
                                                                          connection_options={
                                                                              "path": "s3://pmt-bucket-us-east-2/stedi/customer/trusted/",
                                                                              "partitionKeys": []},
                                                                          format_options={"compression": "snappy"},
                                                                          transformation_ctx="AmazonS3_node1715521903392")

job.commit()
