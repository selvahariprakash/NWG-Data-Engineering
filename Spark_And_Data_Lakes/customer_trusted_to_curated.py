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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1721668338477 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1721668338477")

# Script generated for node CustomerTrusted
CustomerTrusted_node1721668300703 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1721668300703")

# Script generated for node Join
SqlQuery2706 = '''
select ct.* from ct
inner join at
on at.user = ct.email
'''
Join_node1721668367267 = sparkSqlQuery(glueContext, query = SqlQuery2706, mapping = {"at":AccelerometerTrusted_node1721668338477, "ct":CustomerTrusted_node1721668300703}, transformation_ctx = "Join_node1721668367267")

# Script generated for node Distinct
SqlQuery2707 = '''
select distinct * from myDataSource
'''
Distinct_node1721764837989 = sparkSqlQuery(glueContext, query = SqlQuery2707, mapping = {"myDataSource":Join_node1721668367267}, transformation_ctx = "Distinct_node1721764837989")

# Script generated for node CustomerCurated
CustomerCurated_node1721668735809 = glueContext.getSink(path="s3://ashp-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1721668735809")
CustomerCurated_node1721668735809.setCatalogInfo(catalogDatabase="ashp-lakehouse-db",catalogTableName="customer_curated")
CustomerCurated_node1721668735809.setFormat("json")
CustomerCurated_node1721668735809.writeFrame(Distinct_node1721764837989)
job.commit()