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

# Script generated for node CustomerTrusted
CustomerTrusted_node1721654804787 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1721654804787")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1721654572278 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1721654572278")

# Script generated for node SQL Query
SqlQuery1479 = '''
select al.* from al
join ct 
on al.user = ct.email
'''
SQLQuery_node1721654834941 = sparkSqlQuery(glueContext, query = SqlQuery1479, mapping = {"ct":CustomerTrusted_node1721654804787, "al":AccelerometerLanding_node1721654572278}, transformation_ctx = "SQLQuery_node1721654834941")

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1721654958961 = glueContext.getSink(path="s3://ashp-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1721654958961")
AccelerometerTrusted_node1721654958961.setCatalogInfo(catalogDatabase="ashp-lakehouse-db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1721654958961.setFormat("json")
AccelerometerTrusted_node1721654958961.writeFrame(SQLQuery_node1721654834941)
job.commit()