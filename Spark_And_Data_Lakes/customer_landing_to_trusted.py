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

# Script generated for node CustomerLanding
CustomerLanding_node1721653173563 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1721653173563")

# Script generated for node PrivacyFilter
SqlQuery1454 = '''
select * from CustomerLanding
where sharewithresearchasofdate is not null
'''
PrivacyFilter_node1721653837509 = sparkSqlQuery(glueContext, query = SqlQuery1454, mapping = {"CustomerLanding":CustomerLanding_node1721653173563}, transformation_ctx = "PrivacyFilter_node1721653837509")

# Script generated for node CustomerTrusted
CustomerTrusted_node1721653956799 = glueContext.getSink(path="s3://ashp-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1721653956799")
CustomerTrusted_node1721653956799.setCatalogInfo(catalogDatabase="ashp-lakehouse-db",catalogTableName="customer_trusted")
CustomerTrusted_node1721653956799.setFormat("json")
CustomerTrusted_node1721653956799.writeFrame(PrivacyFilter_node1721653837509)
job.commit()