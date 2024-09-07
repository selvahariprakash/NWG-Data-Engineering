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

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1721667512787 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1721667512787")

# Script generated for node CustomerCurated
CustomerCurated_node1721669059730 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1721669059730")

# Script generated for node SQL Query
SqlQuery1611 = '''
select * from stl
join cc
on cc.serialnumber = stl.serialNumber
'''
SQLQuery_node1721669088363 = sparkSqlQuery(glueContext, query = SqlQuery1611, mapping = {"stl":StepTrainerLanding_node1721667512787, "cc":CustomerCurated_node1721669059730}, transformation_ctx = "SQLQuery_node1721669088363")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1721669604904 = glueContext.getSink(path="s3://ashp-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1721669604904")
StepTrainerTrusted_node1721669604904.setCatalogInfo(catalogDatabase="ashp-lakehouse-db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1721669604904.setFormat("json")
StepTrainerTrusted_node1721669604904.writeFrame(SQLQuery_node1721669088363)
job.commit()