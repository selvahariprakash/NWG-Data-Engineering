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
AccelerometerTrusted_node1721669934160 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1721669934160")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1721669910107 = glueContext.create_dynamic_frame.from_catalog(database="ashp-lakehouse-db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1721669910107")

# Script generated for node SQL Query
SqlQuery1616 = '''
select * from at
join stt 
on at.timestamp = stt.sensorreadingtime
'''
SQLQuery_node1721669967005 = sparkSqlQuery(glueContext, query = SqlQuery1616, mapping = {"at":AccelerometerTrusted_node1721669934160, "stt":StepTrainerTrusted_node1721669910107}, transformation_ctx = "SQLQuery_node1721669967005")

# Script generated for node MachineLearningCurated
MachineLearningCurated_node1721670150922 = glueContext.getSink(path="s3://ashp-lakehouse/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1721670150922")
MachineLearningCurated_node1721670150922.setCatalogInfo(catalogDatabase="ashp-lakehouse-db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1721670150922.setFormat("json")
MachineLearningCurated_node1721670150922.writeFrame(SQLQuery_node1721669967005)
job.commit()