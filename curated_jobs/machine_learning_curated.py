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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step trainer trusted
steptrainertrusted_node1698316708967 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1698316708967",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1698316955288 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1698316955288",
)

# Script generated for node Curated Join
SqlQuery0 = """
select * from step_trainer_trusted s 
inner join accelerometer_trusted a 
on s.sensorreadingtime = a.timestamp;

"""
CuratedJoin_node1698318050206 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometertrusted_node1698316955288,
        "step_trainer_trusted": steptrainertrusted_node1698316708967,
    },
    transformation_ctx="CuratedJoin_node1698318050206",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1698317712360 = glueContext.getSink(
    path="s3://ucityglue/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1698317712360",
)
MachineLearningCurated_node1698317712360.setCatalogInfo(
    catalogDatabase="stedi_human_balance_analytics",
    catalogTableName="machine_learning_curated",
)
MachineLearningCurated_node1698317712360.setFormat("json")
MachineLearningCurated_node1698317712360.writeFrame(CuratedJoin_node1698318050206)
job.commit()
