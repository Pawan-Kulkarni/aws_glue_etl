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

# Script generated for node customer trusted
customertrusted_node1698224749347 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1698224749347",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1698224724566 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1698224724566",
)

# Script generated for node Privacy Join
PrivacyJoin_node1698225272257 = Join.apply(
    frame1=customertrusted_node1698224749347,
    frame2=accelerometerlanding_node1698224724566,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyJoin_node1698225272257",
)

# Script generated for node Drop fields
SqlQuery0 = """
select user,timestamp,x,y,z from myDataSource;
"""
Dropfields_node1698225609719 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": PrivacyJoin_node1698225272257},
    transformation_ctx="Dropfields_node1698225609719",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1698225717703 = glueContext.getSink(
    path="s3://ucityglue/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Accelerometertrusted_node1698225717703",
)
Accelerometertrusted_node1698225717703.setCatalogInfo(
    catalogDatabase="stedi_human_balance_analytics",
    catalogTableName="accelerometer_trusted",
)
Accelerometertrusted_node1698225717703.setFormat("json")
Accelerometertrusted_node1698225717703.writeFrame(Dropfields_node1698225609719)
job.commit()
