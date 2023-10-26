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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1698230632458 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1698230632458",
)

# Script generated for node customer_trusted
customer_trusted_node1698230628749 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1698230628749",
)

# Script generated for node Trusted Join
TrustedJoin_node1698230675052 = Join.apply(
    frame1=customer_trusted_node1698230628749,
    frame2=accelerometer_trusted_node1698230632458,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="TrustedJoin_node1698230675052",
)

# Script generated for node Drop Fields and duplicates
SqlQuery0 = """
select distinct customername,email,phone,birthday,serialnumber,registrationdate,lastupdatedate,sharewithresearchasofdate,sharewithpublicasofdate,sharewithfriendsasofdate from myDataSource;
"""
DropFieldsandduplicates_node1698230700292 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": TrustedJoin_node1698230675052},
    transformation_ctx="DropFieldsandduplicates_node1698230700292",
)

# Script generated for node customer_curated
customer_curated_node1698230893744 = glueContext.getSink(
    path="s3://ucityglue/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1698230893744",
)
customer_curated_node1698230893744.setCatalogInfo(
    catalogDatabase="stedi_human_balance_analytics", catalogTableName="customer_curated"
)
customer_curated_node1698230893744.setFormat("json")
customer_curated_node1698230893744.writeFrame(DropFieldsandduplicates_node1698230700292)
job.commit()
