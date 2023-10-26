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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1698137952894 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="customer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1698137952894",
)

# Script generated for node Change Schema
ChangeSchema_node1698148126680 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1698137952894,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "long"),
        ("birthday", "date", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "float", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "float", "sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "float", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="ChangeSchema_node1698148126680",
)

# Script generated for node Filter trusted customers
SqlQuery0 = """
select * from myDataSource where shareWithResearchAsOfDate is not null
"""
Filtertrustedcustomers_node1698138919594 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": ChangeSchema_node1698148126680},
    transformation_ctx="Filtertrustedcustomers_node1698138919594",
)

# Script generated for node Output
Output_node1698139014089 = glueContext.getSink(
    path="s3://ucityglue/customer_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Output_node1698139014089",
)
Output_node1698139014089.setCatalogInfo(
    catalogDatabase="stedi_human_balance_analytics", catalogTableName="customer_trusted"
)
Output_node1698139014089.setFormat("json")
Output_node1698139014089.writeFrame(Filtertrustedcustomers_node1698138919594)
job.commit()
