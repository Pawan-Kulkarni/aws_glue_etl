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

# Script generated for node step trainer landing
steptrainerlanding_node1698315337337 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1698315337337",
)

# Script generated for node customer curated
customercurated_node1698315397420 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance_analytics",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1698315397420",
)

# Script generated for node SQL Query
SqlQuery0 = """
select s.* from customer_curated c 
INNER JOIN step_trainer_landing s on 
c.serialnumber = s.serialnumber;



"""
SQLQuery_node1698315449109 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_curated": customercurated_node1698315397420,
        "step_trainer_landing": steptrainerlanding_node1698315337337,
    },
    transformation_ctx="SQLQuery_node1698315449109",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1698316320896 = glueContext.getSink(
    path="s3://ucityglue/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="steptrainertrusted_node1698316320896",
)
steptrainertrusted_node1698316320896.setCatalogInfo(
    catalogDatabase="stedi_human_balance_analytics",
    catalogTableName="step_trainer_trusted",
)
steptrainertrusted_node1698316320896.setFormat("json")
steptrainertrusted_node1698316320896.writeFrame(SQLQuery_node1698315449109)
job.commit()
