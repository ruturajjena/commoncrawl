import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
import gs_repartition
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
 
# Script generated for node Custom Transform
def ModifyCol(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import split, col
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df2= df.withColumn('Crawl',split(col('warc_filename'), '/').getItem(1))
    dyf_filtered = DynamicFrame.fromDF(df2, glueContext, "warc_filename")
    return(DynamicFrameCollection({"CustomTransform0": dyf_filtered}, glueContext))
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""
 
# Script generated for node Amazon S3
AmazonS3_node1750172165143 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2025-21/"], "recurse": True}, transformation_ctx="AmazonS3_node1750172165143")
 
# Script generated for node Change Schema
ChangeSchema_node1750172297828 = ApplyMapping.apply(frame=AmazonS3_node1750172165143, mappings=[("url_host_name", "string", "url_host_name", "string"), ("fetch_status", "smallint", "fetch_status", "short"), ("warc_filename", "string", "warc_filename", "string")], transformation_ctx="ChangeSchema_node1750172297828")
 
# Script generated for node Custom Transform
CustomTransform_node1750172714602 = ModifyCol(glueContext, DynamicFrameCollection({"ChangeSchema_node1750172297828": ChangeSchema_node1750172297828}, glueContext))
 
# Script generated for node Select From Collection
SelectFromCollection_node1750173033783 = SelectFromCollection.apply(dfc=CustomTransform_node1750172714602, key=list(CustomTransform_node1750172714602.keys())[0], transformation_ctx="SelectFromCollection_node1750173033783")
 
# Script generated for node Change Schema
ChangeSchema_node1750174386923 = ApplyMapping.apply(frame=SelectFromCollection_node1750173033783, mappings=[("url_host_name", "string", "url_host_name", "string"), ("Crawl", "string", "Crawl", "string")], transformation_ctx="ChangeSchema_node1750174386923")
 
# Script generated for node Drop Duplicates
DropDuplicates_node1750172461975 =  DynamicFrame.fromDF(ChangeSchema_node1750174386923.toDF().dropDuplicates(["url_host_name"]), glueContext, "DropDuplicates_node1750172461975")
 
# Script generated for node Autobalance Processing
AutobalanceProcessing_node1750176246630 = DropDuplicates_node1750172461975.gs_repartition(numPartitionsStr="60")
 
# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AutobalanceProcessing_node1750176246630, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750172153751", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1750174452014 = glueContext.write_dynamic_frame.from_options(frame=AutobalanceProcessing_node1750176246630, connection_type="s3", format="csv", connection_options={"path": "s3://<yourbucketpath>", "partitionKeys": []}, transformation_ctx="AmazonS3_node1750174452014")
 
job.commit()