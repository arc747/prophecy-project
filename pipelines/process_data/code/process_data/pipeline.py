from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from process_data.config.ConfigStore import *
from process_data.functions import *
from prophecy.utils import *
from process_data.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sales = sales(spark)
    df_reformatted_data_projection = reformatted_data_projection(spark, df_sales)
    df_filter_not_null_product_id = filter_not_null_product_id(spark, df_reformatted_data_projection)
    df_product = product(spark)
    df_filter_not_null_customer_id = filter_not_null_customer_id(spark, df_product)
    df_join_by_product_id = join_by_product_id(spark, df_filter_not_null_customer_id, df_filter_not_null_product_id)
    df_deduplicate_by_columns = deduplicate_by_columns(spark, df_join_by_product_id)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("process_data")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/process_data")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/process_data", config = Config)(pipeline)

if __name__ == "__main__":
    main()
