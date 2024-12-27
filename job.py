import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import (
    col, to_date, datediff, avg, sum, round, when, 
    date_format, current_date, format_number
)

# Initialize contexts and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'file_name'])
job.init(args['JOB_NAME'])

try:
    # Schema definition with precise decimal handling
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Product_Category", StringType(), True),
        StructField("Sales_Volume", IntegerType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Promotion", IntegerType(), True),
        StructField("Store_Location", StringType(), True),
        StructField("Weekday", IntegerType(), True),
        StructField("Supplier_Cost", DoubleType(), True),
        StructField("Replenishment_Lead_Time", DoubleType(), True),
        StructField("Stock_Level", IntegerType(), True)
    ])

    # Read CSV
    INPUT_PATH = f"s3://warehouse-data-lake/raw/inventory/{args['file_name']}"
    print(f"Reading from: {INPUT_PATH}")
    
    df = spark.read \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .option("delimiter", ",") \
        .schema(schema) \
        .csv(INPUT_PATH)

    # Convert date
    df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

    # Calculate basic KPIs with precise decimal handling
    enriched_df = df.withColumn(
        # Revenue calculation
        "Revenue", round(col("Sales_Volume") * col("Price"), 2)
    ).withColumn(
        # Cost calculation
        "Cost", round(col("Sales_Volume") * col("Supplier_Cost"), 2)
    ).withColumn(
        # Profit calculation
        "Profit", round(col("Revenue") - col("Cost"), 2)
    ).withColumn(
        # Profit Margin percentage
        "Profit_Margin", round((col("Profit") / col("Revenue")) * 100, 2)
    ).withColumn(
        # Time-based information
        "Year_Month", date_format(col("Date"), "yyyy-MM")
    ).withColumn(
        # Simple replenishment timing
        "Days_To_Replenish", col("Replenishment_Lead_Time") + 1
    ).withColumn(
        # Stock Status based on current stock vs daily sales and lead time
        "Stock_Status", 
        when(col("Stock_Level") <= col("Sales_Volume") * col("Replenishment_Lead_Time"), "Low")
        .when(col("Stock_Level") <= col("Sales_Volume") * (col("Replenishment_Lead_Time") * 2), "Medium")
        .otherwise("High")
    )

    # Modify schema to ensure Redshift compatibility
    from pyspark.sql.types import DecimalType
    
    # Convert double columns to decimal with 2 decimal places
    double_columns = [
        "Price", "Supplier_Cost", "Revenue", "Cost", 
        "Profit", "Profit_Margin"
    ]
    
    for column in double_columns:
        enriched_df = enriched_df.withColumn(
            column, 
            col(column).cast(DecimalType(10, 2))
        )

    # Convert to dynamic frame
    dynamic_frame = DynamicFrame.fromDF(enriched_df, glueContext, "nested")

    # Write to Redshift with additional options to prevent duplicate columns
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="Redshift connection",
        connection_options={
            "dbtable": "public.wh_inventory_analytics",
            "database": "dev"
        },
        redshift_tmp_dir = args["TempDir"]
    )

    job.commit()
    print("Job completed successfully!")

except Exception as e:
    print(f"Error in job execution: {str(e)}")
    raise e
