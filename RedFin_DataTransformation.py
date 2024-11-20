from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("PropertyDataPipeline").getOrCreate()

def process_property_data():
    # Input and output paths in S3
    raw_data_s3_path = "s3://property-data-bucket/raw/city_market_tracker.tsv000.gz"
    transformed_data_s3_path = "s3://property-data-bucket/processed/property_data.parquet"
    
    # Read raw data from S3
    raw_data = spark.read.csv(raw_data_s3_path, header=True, inferSchema=True, sep="\t")
    
    # Select and clean necessary columns
    cleaned_data = raw_data.select(
        ['period_end', 'period_duration', 'city', 'state', 'property_type',
         'median_sale_price', 'median_ppsf', 'homes_sold', 'inventory', 
         'months_of_supply', 'median_dom', 'sold_above_list', 'last_updated']
    ).dropna()
    
    # Add year and month columns
    cleaned_data = cleaned_data \
        .withColumn("year", year(col("period_end"))) \
        .withColumn("month", month(col("period_end"))) \
        .drop("period_end", "last_updated")
    
    # Map numeric months to month names using a dictionary
    month_mapping = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    # Use `create_map` to map months to names
    month_mapping_expr = create_map([lit(k).cast("int"), lit(v) for k, v in month_mapping.items()])
    cleaned_data = cleaned_data.withColumn("month_name", month_mapping_expr[col("month")]).drop("month")
    
    # Write transformed data back to S3 in Parquet format
    cleaned_data.write.mode("overwrite").parquet(transformed_data_s3_path)

# Execute the transformation function
process_property_data()
