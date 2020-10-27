import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

def create_spark_session():
    """
    This function instantiate the spark module that we are going to use.

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_staged_data(spark, input_data, output_data):
    """
    This function extracts and process the data from the S3 bucket from udacity 
	to create the dimensional and fact tables and insert them in the new
	S3 bucket.

    INPUTS:
    * spark: the spark module
    * input_data: the path to the S3 bucket that will be extracted and processed.
	+ output_data: the path to the new S3 bucket in which we will insert the tables.
    """
    # get filepath to stock data file
    Staging_stock_table = os.path.join(input_data, "Staging_stock_table.csv")
    
    # read song data file
    stock_df = spark.read.options(inferSchema = True, header= True, delimiter=";").csv(Staging_stock_table)
    
    # get filepath to stringency data file
    Staging_stringency_table = os.path.join(input_data, "Staging_stringency_table.csv")

    # read stringency data file
    stringency_df = spark.read.options(inferSchema = True, header= True, delimiter=";").csv(Staging_stringency_table)
    
    
    # extract columns to create country table
    country_table = stringency_df.selectExpr('Code as country_code','Country as country').dropDuplicates()
    
    # write country table to parquet files
    country_table.write.parquet(os.path.join(output_data, "countries"), mode = "overwrite")

    # extract columns to create stock table
    stock_table = stock_df.selectExpr('Ticker as stock_id','names as company_name','Sector as sector').dropDuplicates()
    
    # write artists table to parquet files
    stock_table.write.parquet(os.path.join(output_data, "stocks"), mode = "overwrite")
        
    # create time_table
    time_table = stringency_df.select(['Date']).withColumn('day', dayofmonth('Date')).withColumn('month', month('Date')).withColumn('year', year('Date')).withColumn('weekday', date_format('Date', 'E')).dropDuplicates()
    time_table.write.parquet(os.path.join(output_data, "times"), mode = "overwrite")
    
    stock_df.createOrReplaceTempView("stocks")
    stringency_df.createOrReplaceTempView("stringency")
    Ec_status_table = spark.sql(
    '''
    SELECT DISTINCT monotonically_increasing_id() as ec_status_id, stringency.Date as date, stringency.Code as country_code, stringency.Stringency_Index as stringency_index, stocks.Ticker as stock_id, stocks.Value_Type as value_type, stocks.Value as value
    FROM stocks
    JOIN stringency 
    ON stocks.Date = stringency.Date AND stocks.Country = stringency.Country
    '''
    )
    Ec_status_table.write.parquet(os.path.join(output_data, "Ec_status"), mode = "overwrite")




def main():
    
    config = configparser.ConfigParser()
    config.read('Cp.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

    spark = create_spark_session()
    input_data = "s3a://capstone-project-udaz/"
    output_data = "s3a://capstone-project-udaz/Output_Data/"

    process_staged_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
