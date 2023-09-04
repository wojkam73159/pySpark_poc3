from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    LongType,
    StringType,
    StructType,
    IntegerType,
    StructField,
    TimestampType,
    BooleanType,
    FloatType
)
from pyspark.sql.functions import col, upper
from typing import List
import argparse
from functools import reduce
import logging
from pyspark.conf import SparkConf


# countries_list=["Poland","France"]
# .*POLAND.*|.*FRANCE.*
# df_col_name="country"
def filter_countries(to_filterDF: DataFrame, countries_list: list[str], df_col_name):
    countries_list = map(lambda x: ".*" + x.upper() + ".*", countries_list)
    countries_regex = "|".join(countries_list)
    return to_filterDF.filter(upper(to_filterDF[df_col_name]).rlike(countries_regex))

def rename_columns(DF_to_rename:DataFrame, new_names:list[str]):
    old_names=financeDF.schema.names
    if len(old_names)!= len(new_names):
        raise Exception("names list different len than datafram columns")

    resDF=reduce(lambda DF, idx : DF.withColumnRenamed(old_names[idx], new_names[idx]),range(len(old_names)),DF_to_rename)
    return resDF


def get_logger(self, spark: SparkSession, my_logger_name:str = ""):
    log4j_logger = spark._jvm.org.apache.log4j  
    return log4j_logger.LogManager.getLogger(my_logger_name )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")

    parser.add_argument("--dataset1", type=str, help="dataset1")
    parser.add_argument("--dataset2", type=str, help="dataset2")
    parser.add_argument("--list_countries", nargs="+", type=str, help="List of strings")

    args = parser.parse_args()
    warehouse_location = r"warehouse/"
    clients_csv_path = args.dataset1
    financial_csv_path = args.dataset2
    countries = args.list_countries

    log_file_path="/home/wojkamin/my_folder/poc3_folder/logs/out.log"
    spark_conf = SparkConf()\
        .set("Dlog4j.rootCategory", "INFO,FILE")\
        .set("spark.log4j.appender.FILE", "org.apache.log4j.FileAppender") \
        .set("spark.log4j.appender.FILE.File", log_file_path) \
        .set("spark.log4j.appender.FILE.layout", "org.apache.log4j.PatternLayout") \
        .set("spark.log4j.appender.FILE.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n") \
        .set("spark.log4j.logger.FILE", "INFO, FILE")
#.set("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=INFO,FILE")\
    

    spark = (
        SparkSession.builder.appName("PySpark_poc_w_Hive")
        .config(conf=spark_conf)
        .enableHiveSupport()
        .getOrCreate())
 
    logger = spark._jvm.org.apache.log4j.Logger.getLogger("PySpark_poc_w_Hive")
    logger.info("This is an informational message.")
    logger.warn("This is a warning message.")
    logger.error("This is an error message.")

    clients_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("country", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("birthdate", TimestampType(), True),
        ]
    )

clientsDF = (
    spark.read.option("sep", ",")
    .option("header", True)
    .schema(clients_schema)
    .csv(clients_csv_path)
    .select("id", "email", "country")#remove PII
)

clientsDF = filter_countries(clientsDF, countries, "country")

finance_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("cc_t", StringType(), True),
        StructField("cc_n", LongType(), True),
        StructField("cc_mc", StringType(), True),
        StructField("a", BooleanType(), True),
        StructField("ac_t", StringType(), True),
    ]
)

financeDF = (
    spark.read.option("sep", ",")
    .option("header", True)
    .schema(finance_schema)
    .csv(financial_csv_path)
    )

new_names=["id","credit_card_type","credit_card_number","credit_card_currency","active","account_type"]
financeDF=rename_columns(financeDF,new_names)
joinedDF = clientsDF.join(financeDF, ["id"])
joinedDF.show()

joinedDF.write.mode("overwrite").csv("client_data/")

# joinedDF.write.format("csv")
