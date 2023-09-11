from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    LongType,
    StringType,
    StructType,
    IntegerType,
    StructField,
    TimestampType,
    BooleanType,
)
from pyspark.sql.functions import  upper
from typing import List
import argparse
from functools import reduce
from pyspark.conf import SparkConf

from poc3_package import rename_columns







if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")

    parser.add_argument("--dataset1", type=str, help="path to dataset with clients csv")
    parser.add_argument("--dataset2", type=str, help="path to dataset with financial csv")
    parser.add_argument("--list_countries", nargs="+", type=str, help="List of countries to filter")

    args = parser.parse_args()
    #warehouse_location = r"warehouse/"
    clients_csv_path = args.dataset1
    financial_csv_path = args.dataset2
    countries = args.list_countries

    spark_conf = (
        SparkConf()
        .set(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.configuration=file:/home/wojkamin/my_folder/poc3_folder/log4j.properties",
        )
        .set(
            "spark.executor.extraJavaOptions",
            "-Dlog4j.configuration=file:/home/wojkamin/my_folder/poc3_folder/log4j.properties",
        )
    )

    spark = (
        SparkSession.builder.appName("PySpark_poc_w_Hive")
        .config(conf=spark_conf)
        .getOrCreate()
    )

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
        .select("id", "email", "country")  # remove PII
    )

    clientsDF = filter_col_for_strings(clientsDF, countries, df_col_name="country")

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

    new_names = [
        "id",
        "credit_card_type",
        "credit_card_number",
        "credit_card_currency",
        "active",
        "account_type",
    ]
    financeDF = rename_columns(financeDF, new_names)
    joinedDF = clientsDF.join(financeDF, ["id"])
    joinedDF.show()

    joinedDF.write.mode("overwrite").csv("client_data/")

    # joinedDF.write.format("csv")
