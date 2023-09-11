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
    FloatType,
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
def filter_col_for_strings(to_filterDF: DataFrame, strs_to_check: list[str], df_col_name):
    strs_to_check = map(lambda x: ".*" + x.upper() + ".*", strs_to_check)
    countries_regex = "|".join(strs_to_check)
    return to_filterDF.filter(upper(to_filterDF[df_col_name]).rlike(countries_regex))


def rename_columns(DF_to_rename: DataFrame, new_names: list[str]):
    old_names = DF_to_rename.schema.names
    if len(old_names) != len(new_names):
        logger.error("rename columns:names list different len than datafram columns ")
        raise Exception("names list different len than datafram columns")

    resDF = reduce(
        lambda DF, idx: DF.withColumnRenamed(old_names[idx], new_names[idx]),
        range(len(old_names)),
        DF_to_rename,
    )
    return resDF


def get_logger(self, spark: SparkSession, my_logger_name: str = ""):
    log4j_logger = spark._jvm.org.apache.log4j
    return log4j_logger.LogManager.getLogger(my_logger_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")

    parser.add_argument("--clients_csv", type=str, help="path to dataset with clients csv")
    parser.add_argument("--financial_csv", type=str, help="path to dataset with financial csv")
    parser.add_argument("--list_countries", nargs="+", type=str, help="List of countries to filter")

    args = parser.parse_args()

    clients_csv_path = args.clients_csv
    financial_csv_path = args.financial_csv
    countries = args.list_countries

    log_file_path = "/home/wojkamin/my_folder/poc3_folder/logs/out.log"
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

    clientsDF = filter_col_for_strings(clientsDF, countries, "country")

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
