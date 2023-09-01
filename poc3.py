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

#countries_list=["Poland","France"]
#.*POLAND.*|.*FRANCE.*
#df_col_name="country"
def filter_countries(to_filterDF:DataFrame, countries_list:list[str],df_col_name):
    countries_list=map(lambda x : ".*"+x.upper()+".*",countries_list)
    countries_regex="|".join(countries_list)
    return to_filterDF.filter(upper(to_filterDF[df_col_name]).rlike(countries_regex))



# Create a Spark session

#database_path = spark.conf.get("spark.sql.warehouse.dir")
#print("Current database path:", database_path)

##
# id,first_name,last_name,email,gender,country,phone,birthdate


##

# id,cc_t,cc_n,cc_mc,a,ac_t
#
# credit's card type
# credit's card number
# credit's card main currency
# active flag
# account type


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")

    # Add the command-line arguments
    parser.add_argument("--dataset1", type=str, help="First argument")
    parser.add_argument("--dataset2", type=str, help="Second argument")
    parser.add_argument("--list_arg", nargs="+", type=str, help="List of strings")

    # Parse the command-line arguments
    args = parser.parse_args()
        
    warehouse_location = r"warehouse/"
    clients_csv_path = args.dataset1
    #r"poc3_dataset/clients.csv"
    financial_csv_path = args.dataset2
    #r"poc3_dataset/clients.csv"
    countries=args.list_arg

    spark = (
    SparkSession.builder.appName("PySpark_SQL_w_Hive")
    .config("spark.sql.warehouse.dir", warehouse_location)
    .config(
        "javax.jdo.option.ConnectionURL",
        f"jdbc:derby:{warehouse_location}/metastore_db;create=true",
    )
    .enableHiveSupport()
    .getOrCreate()
    )
    
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
    .select("id", "email", "country")
    #.filter(
    #    like(upper(col("country")), "%POLAND%")
    #    or like(upper(col("country")), "%FRANCE%")
    #)
    )

clientsDF=filter_countries(clientsDF,countries,"country")

finance_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("cc_t", StringType(), True),
        StructField("cc_n", IntegerType(), True),
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
    .selectExpr(
        "id as id",
        "cc_t as credit_card_type",
        "cc_n as credit_card_number",
        "cc_mc as credit_card_currency",
        "a as active",
        "ac_t as account type",
    )
)

joinedDF = clientsDF.join(financeDF, clientsDF.id == financeDF.id)
joinedDF.show()

#joinedDF.write.format("csv")


