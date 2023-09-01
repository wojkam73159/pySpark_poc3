from pyspark.sql import SparkSession
from pyspark import SparkContext
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
from pyspark.sql.functions import aggregate


warehouse_location = r"warehouse/"
clients_csv_path = r"poc3_dataset/clients.csv"
financial_csv_path = r"poc3_dataset/clients.csv"

# Create a Spark session
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

database_path = spark.conf.get("spark.sql.warehouse.dir")
print("Current database path:", database_path)

##
# id,first_name,last_name,email,gender,country,phone,birthdate
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
)

##

# id,cc_t,cc_n,cc_mc,a,ac_t
#
# credit's card type
# credit's card number
# credit's card main currency
# active flag
# account type
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
)
