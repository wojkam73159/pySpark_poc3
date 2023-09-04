import unittest

from chispa import assert_column_equality, assert_df_equality

from pyspark.sql import SparkSession
from poc3 import rename_columns,filter_countries





class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.spark = (SparkSession.builder
                        .master("local")
                        .appName("chispa")
                        .getOrCreate())()

    @classmethod
    def tearDownClass(self):
        self.spark.stop()


    def test_rename_columns_correct_names(self):
        old_names=["cc_t","cc_n","cc_mc","a","ac_t"]
        new_names=["id","credit_card_type","credit_card_number","credit_card_currency","active","account_type"]
        data = [
            (1,"diners-club-enroute",201785930813822,"CUP",False,"XS"),
            (2,"china-unionpay",5602226300701090920,"EUR",False,"2XL"),
            (3,"jcb",3543626440463933,"IDR",True,"3XL"),
            (4,"mastercard",5002359260942096,"PEN",False,"L")
        ]
        df_tested = self.spark.createDataFrame(data, old_names)
        df_tested = rename_columns(data,new_names)

        df_expected = self.spark.createDataFrame(data, new_names)
        
        assert_df_equality(df_tested, df_expected)


        ###############################