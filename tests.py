import unittest

from chispa import  assert_df_equality

from pyspark.sql import SparkSession
from poc3 import rename_columns, filter_col_for_strings


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.spark = SparkSession.builder.appName("chispa").getOrCreate()
        # .master("local")

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

    def test_rename_columns_correct_names(self):
        old_names = ["cc_t", "cc_n", "cc_mc", "a", "ac_t"]
        new_names = [
            "id",
            "credit_card_type",
            "credit_card_number",
            "credit_card_currency",
            "active",
            "account_type",
        ]
        data = [
            (1, "diners-club-enroute", 201785930813822, "CUP", False, "XS"),
            (2, "china-unionpay", 5602226300701090920, "EUR", False, "2XL"),
            (3, "jcb", 3543626440463933, "IDR", True, "3XL"),
            (4, "mastercard", 5002359260942096, "PEN", False, "L"),
        ]
        df_tested = self.spark.createDataFrame(data, old_names)
        df_tested = rename_columns(df_tested, new_names)

        columns_list = df_tested.schema
        print(columns_list)

        new_names = [
            "id",
            "credit_card_type",
            "credit_card_number",
            "credit_card_currency",
            "active",
            "account_type",
        ]
        df_expected = self.spark.createDataFrame(data, new_names)

        assert_df_equality(df_tested, df_expected)

    def test_filter_col_for_strings_catches_longer_names(self):
        columns = ["id", "country"]
        data = [
            (1, "pol"),
            (2, "POL"),
            (3, "pOl"),
            (4, "poland"),
            (5, "polonie"),
            (6, "polska"),
            (7, "Polska"),
            (8, "POLINEZJA!!!"),
        ]
        df_tested = self.spark.createDataFrame(data, columns)
        df_expected = df_tested.select(columns)
        str_to_filter_for = ["pol"]
        df_tested = filter_col_for_strings(df_tested, str_to_filter_for, "country")
        assert_df_equality(df_tested, df_expected)

    def test_filter_col_for_strings_omits_names_too_short(self):
        columns = ["id", "country"]
        data = [
            (1, "pol"),
            (2, "POL"),
            (3, "pOl"),
            (4, "poland"),
            (5, "polonie"),
            (6, "polska"),
            (7, "Polska"),
            (8, "POLINEZJA!!!"),
        ]
        str_to_filter_for = ["pols"]
        df_tested = self.spark.createDataFrame(data, columns)
        data_expected = [
            (6, "polska"),
            (7, "Polska"),
        ]
        df_expected = self.spark.createDataFrame(data_expected, columns)

        df_tested = filter_col_for_strings(df_tested, str_to_filter_for, "country")
        assert_df_equality(df_tested, df_expected)

    def test_filter_col_for_strings_matches_multiple_elements(self):
        columns = ["id", "country"]
        data = [
            (1, "pol"),
            (2, "fra"),
            (3, "ger"),
            (4, "poland,france,germany"),
            (5, "germany france"),
            (6, "POLINESIA,FRANKONIA"),
            (7, "america"),
        ]
        str_to_filter_for = ["pol", "fra", "ger"]
        df_tested = self.spark.createDataFrame(data, columns)
        data_expected = [
            (1, "pol"),
            (2, "fra"),
            (3, "ger"),
            (4, "poland,france,germany"),
            (5, "germany france"),
            (6, "POLINESIA,FRANKONIA"),
        ]
        df_expected = self.spark.createDataFrame(data_expected, columns)

        df_tested = filter_col_for_strings(df_tested, str_to_filter_for, "country")
        assert_df_equality(df_tested, df_expected)


if __name__ == "__main__":
    unittest.main()
