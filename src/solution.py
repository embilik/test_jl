from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class Solution:
    def create_spark_session(self, appName: str = "analysis") -> SparkSession:
        """
        :param appName: Spark application name, defaults to `analysis`
        :return: SparkSession
        """
        spark = SparkSession.builder.config("spark.app.name", appName).getOrCreate()
        return spark
    

    def read_dataset(self, spark: SparkSession, path: str) -> DataFrame:
        """
        Reads in the base dataset
        :param spark: SparkSessioin
        :param path: input file path
        :return: Spark DataFrame
        """
        df = spark.read.csv(path,header=True,inferSchema=True)
        return df
    
    def remove_duplicate_records(self, df: DataFrame) -> DataFrame:
        """
        Removes duplicated records in datasest
        :param df: Spark DataFrame
        :return: Spark DataFrame
        """
        df = df.dropDuplicates()
        return df
    
    def join_datasets(self,df: DataFrame) -> DataFrame:
        """
        Join base_data and options_data datasets
        :param df: Spark DataFrame
        :return: Spark DataFrame
        """
        base = df["base"]
        options = df["options"]
    
        # Create temporary views for DataFrames
        base.createOrReplaceTempView("base")
        options.createOrReplaceTempView("options")

        # Perform the join using Spark SQL
        df_join = self.spark.sql("""
        SELECT b.*, o.material_cost
        FROM base b
        LEFT JOIN options o
        ON b.model_text = o.model
        AND b.options_code = o.option_code
    """)
        return df_join

    def calculate_production_cost(self, df: DataFrame) -> DataFrame:
        """
        Calculate production cost based on specified logic using Spark SQL
        :param df: Spark DataFrame with joined data
        :return: Spark DataFrame with production cost column
        """
        df.createOrReplaceTempView("df_join")

        df_with_production_cost =self.spark.sql("""
            SELECT *,
                CASE
                    WHEN sales_price <= 0 THEN 0
                    WHEN  b.model_text = o.model AND b.options_code = o.option_code Then o.material_cost
                    WHEN  b.model_text != o.model AND b.options_code != o.option_code THEN AVG(o.material_cost) OVER(PARTITION BY o.model,o.option_code)
                    WHEN b.options_code!=o.option_code THEN 0.45 * b.sales_price 
                END AS production_cost
            FROM joined_data
        """)

        return df_with_production_cost
    
    def calculate_profit(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit per option
        :param df: Spark DataFrame with joined data
        :return: Spark DataFrame with profit column
        """
        df.createOrReplaceTempView("data")  # Create a temporary view for the DataFrame
        df_with_profit = self.spark.sql("""
            SELECT *, (sales_price - production_cost) AS profit
            FROM data
        """)
        return df_with_profit







