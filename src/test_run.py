from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.config("spark.app.name", 'test').getOrCreate()


base = spark.read.csv('/Users/emmanuel/jrl_data/base_data.csv',header=True,inferSchema=True)

options = spark.read.csv('/Users/emmanuel/jrl_data/options_data.csv',header=True,inferSchema=True)



# Create temporary views for DataFrames
base.createOrReplaceTempView("base")
options.createOrReplaceTempView("options")

# Perform the join using Spark SQL
df_join = spark.sql("""
        SELECT *,case
                    when sales_price <= 0 then 0
                    when  b.model_text = o.model AND b.options_code = o.option_code then o.material_cost
                    when  b.model_text != o.model AND b.options_code != o.option_code then AVG(o.material_cost) over(Partition by o.model,o.option_code)
                    when b.options_code!=o.option_code then 0.45 * b.sales_price                     
                    END AS production_cost,(sales_price - production_cost) AS profit
        FROM base b 
        LEFT JOIN options o
        ON b.model_text = o.model
        AND b.options_code = o.option_code

                    order by production_cost desc
         """).show()





# Perform the join using Spark SQL
df_join = spark.sql("""
    SELECT *,
        CASE
            WHEN sales_price <= 0 THEN 0
            WHEN b.model_text = o.model AND b.options_code = o.option_code THEN o.material_cost
            WHEN b.model_text != o.model AND b.options_code != o.option_code THEN AVG(o.material_cost) OVER (PARTITION BY o.option_code)
            WHEN b.options_code != o.option_code THEN 0.45 * b.sales_price
        END AS production_cost
    FROM base b
    LEFT JOIN options o
    ON b.model_text = o.model AND b.options_code = o.option_code
    ORDER BY production_cost DESC
""").show()
