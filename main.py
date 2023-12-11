from pyspark.sql import SparkSession
from src.solution import Solution
from src.utils import (extract_source_data,
                       save_to_sink,
                       read_job_config,
                       parse_known_cmd_args,
                        )


if __name__ =='__main__':
    # Initialize Spark Session
    spark = SparkSession.builder.appName("JLRTask").getOrCreate()
    args = parse_known_cmd_args()
    config = read_job_config(args.config_file_name)



    # Extract
    df_dict = extract_source_data(spark, config["source"])

    # Transform
    # Transform
    solution = Solution()
    sol1 = solution.remove_duplicate_pings(df=df_dict["base"])
   
    
    # Load
