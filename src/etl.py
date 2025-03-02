from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col, sum as spark_sum, rank, when
from pyspark.sql.window import Window

class PremierLeagueETL:
    def __init__(self, input_path: str, output_path: str):
        """
        Constructor of the ETL
        :param input_path: Path to the input JSON files
        :param output_path: Path where the Parquet files will be saved
        """
        print("Initializing Spark Session...")
        self.spark = SparkSession.builder.appName("EPL_ETL").getOrCreate()
        self.input_path = input_path
        self.output_path = output_path
        print(f"Input Path: {self.input_path}")
        print(f"Output Path: {self.output_path}")

    def extract(self):
        """Loads all JSON files in the directory and extracts the season number."""
        json_path = f"{self.input_path}/season-*.json"
        print(f"Attempting to read JSON files from: {json_path}")
        
        try:
            df = self.spark.read.json(json_path)
            print("JSON files successfully loaded.")
            print("Schema before adding season:")
            df.printSchema()
        except Exception as e:
            print(f"Error loading JSON files: {e}")
            return None

        print("Adding season column based on file path...")
        try:
            df = df.withColumn(
                "season",
                regexp_extract(input_file_name(), r"season-(\d{2}\d{2})", 1)
            )
            print("Season column successfully added.")
        except Exception as e:
            print(f"Error adding season column: {e}")
            return None

        print("Schema after adding season:")
        df.printSchema()
        
        return df

    def transform_positions(self, df):
        """Transformation to obtain the league standings by season."""
        print("Transforming data to get league standings by season...")
        
        df = df.withColumn("Points_Home", when(col("FTHG") > col("FTAG"), 3)
                            .when(col("FTHG") == col("FTAG"), 1)
                            .otherwise(0))
        df = df.withColumn("Points_Away", when(col("FTAG") > col("FTHG"), 3)
                            .when(col("FTHG") == col("FTAG"), 1)
                            .otherwise(0))

        positions_df = df.groupBy("season", "HomeTeam").agg(
            spark_sum("Points_Home").alias("Points"),
            spark_sum("FTHG").alias("Goals_Scored"),
            spark_sum("FTAG").alias("Goals_Conceded")
        ).withColumnRenamed("HomeTeam", "Team") \
        .union(df.groupBy("season", "AwayTeam").agg(
            spark_sum("Points_Away").alias("Points"),
            spark_sum("FTAG").alias("Goals_Scored"),
            spark_sum("FTHG").alias("Goals_Conceded")
        ).withColumnRenamed("AwayTeam", "Team"))

        positions_df = positions_df.groupBy("season", "Team").agg(
            spark_sum("Points").alias("Points"),
            spark_sum("Goals_Scored").alias("Goals_Scored"),
            spark_sum("Goals_Conceded").alias("Goals_Conceded")
        )

        positions_df = positions_df.withColumn("Goal_Difference", col("Goals_Scored") - col("Goals_Conceded"))

        window_spec = Window.partitionBy("season").orderBy(
            col("Points").desc(), col("Goal_Difference").desc(), col("Goals_Scored").desc()
        )
        positions_df = positions_df.withColumn("Rank", rank().over(window_spec))
        
        print("Saving Positions DataFrame to Parquet...")
        positions_df.write.mode("overwrite").partitionBy("season").parquet(f"{self.output_path}/positions")
        print("Positions DataFrame saved successfully.")
        
        return positions_df

    def transform_best_scoring_team(self, df):
        """Transformation to obtain the highest scoring team by season."""
        print("Transforming data to get the best scoring team per season...")
        
        scoring_df = df.groupBy("season", "HomeTeam").agg(
            spark_sum("FTHG").alias("Home_Goals")
        ).withColumnRenamed("HomeTeam", "Team") \
        .union(df.groupBy("season", "AwayTeam").agg(
            spark_sum("FTAG").alias("Away_Goals")
        ).withColumnRenamed("AwayTeam", "Team"))

        scoring_df = scoring_df.groupBy("season", "Team").agg(
            spark_sum("Home_Goals").alias("Total_Goals")
        )
        
        window_spec = Window.partitionBy("season").orderBy(col("Total_Goals").desc())
        best_scoring_df = scoring_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") == 1).drop("rank")
        
        print("Saving Best Scoring Team DataFrame to Parquet...")
        best_scoring_df.write.mode("overwrite").partitionBy("season").parquet(f"{self.output_path}/best_scoring_team")
        print("Best Scoring Team DataFrame saved successfully.")
        
        return best_scoring_df

    def run(self):
        """Executes the full ETL pipeline."""
        print("Starting ETL process...")
        df = self.extract()
        if df is not None:
            print("Extracted Data Sample:")
            df.show(10)  # Show first 10 rows for validation
            
            positions_df = self.transform_positions(df)
            best_scoring_df = self.transform_best_scoring_team(df)
        else:
            print("Extraction failed, no data to transform.")

if __name__ == "__main__":
    etl = PremierLeagueETL(input_path="/home/mbayonal/Documents/crisil_assignment/data/input", 
                            output_path="/home/mbayonal/Documents/crisil_assignment/data/output")
    etl.run()
