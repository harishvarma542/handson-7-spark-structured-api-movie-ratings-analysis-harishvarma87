from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Trend_Analysis"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def analyze_movie_watching_trends(df):
    """
    Analyze trends in movie watching over the years.
    """
    # Group by WatchedYear and count the number of movies watched
    trend_df = df.groupBy("WatchedYear").agg(count("MovieID").alias("MoviesWatched"))

    # Order by WatchedYear to identify trends
    trend_df = trend_df.orderBy("WatchedYear")

    return trend_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a single CSV file using pandas.
    """
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = result_df.toPandas()
    # Write to CSV
    pandas_df.to_csv(output_path, index=False)

def main():
    """
    Main function to execute Task 3.
    """
    spark = initialize_spark()

    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-harishvarma87/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-harishvarma87/Outputs/movie_watching_trends.csv"

    df = load_data(spark, input_file)
    result_df = analyze_movie_watching_trends(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()