using Arrow
using CSV
using Parquet

# Download data files from the Internet and upload to S3. Convert to Parquet and Arrow formats

# iris.csv
download("https://raw.githubusercontent.com/Sketchjar/MachineLearningHD/main/iris.csv")

# yellow_tripdata_2016-01.csv
download("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv")

# flights.csv
