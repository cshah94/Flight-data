import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, DecimalType, LongType, DateType
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, udf, unix_timestamp
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# spark.sparkContext.setLogLevel('WARN')
# sc = spark.sparkContext

# add more functions as necessary



# In[15]:
def main(inputs):
    air_schema = StructType([
        StructField("Year", IntegerType()),
        StructField("Month", IntegerType()),
        StructField("DayofMonth", IntegerType()),
        StructField("DayOfWeek", IntegerType()),
        StructField("FlightDate", DateType()),
        StructField("UniqueCarrier", StringType()),
        StructField("TailNum", StringType()),
        StructField("FlightNum", IntegerType()),
        StructField("OriginAirportID", IntegerType()),
        StructField("OriginCityMarketID", IntegerType()),
        StructField("Origin", StringType()),
        StructField("OriginCityName", StringType()),
        StructField("OriginState", StringType()),
        StructField("OriginStateName", StringType()),
        StructField("OriginWac", IntegerType()),
        StructField("DestAirportID", IntegerType()),
        StructField("DestCityMarketID", IntegerType()),
        StructField("Dest", StringType()),
        StructField("DestCityName", StringType()),
        StructField("DestState", StringType()),
        StructField("DestStateName", StringType()),
        StructField("DestWac", IntegerType()),
        StructField("CRSDepTime", StringType()),
        StructField("DepTime", StringType()),
        StructField("DepDelay", DoubleType()),
        StructField("DepDel15", DoubleType()),
        StructField("TaxiOut", DoubleType()),
        StructField("WheelsOff", StringType()),
        StructField("WheelsOn", StringType()),
        StructField("TaxiIn", DoubleType()),
        StructField("CRSArrTime", StringType()),
        StructField("ArrTime", StringType()),
        StructField("ArrDelay", DoubleType()),
        StructField("ArrDel15", DoubleType()),
        StructField("Cancelled", DoubleType()),
        StructField("CancellationCode", StringType()),
        StructField("Diverted", DoubleType()),
        StructField("CRSElapsedTime", DoubleType()),
        StructField("ActualElapsedTime", DoubleType()),
        StructField("AirTime", DoubleType()),
        StructField("Flights", DoubleType()),
        StructField("Distance", DoubleType()),
        StructField("DistanceGroup", IntegerType()),
        StructField("CarrierDelay", DoubleType()),
        StructField("WeatherDelay", DoubleType()),
        StructField("NASDelay", DoubleType()),
        StructField("SecurityDelay", DoubleType()),
        StructField("LateAircraftDelay", DoubleType()),
        StructField("FirstDepTime", StringType()),
        StructField("DivAirportLandings", StringType()),
        StructField("DivReachedDest", StringType()),
        StructField("DivActualElapsedTime", StringType()),
        StructField("DivArrDelay", StringType()),
        StructField("DivDistance", StringType()),
    ])




flight_data = spark.read.csv(inputs, schema = air_schema)
flight_data.printSchema()


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
