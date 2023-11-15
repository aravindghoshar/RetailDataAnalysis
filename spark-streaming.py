#Importing the required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Calculating is_return
def get_is_return(type):
	is_return=0
        if type=="RETURN":
            is_return = 1
	return is_return

# Calculating is_order
def get_is_order(type):
	is_order = 0
	if type=="ORDER":
	    is_order = 1
	return is_order 

# Calculating total cost of a single order
def get_total_cost(items,type):
	total_cost=0
	if items is not None:
    	    for item in items:
        	total_cost = total_cost + (item['quantity'] * item['unit_price'])
	    	if type=="ORDER":
			return total_cost
		else:
		    	return -total_cost   

# Calculating total number of items present in a single order
def get_total_item_count(items):
	total_count=0
	if items is not None:
    	    for item in items:
        	total_count = total_count + item['quantity']
		return total_count  

# Creating Spark Session
spark = SparkSession  \
		.builder  \
		.appName("StructuredSocketRead")  \
		.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
# Reading data from Kafka
lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","real-time-project")  \
    .option("failOnDataLoss","false") \
    .option("startingOffsets", "earliest")  \
	.load()


masterDF = lines.selectExpr("cast(key as string)","cast(value as string)")	


#Defining schema
customSchema = StructType([
                StructField("country", StringType()),
                StructField("invoice_no", LongType()) ,
                StructField("items", ArrayType(
                    StructType([
                        StructField("SKU", StringType()),
                        StructField("title", StringType()),
                        StructField("unit_price", FloatType()),
                        StructField("quantity", IntegerType())
                              ])
                        )),
                StructField("timestamp", TimestampType()),
                StructField("type", StringType()),  
            ])

orderStreamDF = masterDF.select(from_json(col("value"),customSchema).alias("data")).select("data.*")

# Define the UDFs.
add_total_item_count = udf(get_total_item_count, IntegerType())
add_total_cost = udf(get_total_cost, DoubleType())
add_is_order = udf(get_is_order,IntegerType())
add_is_return = udf(get_is_return,IntegerType())

# Calculating additional columns
expOrderStreamDF = orderStreamDF \
	.withColumn("total_items",add_total_item_count(orderStreamDF.items)) \
	.withColumn("total_cost",add_total_cost(orderStreamDF.items,orderStreamDF.type)) \
	.withColumn("is_order",add_is_order(orderStreamDF.type)) \
	.withColumn("is_return",add_is_return(orderStreamDF.type))

# Calculating time based KPIs
aggregateStreamByTime = expandedOrderStreamDF \
		.withWatermark("timestamp", "1 minute") \
		.groupBy(window("timestamp" , "1 minute" , "1 minute"), "country") \
		.agg(sum("total_cost").alias("total_sales_volume"),count("invoice_no").alias("OPM"), 
		avg("is_return").alias("rate_of_return"),(sum("total_cost")/count("type")).alias("avg_transaction_size")) \
		.select("window" , "OPM", "total_sales_volume", "rate_of_return", "avg_transaction_size")

#Time based KPI
queryTime = aggStreamByTime.writeStream \
	.outputMode("append") \
	.format("json") \
	.option("truncate","false") \
	.option("path","Timebased-KPI") \
	.option("checkpointLocation","Timebased-KPI_Json") \
	.trigger(processingTime="1 minute") \
	.start()

# Calculating time and country based KPIs
aggregateStreamByTimeCountry = expandedOrderStreamDF \
		.withWatermark("timestamp", "1 minute") \
		.groupBy(window("timestamp" , "1 minute" , "1 minute"), "country") \
		.agg(sum("total_cost").alias("total_sales_volume"),count("invoice_no").alias("OPM"), avg("is_return").alias("rate_of_return")) \
		.select("window", "country" ,"OPM", "total_sales_volume", "rate_of_return")

#Time-Country based KPI
querybyTimeAndCountry = aggregateStreamByTimeCountry.writeStream \
	.outputMode("append") \
	.format("json") \
	.option("truncate","False") \
	.option("path","Country-and-timebased-KPI") \
	.option("checkpointLocation","Country-and-timebased-KPI_Json") \
	.trigger(processingTime="1 minute") \
	.start()

# Printing on Console
queryFinal = expandedOrderStreamDF  \
	.select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return") \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.option("truncate","False")  \
	.start()
	
queryFinal.awaitTermination()
queryTime.awaitTermination()
querybyTimeAndCountry.awaitTermination()

