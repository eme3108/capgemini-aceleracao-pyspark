from os import truncate
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


schema_product_month_sell = StructType([
								StructField('Description', StringType(), True), 
								StructField('Month', StringType(), True), 
								StructField('total_sold', IntegerType(), True)])

#Transformation functions

### Transform numeric format from ##,## to ##.##
def formatNum(df, col):
	return df.withColumn(col, 
				F.when(F.col(col).contains(','), 
					F.translate(col, ',', '.').cast(DoubleType()))
				.otherwise(F.col(col).cast(DoubleType())))

### Standardize string to lower case
def stringLower(df, col):
	return df.withColumn(col, F.lower(F.col(col)))


### Calculate value of an order
def value(df, quant, price):
	return df.withColumn('value', F.abs(F.col(quant)) * F.col(price))

### Return only the month
def fMonth(df, time):
	return (df.withColumn('month', 
					F.lpad(F.split(F.col(time), '[:/ ]').getItem(1), 2,'0')))

def fHour(df, time):
	return (df.withColumn('hour', 
					F.lpad(F.split(F.col(time), '[:/ ]').getItem(3), 2,'0')))

def totalSold(df):
	return F.sum('Quantity').cast('int').alias('total_sold')

#Solution functions
def giftCard(df, quant, price):
	print("Question 1")
	df_aux = value(df, quant, price)
	return (df_aux.filter((F.col('StockCode').startswith('gift_0001')) & 
						~(F.col('InvoiceNo').startswith('C')))
					.select(
						F.round(F.sum('value'), 2).alias('total_sold_value'))
					.show())

def giftCardMonth(df, quant, price, time):
	print("Question 2")
	df_aux = fMonth(df, time)
	df_aux = value(df_aux, quant, price)
	return (df_aux.filter((F.col('StockCode').startswith('gift_0001')) & 
						~(F.col('InvoiceNo').startswith('C')) &
						(F.col("Quantity") > 0))
					.groupBy("month")
					.agg(F.round(F.sum("value"), 2).alias('total_sold_value'))
					.sort("month")
					.show())


def samplesGiven(df, quant, price):
	print("Question 3")
	df = value(df, quant, price)
	return (df.filter((F.col('StockCode') == 'S' )& 
					 	~(F.col('InvoiceNo').startswith('C')))
					.select(
						F.round(
							F.sum(F.col('value')), 2).alias('total_sample_value'))
					.show())

def frequentProduct(df):
	print("Question 4")
	return (df.filter(~(F.col('InvoiceNo').startswith('C') & 
						(F.col("Quantity") > 0)) &
						(F.col('StockCode') != 'PADS'))
				.groupBy('Description')
					.agg(totalSold(df))
					.agg(F.max(
						F.struct(
							F.col('total_sold'), 
							F.col('Description'))).alias('most_frequent_product'))
				.select(F.col('most_frequent_product.Description').alias('product'), 
						F.col('most_frequent_product.total_sold').alias('total_sold'))
				.show(truncate=False))

def frequentProductMonth(df, time):
	print("Question 5")
	df_month = spark.getOrCreate().createDataFrame([], schema_product_month_sell)
	df = fMonth(df, time)
	for i in range(12):
		df_month = df_month.union(df.filter((F.col('Month') == i + 1) & 
											(F.col('Description').isNotNull()) &
											(~F.col('InvoiceNo').startswith('C')) &
											(F.col("Quantity") > 0) &
											(F.col('StockCode') != 'PADS')) 
				.groupBy('Description', 'Month')
				.agg(totalSold(df))
				.agg(F.max(
					F.struct(F.col('total_sold'), 
					F.col('Month'), 
					F.col('Description'))).alias('most_frequent_product_month'))
				.select(F.col('most_frequent_product_month.Description').alias('product'), 
						F.col('most_frequent_product_month.Month').alias('Month'),
						F.col('most_frequent_product_month.total_sold').alias('total_sold')))
	
	return df_month.show(truncate=False)

def hourMostFrequentSells(df, time):
	print("Question 6")
	df = fHour(df, time)
	return (df.filter((~F.col('InvoiceNo').startswith('C')) & 
						(F.col("Quantity") > 0) &
						(F.col('StockCode') != 'PADS'))
				.groupBy('Hour')
					.agg(F.sum(F.col('Quantity').cast('int')).alias('total_sold'))
					.agg(F.max(
						F.struct(
							F.col('total_sold'), F.col('Hour'))).alias('hour_most_frequent'))
				.select(F.col('hour_most_frequent.Hour').alias('Hour'),
						F.col('hour_most_frequent.total_sold').alias('sells'))
				.show())

def monthMostFrequentSells(df, time):
	print("Question 7")
	df = fMonth(df, time)
	return (df.filter((~F.col('InvoiceNo').startswith('C')) & 
						(F.col("Quantity") > 0) &
						(F.col('StockCode') != 'PADS'))
				.groupBy('Month')
					.agg(F.sum(F.col('Quantity').cast('int')).alias('total_sold'))
					.agg(F.max(
						F.struct(
							F.col('total_sold'), F.col('Month'))).alias('hour_most_frequent'))
				.select(F.col('hour_most_frequent.Month').alias('Month'),
						F.col('hour_most_frequent.total_sold').alias('sells'))
				.show())

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType()\
								.add('InvoiceNo', StringType(), True)\
								.add('StockCode', StringType(), True)\
								.add('Description', StringType(), True)\
								.add('Quantity', StringType(), True)\
								.add('InvoiceDate', StringType(), True)\
								.add('UnitPrice', StringType(), True)\
								.add('CustomerID', IntegerType(), True)\
								.add('Country', StringType(), True)

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	
	#print(df.show())

	df = formatNum(df, 'UnitPrice')
	df = stringLower(df, "Description")

	giftCard(df, 'Quantity', 'UnitPrice')

	giftCardMonth(df, 'Quantity', 'UnitPrice', 'InvoiceDate')

	samplesGiven(df, 'Quantity', 'UnitPrice')

	frequentProduct(df)

	frequentProductMonth(df, 'InvoiceDate')

	hourMostFrequentSells(df, 'InvoiceDate')

	monthMostFrequentSells(df, 'InvoiceDate')