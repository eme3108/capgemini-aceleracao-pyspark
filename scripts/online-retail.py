from os import truncate
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
 
def gift_(df):
	return df.withColumn("StockCode", 
				F.when(F.col('StockCode').startswith('gift_0001'), 'gift_0001')
				.otherwise(F.col('StockCode')))

def giftCard(df):
	print("Questao 1")
	df_aux = df.withColumn('buy_value', F.round(F.col('Quantity') * F.col('UnitPrice'), 2))
	return df_aux.filter(F.col('StockCode') == 'gift_0001')\
					.groupBy("StockCode")\
					.agg(F.round(F.sum("buy_value"), 2).alias('total_sold_value')).show()

def giftCardMonth(df):
	df_aux = (df.withColumn('month', 
					F.lpad(F.split(F.col('InvoiceDate'), '[:/ ]').getItem(1), 2,'0'))
				.withColumn('buy_value', F.round(F.col('Quantity') * F.col('UnitPrice'), 2)))
	return (df_aux.filter(F.col('StockCode') == 'gift_0001')
					.groupBy("StockCode", "month")
					.agg(F.round(F.sum("buy_value"), 2).alias('total_sold_value'))
					.sort("month")
					.show())

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType()\
								.add('InvoiceNo', IntegerType(), True)\
								.add('StockCode', StringType(), True)\
								.add('Description', StringType(), True)\
								.add('Quantity', IntegerType(), True)\
								.add('InvoiceDate', StringType(), True)\
								.add('UnitPrice', DoubleType(), True)\
								.add('CustomerID', IntegerType(), True)\
								.add('Country', StringType(), True)

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	
	#print(df.show())
	
	df = gift_(df)
	# giftCard(df)

	giftCardMonth(df)