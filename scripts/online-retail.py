from os import truncate
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


schema_product_month_sell = StructType([
								StructField('Description', StringType(), True), 
								StructField('Month', StringType(), True), 
								StructField('total_sold', IntegerType(), True)])

#Quality functions
def qaInvoiceNo(df):
	return df.withColumn('qa_invoice_no', 
							F.when(F.col('InvoiceNo').startswith('C'), 'C')
							.when(F.col('InvoiceNo').isNull(), 'F'))

def qaStockCode(df):
	return df.withColumn('qa_stock_code',
							F.when(F.col('StockCode') == 'PADS', 'P')
							.when(F.col('StockCode') == 'S', 'S')
							.when(F.col('StockCode') == 'M', 'M')
							.when(F.col('StockCode').rlike(r'.*[A-Z]'), 'A'))

def qaDescription(df):
	return df.withColumn('qa_description',
							F.when(F.col('StockCode').isNull(), 'F')
							.when(F.col('StockCode').contains('?'), 'H')
							.when(~F.col('StockCode').rlike(r'[A-Za-z]'), 'N')
							.when(F.col('StockCode').contains('wrong') |
								  F.col('StockCode').contains('Wrong'), 'W')
							.when(F.col('StockCode').contains('damage') |
								  F.col('StockCode').contains('Damage') |
								  F.col('StockCode').contains('DAMAGE'), 'D')
							.when(F.col('StockCode').contains('Sample') |
								  F.col('StockCode').contains('SAMPLE'), 'S'))

def qaQuantity(df):
	return df.withColumn('qa_quantity',
							F.when(F.col('Quantity') < 0, 'NS'))

def qaInvoiceDate(df):
	return df.withColumn('qa_invoice_date',
							F.when(F.col('InvoiceDate').isNull(), 'F')
							.when(~F.col('InvoiceDate').rlike(r'[0-9 /:]'), 'T'))

def qaUnitPrice(df):
	return df.withColumn('qa_unit_price',
							F.when(~F.col('UnitPrice').rlike(r'[0-9,]'), 'A')
							.when(F.col('UnitPrice') < 0, 'NS')
							.when(F.col('UnitPrice') == 0, 'FR'))

def qaCustomerId(df):
	return df.withColumn('qa_customer_id',
							F.when(F.col('CustomerID').isNull(), 'F'))

def qaCountry(df):
	return df.withColumn('qa_country',
							F.when(F.col('Country') == 'Unspecified', 'U'))

def qaOnlineRetail(df):
	df = qaInvoiceNo(df)
	df = qaStockCode(df)
	df = qaDescription(df)
	df = qaQuantity(df)
	df = qaInvoiceDate(df)
	df = qaUnitPrice(df)
	df = qaCustomerId(df)
	df = qaCountry(df)
	df.show(5)

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
def toTimestamp(df, time):
	return (df.withColumn(time, 
				F.to_timestamp(F.col(time), 'd/M/yyyy H:m')))

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

def giftCardMonth(df, quant, price):
	print("Question 2")
	df_aux = value(df, quant, price)
	(df_aux.filter((F.col('StockCode').startswith('gift_0001')) & 
					~(F.col('InvoiceNo').startswith('C')) &
					(F.col("Quantity") > 0))
			.groupBy(F.month('InvoiceDate').alias("month"))
			.agg(F.round(F.sum("value"), 2).alias('total_sold_value'))
			.sort("month")
			.show())


def samplesGiven(df, quant, price):
	print("Question 3")
	df = value(df, quant, price)
	(df.filter((F.col('StockCode') == 'S' )& 
				~(F.col('InvoiceNo').startswith('C')))
		.select(
				F.round(
				F.sum(F.col('value')), 2).alias('total_sample_value'))
		.show())

def frequentProduct(df):
	print("Question 4")
	(df.filter(~(F.col('InvoiceNo').startswith('C') & 
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

def frequentProductMonth(df):
	print("Question 5")
	df_month = spark.getOrCreate().createDataFrame([], schema_product_month_sell)
	df_aux = df.withColumn('Month', F.month('InvoiceDate'))
	for i in range(12):
		df_month = df_month.union(df_aux.filter((F.col('Month') == i + 1) & 
											(F.col('Description').isNotNull()) &
											(F.col('Description').rlike(r'[a-z0-9]')) &
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
	
	df_month.show(truncate=False)

def hourMostFrequentSells(df):
	print("Question 6")
	df_aux = df.withColumn("Hour", F.hour('InvoiceDate'))
	(df_aux.filter((~F.col('InvoiceNo').startswith('C')) & 
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

def monthMostFrequentSells(df):
	print("Question 7")
	df_aux = df.withColumn('Month', F.month('InvoiceDate'))
	(df_aux.filter((~F.col('InvoiceNo').startswith('C')) & 
						(F.col("Quantity") > 0) &
						(F.col('StockCode') != 'PADS'))
			.groupBy('Month')
			.agg(F.sum(F.col('Quantity').cast('int')).alias('total_sold'))
			.agg(F.max(
						F.struct(
								F.col('total_sold'), F.col('Month'))).alias('month_most_frequent'))
			.select(F.col('month_most_frequent.Month').alias('Month'),
					F.col('month_most_frequent.total_sold').alias('sells'))
			.show())

def productMonthMostSells(df):
	print('Question 8')
	df = df.withColumn('month', F.month('InvoiceDate'))
	df_aux =(df.filter((~F.col('InvoiceNo').startswith('C')) & 
						(F.col('StockCode') != 'PADS'))
			.groupBy('month')
			.agg(F.sum(F.col('Quantity')).alias('sum_q'))
			.agg(
				F.max(
					F.struct(F.col('sum_q'), F.col('month'))).alias('most_frequent_month')))
	
	df_aux = df_aux.join(df, df_aux['most_frequent_month.month'] == df.month, 'inner')

	(df_aux.groupBy('Description')
		.agg(F.sum("Quantity").cast('int').alias('sold'))
		.sort('sold', ascending=False)
		.show(1))

def countrySellsValue(df):
	print("Question 9")
	df = value(df, 'Quantity', 'UnitPrice')
	(df.filter((~F.col('InvoiceNo').startswith('C')) & 
						(F.col("Quantity") > 0) &
						(F.col('UnitPrice') > 0) &
						(F.col('StockCode') != 'PADS'))
		.groupBy('Country')
		.agg(F.round(F.sum(F.col('value')), 2).alias('sold_value'))
		.agg(
			F.max(
				F.struct('sold_value', 'Country')).alias('country_most_sells'))
		.select(F.col('country_most_sells.Country'), 
				F.col('country_most_sells.sold_value'))
		.show())

def countryManualSellsValue(df):
	print("Question 10")
	df = value(df, 'Quantity', 'UnitPrice')
	(df.filter((~F.col('InvoiceNo').startswith('C')) & 
						(F.col("Quantity") > 0) &
						(F.col('UnitPrice') > 0) &
						(F.col('StockCode') != 'PADS') &
						(F.col('StockCode') == 'M'))
		.groupBy('Country')
		.agg(F.round(F.sum(F.col('value')), 2).alias('sold_value'))
		.agg(
			F.max(
				F.struct('sold_value', 'Country')).alias('country_most_sells'))
		.select(F.col('country_most_sells.Country'), 
				F.col('country_most_sells.sold_value'))
		.show())

def mostValuebleInvoice(df):
	print("Question 11")
	df = value(df,'Quantity', 'UnitPrice')
	(df.filter((~F.col('InvoiceNo').startswith('C')) & 
				(F.col("Quantity") > 0) &
				(F.col('UnitPrice') > 0) &
				(F.col('StockCode') != 'PADS'))
		.groupBy('InvoiceNo')
		.agg(F.round(F.sum(F.col('value')), 2).alias('invoice_value'))
		.sort('invoice_value', ascending=False)
		.show(1))

def invoiceMostItems(df):
	print("Question 12")
	(df.filter((~F.col('InvoiceNo').startswith('C')) & 
				(F.col("Quantity") > 0) &
				(F.col('UnitPrice') > 0) &
				(F.col('StockCode') != 'PADS'))
		.groupBy('InvoiceNo')
		.agg(F.sum('Quantity').cast('int').alias('Items'))
		.sort('Items', ascending=False)
		.show(1))

def mostFrequentCustomer(df):
	print('Question 13')
	(df.filter((~F.col('InvoiceNo').startswith('C')) & 
				(F.col("Quantity") > 0) &
				(F.col('UnitPrice') > 0) &
				(F.col('StockCode') != 'PADS') &
				(F.col('CustomerID').isNotNull()))
			.groupBy('CustomerID')
			.count()
			.sort('count', ascending=False)
			.show(1))

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

	#qaOnlineRetail(df)

	df = formatNum(df, 'UnitPrice')
	df = stringLower(df, "Description")
	df = toTimestamp(df, 'InvoiceDate')

	# df.printSchema()
	# df.withColumn('month', F.month('InvoiceDate')).show()

	# giftCard(df, 'Quantity', 'UnitPrice') #Pergunta 1
	# giftCardMonth(df, 'Quantity', 'UnitPrice') #Pergunta 2
	# samplesGiven(df, 'Quantity', 'UnitPrice') # Pergunta 3
	# frequentProduct(df) #Pergunta 4
	# frequentProductMonth(df) #Pergunta 5 
	# hourMostFrequentSells(df) #Pergunta 6
	# monthMostFrequentSells(df) #Pergunta 7
	# productMonthMostSells(df) #Pergunta 8
	#countrySellsValue(df) #Pergunta 9
	#countryManualSellsValue(df) #Pergunta 10
	#mostValuebleInvoice(df) #Pergunta 11
	#invoiceMostItems(df) #Pergunta 12
	#mostFrequentCustomer(df) #Pergunta 13
