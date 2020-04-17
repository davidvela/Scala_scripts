# examples using pyspark-- 
# https://www.analyticsvidhya.com/blog/2016/09/comprehensive-introduction-to-apache-spark-rdds-dataframes-using-pyspark/
 
from pyspark import SparkContext
sc = SparkContext()

# create RDD - list 
data = range(1,1000)
rdd = sc.parallelize(data)

# see content of RDD 
rdd.collect()

# see first n element : 
rdd.take(2) # It will print first 2 elements of rdd

# Map transformation 
data = ['Hello' , 'I' , 'AM', 'Ankit ', 'Gupta']
Rdd = sc.parallelize(data)
Rdd1 = Rdd.map(lambda x: (x,1))
Rdd1.collect()
#output: [('Hello', 1), ('I', 1), ('AM', 1), ('Ankit ', 1), ('Gupta', 1)]

# lazy operations - you only execute the operations when you need them 

# Practice example: Black Friday Sales ... 
#https://datahack.analyticsvidhya.com/contest/black-friday/
def blackFridaySalesExample():
    #$ ./bin/pyspark --packages com.databricks:spark-csv_2.10:1.3.0
    sc = sparkContext()
    sqlContext = SQLContext(sc)
    train = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/train.csv', header = True,inferSchema = True)
    test = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/test-comb.csv', header = True,inferSchema = True)
    train.printSchema()
    train.head(10)
    train.count()
    train.na.drop().count(),test.na.drop('any').count()
    train = train.fillna(-1)
    test = test.fillna(-1)
    train.describe().show()
    train.select('User_ID').show()
    train.select('Product_ID').distinct().count(), test.select('Product_ID').distinct().count()
    #Output:(3631, 3491)
    diff_cat_in_train_test=test.select('Product_ID').subtract(train.select('Product_ID'))
    diff_cat_in_train_test.distinct().count()# For distict count
    # Output: 46
    from pyspark.ml.feature import StringIndexer
    plan_indexer = StringIndexer(inputCol = 'Product_ID', outputCol = 'product_ID')
    labeller = plan_indexer.fit(train)
    Train1 = labeller.transform(train)
    Test1 = labeller.transform(test)
    Train1.show()

    from pyspark.ml.feature import RFormula
    formula = RFormula(formula="Purchase ~ Age+ Occupation +City_Category+Stay_In_Current_City_Years+Product_Category_1+Product_Category_2+ Gender",featuresCol="features",labelCol="label")
    t1 = formula.fit(Train1)
    train1 = t1.transform(Train1)
    test1 = t1.transform(Test1)
    train1.show()

    train1.select('features').show()
    train1.select('label').show()

    from pyspark.ml.regression import RandomForestRegressor
    rf = RandomForestRegressor()
    (train_cv, test_cv) = train1.randomSplit([0.7, 0.3])
    model1 = rf.fit(train_cv)
    predictions = model1.transform(test_cv)

    from pyspark.ml.evaluation import RegressionEvaluator
    evaluator = RegressionEvaluator()
    mse = evaluator.evaluate(predictions,{evaluator.metricName:"mse" })
    import numpy as np
    np.sqrt(mse), mse

    model = rf.fit(train1)
    predictions1 = model.transform(test1)

    df = predictions1.selectExpr("User_ID as User_ID", "Product_ID as Product_ID", 'prediction as Purchase')

    df.toPandas().to_csv('submission.csv')

# https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
# $ ./bin/pyspark --packages com.databricks:spark-csv_2.10:1.3.0
from pyspark import SparkContext
from pyspark import sqlContext
sc = SparkContext()
train = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/train.csv', header = True,inferSchema = True)
test = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/test-comb.csv', header = True,inferSchema = True)
def testSpark(): 
    # remove cat of products id 
    diff_cat_in_train_test=test.select('Product_ID').subtract(train.select('Product_ID'))
    diff_cat_in_train_test.distinct().count()# For distict count
    # output 46 - categories not found in test 
    # remove those categories
    not_found_cat = diff_cat_in_train_test.distinct().rdd.map(lambda x: x[0]).collect()
    len(not_found_cat)

    from pyspark.sql.types import StringType
    from pyspark.sql.functions import udf  # user defined function
    F1 = udf(lambda x: '-1' if x in not_found_cat else x, StringType())

    k = test.withColumn("NEW_Product_ID",F1(test["Product_ID"])).select('NEW_Product_ID')
    diff_cat_in_train_test=k.select('NEW_Product_ID').subtract(train.select('Product_ID'))
    diff_cat_in_train_test.distinct().count()# For distinct count
    # output = 1 => category -1
    diff_cat_in_train_test.distinct().collect()
    # Output:Row(NEW_Product_ID=u'-1')

def applySQLqueriesDataFrame():
    train.registerAsTable('train_table')
    sqlContext.sql('select Product_ID from train_table').show(5)
    sqlContext.sql('select Age, max(Purchase) from train_table group by Age').show()


# Pandas vs PySpark DataFrame
# Operations on Pyspark DataFrame run parallel on different nodes in cluster // not possible in pandas
# Operations in PySpark Dataframe are lazy in nature. 
# PySpark DataFrame is immutable -> we need to transform it 
# Pandas API support more operations than PySpark -- pandas API more powerful than Spark 
# Complex operation in pandas are easier to perform in Pyspark Dataframe 

# documentation spark in python: http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD