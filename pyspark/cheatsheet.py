# ADB notebooks: 
# dbutils.widgets.text("name","value" "Label")
# y = dbutils.widgets.get("name")
from pyspark import SparkContext

sc = SparkContext()
data = range(1,1000)

def RDDs():
    rdd = sc.parallelize(data)
    rdd.take(2)
    rdd.collect()
    data = ['Hello' , 'I' , 'AM', 'Ankit ', 'Gupta']
    Rdd = sc.parallelize(data)
    Rdd1 = Rdd.map(lambda x: (x,1))
    Rdd1.collect()
def sql():
    sc = sparkContext()
    sqlContext = SQLContext(sc)
    train = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/train.csv', header = True,inferSchema = True)
    test = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/test-comb.csv', header = True,inferSchema = True)
   
    train.registerAsTable('train_table')
    sqlContext.sql('select Product_ID from train_table').show(5)
    sqlContext.sql('select Age, max(Purchase) from train_table group by Age').show()
def dataframe():
    train.printSchema()
    train.head(10)
    train.count()
    train.na.drop().count(),test.na.drop('any').count()
    train = train.fillna(-1)
    test = test.fillna(-1)
    train.describe().show()
    train.select('User_ID').show()
    train.select('Product_ID').distinct().count(), test.select('Product_ID').distinct().count()
    df = predictions1.selectExpr("User_ID as User_ID", "Product_ID as Product_ID", 'prediction as Purchase')
    df.toPandas().to_csv('submission.csv')    

def spark_ml():
    diff_cat_in_train_test=test.select('Product_ID').subtract(train.select('Product_ID'))
    diff_cat_in_train_test.distinct().count()
    
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


import os, sys, uuid, re, csv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from os import path
from pyspark.sql import *
from datetime import datetime

def formatting():    
    if sys.platform.lower() == "win32":
        os.system('color')
    #Formatting
    HEADER = lambda x: print('\033[94m' + str(x) + '\033[0m')
    PASS = lambda x: print('\033[92m' + str(x) + '\033[0m')
    WARNING = lambda x: print('\033[93m' + str(x) + '\033[0m')
    FAIL = lambda x: print('\033[91m' + str(x) + '\033[0m')
    TESTCASE = lambda x: print('\033[94m' + "\n=================================\n" + str(x) + "\n=================================" + '\033[0m')
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    # end 

def SQL_SynapseConnection():
    #ENV = "DEV"
    ENV = "QA"
    #ENV = "PROD"
    #==========================
    # Setup log file
    #if not path.exists(ENV + "_TestResults.csv"):
        #log_list = [["Test Run Time", "Test ID", "Element", "Result", "Reason"]]
    #else:

    log_list = []
    now = datetime.now()
    test_run_time = now#.strftime("%d/%m/%Y %H:%M:%S")

    log_row = Row("timestamp", "test_id", "element", "result", "reason")

    # Set Env variables
    if ENV == "DEV":
        server   = 'sql_server_prod.windows.net'
        username = 'sql_user'
        password = 'sql_password'
        database = 'SQL_DB'
        connect_str = 'adl_connection_string'
    elif ENV == "QA":
        # ... 
        print("other configurations")

    # Common Variables
    connectionProperties = {    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver" }
    db_port = 1433
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(server, db_port, database, username, password)

import pyodbc 
def QU_EX(query, sys = 0 ):
    print(query)
    a_c = [ { "h": "sql_server1" , "u" : "sql_user1",  "p":"password1" , "db": "DW_DB" ,  "n": ""},
            { "h": "sql_server2" , "u" : "sql_user2",  "p":"password2" , "db": "DW_DB" ,  "n": ""}
      ]

    l_rmph  = a_c[sys]["h"];  l_rmpu  = a_c[sys]["u"]; l_rmpp  = a_c[sys]["p"]; l_rmpn  = a_c[sys]["n"]; l_sqldb = a_c[sys]["db"]
    connectionProperties = { "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"     } ;   db_port = 1433  
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(l_rmph, db_port, l_sqldb, l_rmpu, l_rmpp)
    #cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};" + jdbc_url)
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connectionProperties)
    counter_rows = [x for x in df.rdd.collect()]
    print(len(counter_rows))
    return df

l_qqq = "SELECT * FROM DB WHERE COL_1 LIKE 'X' ORDER BY COL_2; "
l_qqq = "(select DB from ETL.Entity_ABC) alias"
df = QU_EX(l_qqq, 1)  
result = [(x[0]) for x in df.rdd.collect()]
