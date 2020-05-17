
//https://learn.datacamp.com/courses/introduction-to-scala 
def DataCamp() // BlackJack
{
  // Unit 1: a scalable language
  val difference = 8-5
  // Print the difference
  println(difference ) 

  // val = immutable; var = muttable 
  // Define immutable variables for clubs 2♣ through 4♣
  val twoClubs:   Int = 2
  val threeClubs: Int = 3
  val fourClubs:  Int = 4

  // Define immutable variables for player names
  val playerA: String = "Alex" ; 
  val playerB: String = "Chen" ;
  val playerC: String = "Umberto"; //"Marta";

  // Change playerC from Marta to Umberto
  // playerC = "Umberto" -- val is imutable
  var aceClubs 	: Int = 1  
  var aceDiamonds : Int = 1 
  var aceHearts 	: Int = 1 
  var aceSpades 	: Int = 1 

  // Create a mutable variable for Alex as player A
  var playerA: String = "Alex" ; 
  // Change the point value of A♦ from 1 to 11
  var aceDiamonds : Int = 11
  var jackClubs 	: Int = 10
  // Calculate hand value for J♣ and A♦
  println(aceDiamonds + jackClubs)
}

// https://www.analyticsvidhya.com/blog/2017/01/scala/ 
def ScalaBasis(){
  // 10 
  var Var3 =1 
  if (Var3 ==1){
  println("True")}else{
  println("False")}      // Output: True

  // 11
  for( a <- 1 to 10){
  println( "Value of a: " + a );
  }
  //12 
  def mul2(m: Int): Int = m * 10  // Output: mul2: (m: Int)Int
  mul2(2) //Output: res9: Int = 20

  //13
  var name = Array("Faizan","Swati","Kavya", "Deepak", "Deepak")
  var name:Array[String] = new Array[String](3)
  var name = new Array[String](3)
  // output : name: Array[String] = Array(null, null, null)
  name(0) = "jal"; name(1) = "Faizy"; name(2) = "Expert in deep learning"
  val numbers = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
  number1 = List( List(1, 0, 0), List(0, 1, 0), List(0, 0, 1) )
  numbers(2) // res: Int 3 


  //14
  // . Scala programmer can’t use static methods because they use singleton objects. To read more about singleton object
  //HelloWorld.scala => scalac HelloWorld.scala => scala HelloWorld
  object HelloWorld {
  def main(args: Array[String]) {
    println("Hello, world!")
    }
  }

  // main reasons to learn scala: 
  // Working with Scala is more productive than working with Java
  // Scala is faster than Python and R because it is compiled language
  // Scala is a functional language

}


// Scala - Spark 
def ScalaSparkBasis(){

// 18 RDD - (Resilient Distributed Database)
val data = Array(1, 2, 3, 4, 5,6,7,8,9,10)
val distData = sc.parallelize(data)
distData.collect() /// see the content 
val lines = sc.textFile("text.txt");
lines.take(2) // see first 2 lines => Array(xx,yy)

// 19 DataFrame - 
// csv => packages com.databricks:spark-csv_2.10:1.3.0
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("train.csv")
df.columns // colum names 
df.count() // number of observations 
df.printSchema() // print columns datatype 
df.show(2)
df.select("Age").show(10) // select columns 
// filter rows 
df.filter(df("Purchase") >= 10000).select("Purchase").show(10) 
df.filter(df("Purchase") isin ("10000","2000")).select("Purchase").show(10)
df.filter(df("Purchase") ===  10000).select("Purchase").show(10)
df.groupBy("Age").count().show() 
// apply SQL queries 
df.registerTempTable("B_friday")  // register SQL table B_friday
sqlContext.sql("select Age from B_friday").show(5)

df.collect().foreach(row => <do something>)



// 20 ML model 
val df1 = df.select("User_ID","Occupation","Marital_Status","Purchase")
// -- import formula
import org.apache.spark.ml.feature.RFormula
val formula = new RFormula().setFormula("Purchase ~ User_ID+Occupation+Marital_Status").setFeaturesCol("features").setLabelCol("label")
val train = formula.fit(df1).transform(df1)
// -- linear regression 
import org.apache.spark.ml.regression.LinearRegression
val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
val lrModel = lr.fit(train)
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
val trainingSummary = lrModel.summary
Now, See the residuals for train's first 10 rows.
trainingSummary.residuals.show(10)
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}") 
val train = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("train.csv")
val splits = train.randomSplit(Array(0.7, 0.3))
val (train_cv,test_cv) = (splits(0), splits(1))
import org.apache.spark.ml.feature.RFormula
val formula = new RFormula().setFormula("Purchase ~ User_ID+Occupation+Marital_Status").setFeaturesCol("features").setLabelCol("label")
val train_cv1 = formula.fit(train_cv).transform(train_cv)
val test_cv1 = formula.fit(train_cv).transform(test_cv)
import org.apache.spark.ml.regression.LinearRegression
val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
val lrModel = lr.fit(train_cv1)
val train_cv_pred = lrModel.transform(train_cv1)
val test_cv_pred = lrModel.transform(test_cv1)


// end 

}


// scala partitions: https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
