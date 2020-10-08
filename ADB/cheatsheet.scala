// Scala Cheat sheet 
def basics(){
    val vString: String = "String" ; 
    var i:       Int    = 1; 
    if (i == 1){ println("test");}
    for( i <- 1 to 10){ println( "Value of i: " + i );}
    mod match {
        case "DL"  => print("DL:" + pdate + ":\n" ); // Delta load 
        case "IL"  => print("IL:" + ":\n" ); // Initial load 
        case whoa  => println("Unexpected case: " + whoa.toString)
    }
    def mul2(m: Int): Int = m * 10  // Output: mul2: (m: Int)Int
    mul(2)
    
    // arrays
    var name = Array("Faizan","Swati","Kavya", "Deepak", "Deepak")//    var name:Array[String] = new Array[String](3)
    var name = new Array[String](3) // output : name: Array[String] = Array(null, null, null)
    name(0) = "jal"; name(1) = "Faizy"; name(2) = "Expert in deep learning"
    val numbers = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5) ;   numbers(2) // res: Int 3 
    number1 = List( List(1, 0, 0), List(0, 1, 0), List(0, 0, 1) )
    // Class
    object HelloWorld {
    def main(args: Array[String]) {
        println("Hello, world!")
        }
    }
    // %fs magic commands 
    // %fs ls, rm -r foobar , put -f "mnt/my-file" "Hello World!"
}

// SPARK 

def RDD_Basics(){ // collect 
    val data = Array(1, 2, 3, 4, 5,6,7,8,9,10)
    val distData = sc.parallelize(data); 
    distData.collect() /// see the content 
    val lines = sc.textFile("text.txt"); lines.take(2) // see first 2 lines => Array(xx,yy)
}

def DataFrames(){
    // csv => packages com.databricks:spark-csv_2.10:1.3.0
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("train.csv")
    // df.columns ; df.count() ; df.printSchema() ; df.show(2)
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
    // 
    // FILTERS: 
}


def DataFrameWithSchema(){
    // https://docs.databricks.com/_static/notebooks/read-csv-schema.html
    import org.apache.spark.sql.types._

    val schema = new StructType()
    .add("_c0",IntegerType,true)
    .add("carat",DoubleType,true)
    .add("cut",StringType,true)
    .add("color",StringType,true)
    .add("clarity",StringType,true)
    .add("depth",DoubleType,true)
    .add("table",DoubleType,true)
    .add("price",IntegerType,true)
    .add("x",DoubleType,true)
    .add("y",DoubleType,true)
    .add("z",DoubleType,true)

    val diamonds_with_schema = spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

    diamonds_with_schema.printSchema

    display(diamonds_with_schema)


}

