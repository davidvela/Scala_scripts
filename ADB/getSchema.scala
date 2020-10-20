import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray

def getSchema(env: String, cont:String="staging"): StructType = {
  var l_jcs   = "path_prefix/" + p_server + "/SCHEMAS/" + a_inpt(3) + ".json" ; 
  val jsonSCH = spark.read.json(s"$containerPath/$l_jcs")
  // display(fields_ds)
  var schema = new StructType()
  // schema = schema.add("", StringType,true)
  // var first = jsonSCH.first
  // val mapped = first.getAs[WrappedArray[Int]](0).mkString
  jsonSCH.collect().foreach(row=> {
  // fields_ds.collect().foreach(row =>{ 
         print(row.get(1));println(row.get(2))
        row.get(2) match {  
          case "C"   => schema = schema.add(row.get(1).toString, StringType, true) ;    // CHAR - Character String 
          case "N"   => schema = schema.add(row.get(1).toString, StringType, true);     //  NUMC - Character String with Digits Only
          case "D"   => schema = schema.add(row.get(1).toString, StringType, true);     //  Date (Date: YYYYMMDD)
          case "T"   => schema = schema.add(row.get(1).toString, StringType, true);     //  Time (Time: HHMMSS)
          case "X"   => schema = schema.add(row.get(1).toString, StringType, true);     //  Byte Seq. (heXadecimal), in DDIC metadata also for INT1/2/4
          case "I"   => schema = schema.add(row.get(1).toString, IntegerType,true);     //  Integer number (4-byte integer with sign)
        //   case "b"   => schema = schema.add(row.get(1).toString, StringType,true);   //  1-byte integer, integer number <= 254
        //   case "s"   => schema = schema.add(row.get(1).toString, StringType,true);   //  2-byte integer, only for length field before LCHR or LRAW
          case "P"   => schema = schema.add(row.get(1).toString, DoubleType, true);     //  Packed number
          case "F"   => schema = schema.add(row.get(1).toString, DoubleType, true);     //  Floating point number to accuracy of 8 bytes
        //   case "g"   => schema = schema.add(row.get(1).toString, StringType,true).;  //  Character string with variable length (ABAP type STRING)
        //   case "y"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Byte sequence with variable length (ABAP type XSTRING)
        //   case "u"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Structured type, flat
        //   case "b"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Structured type, deep
        //   case "h"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Table type
        //   case "V"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Character string (old Dictionary type VARC)
        //   case "r"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Reference to class/interface
        //   case "l"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Reference to data object
        //   case "a"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Decimal Floating Point Number, 16 Digits
        //   case "e"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Decimal Floating Point Number, 34 Digits
        //   case "j"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Static Boxed Components
        //   case "k"   => schema = schema.add(row.get(1).toString, StringType,true);   //  Generic Boxed Components
          case whoa  => schema = schema.add(row.get(1).toString, StringType,true) // others...      
        }                               
  })
  print(schema)
  return schema
}

var df_data = spark.emptyDataFrame
println(l_path)
if( a_inpt(3) == "table1"){ 
  val schema = getSchema();
  val df3   = spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "false") // option 1 
        .schema(schema) // option 2
  //       .option("sep","\t")
        .option("delimiter", "\t")
        .load(s"$containerPath/$l_path")
  df_data = df3
}else{
    val df2   = spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
  //          .schema(schema)
  //       .option("sep","\t")
        .option("delimiter", "\t")
        .load(s"$containerPath/$l_path")
    df_data = df2
}
// val df_data = df.select(df.columns.map(c => col(c).cast(StringType)) : _*)
