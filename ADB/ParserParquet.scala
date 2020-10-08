/**********************************************
    INIT 
********************************************* */
val p_server = dbutils.widgets.get("p_server")
val p_input =  dbutils.widgets.get("p_input")
var a_inpt     = p_input.split("-")
println(a_inpt.length);
var l_desc     = a_inpt(4)
if( a_inpt.length > 6){
  var iinp = 5 ;
  var ii_end = a_inpt.length - 2 ;
  while(iinp <= ii_end ) {
     l_desc  = l_desc + "-" + a_inpt(iinp)
     iinp    = iinp + 1;
  }
}
val l_type     = "xx"  
// var l_path     = "landing/folders>>>" + p_server + "/xx/" + a_inpt(3) + "/" + a_inpt(2) + "/" + l_desc
var l_path     = "landing/folder/"
// var l_pathst   = "staging/fodler/" + p_server + "/xx/" + a_inpt(3) + "/" + a_inpt(2) + "/" 
var l_pathst   = "staging/fodler/" + p_server + s"/$l_type/" + a_inpt(3) + "/" + a_inpt(2) + "/" 
var l_jc       = "landing/folder/schema.json"

var env = "dev" //dev, qual, prod
p_server match {
     case "dev"   => env = "dev"
     case "qual"  => env = "qual"
     case "prd"   => env = "prod"
     case whoa  => println("Unexpected case: " + whoa.toString)
  }

print("Initialization")
import org.apache.spark.sql.functions._
import com.databricks.backend.daemon.dbutils.FileInfo

/* *********** GET CONTAINER********************* */ 
def getContainerPath(env: String, cont:String="staging"): String = {
  var containerPath = ""
  var dl = ""
  var kk = ""
  env match {
     case "dev"  => dl =  "adl_dev" 
      //kk = dbutils.secrets.get(scope="Scope",key="ADL_K_DEV");
      kk = "kk"
     case "qual"  => dl =  "adl_qual"
      kk =  "kk"
     case "prod"  => dl =  "adl_prod"
      kk =  "kk"
     case whoa  => println("Unexpected case: " + whoa.toString)
  }
  spark.conf.set("fs.azure.account.key."+dl+".dfs.core.windows.net",kk)
  //containerPath = "abfss://staging@"+dl+".dfs.core.windows.net"
  containerPath = "abfss://"+cont+"@"+dl+".dfs.core.windows.net"
  return containerPath
}
val containerPath = getContainerPath(env,"landing" )
val toDir = getContainerPath(env,"staging" )


/* *********** MOUNT CONTAINER********************* */ 


/* *********** PARSE TSV to parquet********************* */ 
def convertTSV_PARQUET(): String = {
    val df   = spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        //.option("sep","\t")
        .option("delimiter", "\t")
        .load(s"$containerPath/$l_path")


    print(df_data.count); print("\n")
    val outputData = df_data; 
    outputData.repartition(1).write
    // outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
        .mode(SaveMode.Overwrite).parquet(s"$toDir/$l_path") 
        //.mode(SaveMode.Overwrite).parquet(s"$toDir/$l_pathst") 
    print("test")
    //delete folder after parquet file created // copy files into ARCHIVE folder
    // dbutils.fs.rm(s"$containerPath/$l_path", true)

}

/* *********** PARSE TSV to Parquet with JSON SCHEMA ********************* */ 
def convertTSV_PARQUET_withSchema(): String = {
    // read json: 
    val countDF = spark.read.json(s"$containerPath/$l_jc")

    var schema = new StructType()

    // schema = schema.add("col_name", StringType,true)
    // schema.add("", StringType,true)

							
    val df   = spark.read.format("com.databricks.spark.csv")
                  .option("header", "true")
                //.option("inferSchema", "true")
                .schema(schema)
                .option("delimiter", "\t")
                .load(s"$containerPath/$l_path")

    // val df_data = df.select(df.columns.map(c => col(c).cast(StringType)) : _*)
    val df_data = df
    //print(df_data.count); print("\n")


}
