// Databricks -- ADL spark filter 
print("Initialization")
import org.apache.spark.sql.functions._
import com.databricks.backend.daemon.dbutils.FileInfo
import scala.collection.mutable.ListBuffer

dbutils.widgets.text("Hostname","")
val env    = dbutils.widgets.get("p_env") //DEV, QUAL, PROD
val mod    = dbutils.widgets.get("p_mod") // IL, DL, ML, other (stype) 
val pdate  = dbutils.widgets.get("p_date") // IL or date yyyymmdd 20200603

val env = "QUAL" //DEV, QUAL, PROD

/* *********** GET CONTAINER********************* */ 
def getContainerPath(env: String, cont:String="staging"): String = {
  var containerPath = ""; 
  var dl = ""; 
  var kk = ""
  env match {
     case "DEV"  => dl =  "ADL_DEV"
      kk =  "dev_access_key"
     case "QUAL"  => dl =  "ADL_QUAL"
      kk =  "qual_access_key"
     case "PROD"  => dl =  "ADL_PROD"
      kk =  "prod_access_key"
  }
  spark.conf.set("fs.azure.account.key."+dl+".dfs.core.windows.net",kk)
  containerPath = "abfss://"+cont+"@"+dl+".dfs.core.windows.net"
  return containerPath
}
val containerPath = getContainerPath(env)

val df_parquet  = spark.read.parquet(s"$containerPath/path_prefix/${env.toUpperCase()}/path_sufix/IL/")
val df_csv      = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").option("sep",";")
      .load(s"$containerPath/XXX/Z/abc.csv")

/* *********** COPY DATA ********************* */ 
def copyData(tableName: String, env: String, btype:String, delta_day:String, filter:String  ): String = {
  var fromDir   = s"$containerPath/YYY/from/${env.toUpperCase()}/$tableName"
  var toDir     = s"$containerPath/XXX/to/${env.toUpperCase()}/$tableName"
  var foldDir   = ""
  val fName     = tableName.replace('/', '_') 
  var Filter_1  = List("test")
  var Filter_2  = List("test")
  println(s"table ${tableName} - Delta_day = ${delta_day} -- Btype: ${btype}")  
  
  try {  
    if( delta_day == "IL") foldDir = s"IL"; 
    else foldDir = s"${delta_day}_DL"; 
    if(filter == "XXX"){
      Filter_1  = List("ABC");
      Filter_2  = List("0010", "0011", "0012");
    }   
    val inputData = spark.read.parquet(s"$fromDir/$foldDir/*/") //Path to ingested data
    btype match {
       case "FL"        => println(s"Full Load"); 
            val outputData = inputData; 
            //outputData.repartition(1).write
            outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
            .mode(SaveMode.Overwrite).parquet(s"$toDir/IL/") 

      case "F1"    => println(s"Filter 1"); 
            //val outputData = inputData.filter(inputData("COL1") isin ("XXX") )
            val outputData = inputData.filter(inputData("COL1") isin ( Filter_1:_*) )
            //outputData.repartition(1).write.mode(SaveMode.Overwrite).parquet(s"$toDir/$foldDir/") 
            outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
            .mode(SaveMode.Overwrite).parquet(s"$toDir/$foldDir/") 
    
      case "F2"     => println(s"Filter 2");
            //val outputData = inputData.filter(inputData("COL2") isin ("0010", "0011", "0012")) 
            val outputData = inputData.filter(inputData("COL2") isin (Filter_2:_*))   
            outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
            .mode(SaveMode.Overwrite).parquet(s"$toDir/$foldDir/")       
      
      case "F3_JOIN"  => println(s"JOIN Filter");
            val outputData = inputData.join(df_parquet.select("COL3"), "COL3" )
            outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
            .mode(SaveMode.Overwrite).parquet(s"$toDir/$foldDir/") 
  
      case "F4_MX"  => println(s"MX Filter");
            val col4_f     = inputData.filter(inputData("COL4") isin("TYPE_A"))
            val val_col4   = df_parquet.select("COL4").na.drop().rdd.map(r => r(0)).collect()
            val filt_4    = col4_f.filter(col4_f("VALUE") isin(val_col4:_*  ))
            val outputData = inputData.join(filt_4.select("COL5"), "COL5" )    
            outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
            .mode(SaveMode.Overwrite).parquet(s"$toDir/$foldDir/")      

      case "TABLE_X"  => println(s"TABLE_X Custom Filter");
        var fromDirX   = s"$containerPath/XXX/${env.toUpperCase()}/YYY/TABLE_X"
        val inputX     = spark.read.parquet(s"$fromDirX/$foldDir/*/") 
        val outputData = inputData.join(inputX.withColumnRenamed("ID","ID_Y").select("ID_Y"), "ID_Y" ) 
        outputData.coalesce(1).write.option("maxRecordsPerFile", 30000000)    
            .mode(SaveMode.Overwrite).parquet(s"$toDir/$foldDir/")
        
      case whoa  => println(s"Unexpected case: " + btype)
    }
    
    } catch { case _ : 
       Throwable => { 
         println(s"Table does not exists in ${env.toUpperCase()}: $tableName /$foldDir/ ") 
            "N/A" } 
    }  
    "return String"
} 

/* *********COPY DATA LOGIC************************ */ 
import scala.collection.mutable.ListBuffer
//var flist = new ListBuffer[FileInfo]() // fruits += "Apple"
var flist = new ListBuffer[String]() // fruits += "Apple"
var l_count : Int = 0;  
var l_count_ok : Int = 0;  
var l_count_wr : Int = 0;  
val l_limit : Int = 400;  
def copyData_API(row: org.apache.spark.sql.Row, delta_day:String, region:String ): String = {
  // 2 - schema 3 - table 
  //print(row.get(2).toString + row.get(3).toString + "---" + row.get(10).toString + row.get(7).toString + row.get(11).toString + "\n")
  //print(row.get(2).toString + row.get(3).toString + "---"  + row.get(11).toString   +"\n")
  //val mainDir = s"$containerPath/XXXX/${env.toUpperCase()}/${row.get(2)}/${row.get(3)}/";
  val l_tableName = s"${row.get(2)}/${row.get(3)}";
  if(l_limit<10){print(l_tableName);print("\n")} 
  
  if( row.get(7) == "IL" ){
    copyData_FL(l_tableName,env,row.get(11).toString ,"IL", region)
  } else {
    copyData_FL(l_tableName,env,row.get(11).toString ,delta_day, region)
  }
  flist += l_tableName
  l_count += 1
  "N/A"
}
// GET SCHEMAS
def getSchemas(row: org.apache.spark.sql.Row): String = {
  if(l_limit<10){print(row.get(2).toString + "/" + row.get(3).toString  );print("\n")} 
  flist += row.get(2).toString
  l_count += 1
  "N/A"
}
mod match {
    //case "IL"  => not defined 
     case "DL"  =>  // Delta load 
        val aDates = List(pdate)
        val stypes = List("DL", "12", "11") // DL, 11, 12, ML, BT, 41,42, ... 
        aDates.foreach(pdate => {
          print(pdate +":\n" )
          stypes.foreach(_stype=>{
            val dl_x   = df.filter(df("BK") === "X" && df("STYPE") === _stype  ) // && df("DLTYPE") =!= "IL")
            println(_stype + "--" + dl_x.count )
            dl_x.limit(l_limit).collect().foreach(row => copyData_API(row, pdate, "XXX"))
          })
          print("\n\n")
        })  
     case "ML"  => // Monthly load 
           val dl_x   = df.filter(df("BK") === "X" && df("STYPE") === "ML" ) // && df("DLTYPE") =!= "IL")
           println(mod + "--" + dl_x.count )
           dl_x.limit(l_limit).collect().foreach(row => copyData_API(row,"IL", "XXX"))
     case whoa  => // stype  
           val dl_x   = df.filter(df("BK") === "X" && df("STYPE") === mod  ) // && df("DLTYPE") =!= "IL")
           println(mod + "--" + dl_x.count )
           dl_x.limit(l_limit).collect().foreach(row => copyData_API(row, pdate, "XXX"))     
}

def convertTSV_PARQUET():
    // val p_server = dbutils.widgets.get("p_server")
    val p_server = "DX_000"
    //--             // 0         1   2       3            4
    // val p_input =  dbutils.widgets.get("p_input")
    val p_input    = "20200526-124200-IL-TABLE_X-_PARAM"
    var a_inpt     = p_input.split("-")
    println(a_inpt.length);
    var l_desc     = a_inpt(4)
    if( a_inpt.length > 6){
    var iinp = 5 ;
    var ii_end = a_inpt.length - 2 ;
    while(iinp <= ii_end ) {
        l_desc  = l_desc + "-" + a_inpt(iinp)
        iinp    = iinp + 1;  }
    }
    var l_path     = "XXX" + p_server + "/TT/" + a_inpt(3) + "/" + a_inpt(2) + "/" + l_desc
    var l_pathst   = "YYY" + p_server + "/TT/" + a_inpt(3) + "/" + a_inpt(2) + "/" 
    var env = "dev" //dev, qual, prod
    p_server match {
        case "DXX_000"  => env = "dev"
        case "QXX_000"  => env = "qual"
        case "PXX_000"  => env = "prod"
    }
    val df_data   = spark.read.format("com.databricks.spark.csv")
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
    //delete folder after parquet file created
    // dbutils.fs.rm(s"$containerPath/$l_path", true)


// convert_TSV_2_parquet


// LOOP FILES
import scala.collection.mutable.ListBuffer
//var flist = new ListBuffer[FileInfo]() // fruits += "Apple"
var flist = new ListBuffer[String]() // fruits += "Apple"
var l_count : Int = 0; var l_count_ok : Int = 0; var l_count_wr : Int = 0;  val l_limit : Int = 9;  

/* ***************  GET FOLDERS ****************** */ 
def getFolders(row: org.apache.spark.sql.Row): String = {
  // print(row.get(2).toString+row.get(3).toString + "---" + row.get(10).toString + row.get(7).toString + row.get(11).toString + "\n")   // 2 - schema 3 - table 
  //val mainDir = s"$containerPath/GLOBAL/XXX"  
  val mainDir =s"$containerPath/XXX/${env.toUpperCase()}/${row.get(2)}/${row.get(3)}/";
  val tableName = s"${row.get(2)}/${row.get(3)}";
  if(l_limit<10){print(mainDir);print("\n")} 
  try {
      val list = dbutils.fs.ls(mainDir).toList
      //print(list);print("\n")
      list.foreach(file =>{ 
          flist += file.name; 
          //if(file.name != "IL/") 
          l_count += 1;  
      })
      //flist =  flist :: list
   } catch { case _ : Throwable => { println(s"Table does not exists in ${env.toUpperCase()}: $tableName") } }
  "N/A"
}
/* ***************  GET DL FOLDERS > date ****************** */ 
def getDLFolders(row: org.apache.spark.sql.Row, date:String="20200101"): String = {
  val mainDir =s"$containerPath/GL/XX/${env.toUpperCase()}/${row.get(2)}/${row.get(3)}/";
  val tableName = s"${row.get(2)}/${row.get(3)}";
  if(l_limit<10){print(mainDir);print("\n")} 
  try {
      val list = dbutils.fs.ls(mainDir).toList
      //print(list);print("\n")
      list.foreach(file =>{ 
          if(file.name != "IL/"){ 
            if (file.name.slice(0,8) >= date ){
                //print(file.name.slice(0,8)); print("\n")
                flist += file.name.slice(0,8); 
                l_count += 1;  
            }
          }
      })
      //flist =  flist :: list
   } catch { case _ : Throwable => { println(s"Table does not exists in ${env.toUpperCase()}: $tableName") } }
  "N/A"
}
/* ***************** DELETE FOLDERS where xxx ******** */ 
def deleteFolders(row: org.apache.spark.sql.Row, dfold:String): String = {
  // print(row.get(2).toString+row.get(3).toString + "---" + row.get(10).toString + row.get(7).toString + row.get(11).toString + "\n")
  val mainDir =s"$containerPath/GLOBAL/XXX/${env.toUpperCase()}/${row.get(2)}/${row.get(3)}/";
  val l_tableName = s"${row.get(2)}/${row.get(3)}";
  //print(mainDir);print("\n")
  try { val list = dbutils.fs.ls(mainDir).toList
        list.foreach(file =>{  
          if(file.name.contains(dfold)){
            l_count += 1; flist += file.name; 
            //delete command
            //val res = dbutils.fs.rm(file.path, true)
            // if( res == true ) l_count_ok += 1; else l_count_wr += 1; 
          }
      })  } catch { case _ : Throwable => { println(s"Table does not exists in ${env.toUpperCase()}:${row.get(2)}.${row.get(3)}") } }
    "N/A"
}
// -- LIST FOLDERS
// df.limit(l_limit).collect().foreach(row => getFolders(row))
// df.limit(l_limit).collect().foreach(row => getDLFolders(row))
//df.limit(l_limit).collect().foreach(row => getDLFolders(row, "20200512"))
// -- DELETE FOLDERS
// df.limit(l_limit).collect().foreach(row => deleteFolders(row, "_IL"))
print("\n");
print(l_count);print("\n")
print(l_count_ok);print("\n")
print(l_count_wr);print("\n")
flist.distinct

// MOUNT ADL disc in ADB 
def mount_adl(){
    val secret = dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>");
    val configs = Map(  "fs.azure.account.auth.type" -> "OAuth",
    "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id" -> "<App ID>",
    "fs.azure.account.oauth2.client.secret" -> "<Secret>", 
    "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/<directory-id>/oauth2/token")

    // # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(   source = s"abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
                        mount_point = s"/mnt/<mount-name>", extra_configs = configs)
}

// SQL 
// Get some data from an Azure Synapse table.
def sql_adf(){
    val df: DataFrame = spark.read
    .format("com.databricks.spark.sqldw")
    .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>")
    .option("tempDir", "wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>")
    .option("forwardSparkAzureStorageCredentials", "true")
    .option("dbTable", "my_table_in_dw")
    .load()
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(server, db_port, database, username, password)

}
// to do -- 
// write log txt in databricks 

// apply schema to parquet file 

def end():
    print("end")