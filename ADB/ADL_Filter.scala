// Databricks -- ADL spark filter 
print("Initialization")
import org.apache.spark.sql.functions._
import com.databricks.backend.daemon.dbutils.FileInfo
import scala.collection.mutable.ListBuffer

/* *********** GET CONTAINER********************* */ 