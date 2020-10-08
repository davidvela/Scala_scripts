# Scala scripts 

getting started in scala and building small scripts... 
master test

![sc1](https://github.com/davidvela/Scala_scripts/blob/master/assets/logo.jpg)
<br/>
Logo: EPFL computer since building 
École polytechnique fédérale de Lausanne

## variables / types / operations 
val / var xxx : Int, String, Array ... <br/>
val = Immutable  / Var = Mutable 


## links
https://www.scala-lang.org/
https://www.dezyre.com/article/scala-vs-python-for-apache-spark/213 
https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
https://learn.datacamp.com/courses/introduction-to-scala 
https://docs.databricks.com/index.html
https://www.analyticsvidhya.com/blog/2016/09/comprehensive-introduction-to-apache-spark-rdds-dataframes-using-pyspark/

# SPARK 
- three data representation viz: 
    * RDD (Resilient Distributed Database)  - immutable
        * operations: 
        1. Transformation (T) - operation applied on a RDD to create a new RDD
        2. Action (A)- operation applied on RDD that perform computation and send the result back to driver 
        
        * Examples: 
        - Map (T) - operation on each element of RDD - returns new RDD 
        - Reduce (A) - Reduce by key 
        - Apache Spark documentation 

        * RDDs use Shared Variable : 
        ```whenever a task is sent by a driver to executors program in a cluster, a copy of shared variable is sent to each node in a cluster ``` Types: 
        1. Broadcast - save the copy across al node
        2. Accumulator - aggregating the information 
        
        * how to create RDD: 
            1. Existing Storage - ex- a list to RDD 
            2. External Sources - ex shared file system, HDFS, HBase data source offering a Haddop Input Format.
            

    * Dataframe  ** faster > RDD (it as metadata) 
    * Dataset 