import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.io.StdIn
object smallfileDemo {

  def main(args: Array[String]): Unit = {


   val sc=new SparkContext("local[*]","Spark_dev")

    val data = sc.textFile("C:/Users/Karthik Kondpak/Desktop/abc.txt")
    val rdd1 = data.flatMap(x => x.split(" "))


    val rdd2 = rdd1.map(x => (x, 1))


    val rdd3 = rdd2.reduceByKey((x, y) => x + y)


     rdd3.take(1).foreach(println)






    scala.io.StdIn.readLine()







  }

}