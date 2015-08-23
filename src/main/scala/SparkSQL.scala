import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hsperr on 8/6/15.
 */
object SparkSQL {

  def main(args: Array[String]) = {
      val conf = new SparkConf()
    .setMaster("local[3]")
    .setAppName("SparkSQL")
    .set("spark.executor.memory", "1g")
//    .set("spark.rdd.compress", "true")
//    .set("spark.storage.memoryFraction", "1")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/cs-training.csv")
  df.show()
    df.printSchema()
    df.registerTempTable("credit")
    val aggDF = sqlContext.sql("select sum(SeriousDlqin2yrs) from credit")
    println(aggDF.collectAsList())
  }

}
