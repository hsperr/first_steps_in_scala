import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

trait ITree;
class innerNode(left:ITree, right:ITree, split_column:Int, split_value:Double) extends ITree
class exNode(size:Long) extends ITree

class IsolationForest(numTrees:Int = 2,
                      subSampleSize:Int = 256,
                      seed: Long = Random.nextLong) {

  val height_limit = math.ceil(math.log(subSampleSize)).toInt

  def fit(data: RDD[Array[Double]]): Unit ={
    for(t<-1 to numTrees){
      val sample = data.sample(false, subSampleSize/data.count(), seed=seed)
      val tree = grow_tree(sample, 0)
    }
  }

  def grow_tree(X:RDD[Array[Double]], currentHeight:Int): ITree ={
    val num_samples = X.count()
    if(currentHeight>=height_limit || num_samples<=1){
      return new exNode(num_samples)
    }
    val split_column = Random.nextInt(X.take(0).size)
    println(split_column)
    val column = X.map(s=>s(split_column))
    println(column)

    val col_min = column.min()
    val col_max = column.max()
    println(col_min)
    println(col_max)

    val split_value = col_min+Random.nextDouble()*(col_max-col_min)
    println(split_value)

    val X_left = X.filter(s=>s(split_column)<split_value)
    val X_right = X.filter(s=>s(split_column)>=split_value)

    return new innerNode(grow_tree(X_left, currentHeight+1),
                     grow_tree(X_right, currentHeight+1),
                     split_column,
                     split_value)
  }
}

object Runner{
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("IsolationTree")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "1")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/cs-training.csv")

    val data = lines.map(line=>line.replace("NA", "-1.0")).map(line => line.split(",").map(elem => elem.trim)).map(s=>s.slice(1,s.length)) //lines in rows
    val header = new SimpleCSVHeader(data.take(1)(0)) // we build our header with the first line
    val rows = data.filter(line => line(0) != header.getColumn(0) ).map(s=>s.map(_.toDouble)) // filter the header out and first row

    println("Loaded CSV File...")
    println(header.name2column.keys.mkString("\n"))
    println(rows.take(10).deep.mkString("\n"))

    println("Building Points...")
    //val points = rows.map(s=>LabeledPoint(s(0), Vectors.dense(s.slice(1, s.length))))
    val vecs = rows.map(s=>LabeledPoint(s(0), Vectors.dense(s.slice(1, s.length))))

    val forest = new IsolationForest()
    forest.fit(rows.map(s=>s.slice(1, s.length)))

    println("Finished Isolation")

    val splits = vecs.randomSplit(Array(0.8, 0.2), seed=1337)
    val training = splits(0).cache()
    val test = splits(1).cache()


    val model = RandomForest.trainClassifier(training, 2, Map[Int, Int](), 250, "auto", "gini", 5, 32)
    val scoreAndLabels = test.map({ point =>
      val score = model.predict(point.features)
      (score, point.label)
    })

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = "+auROC)

  }

}

class SparkCSV(sc:SparkContext,
               filename:String,
               has_header:Boolean = true,
               sep:String = ",",
               na_fill_value:String="-1.0",
                drop_columns: List[Int] = Nil){

  val lines = sc.textFile(filename)
  val data = lines.map(line => line.split(sep).map(elem => elem.replace("NA", na_fill_value).trim))

  val header = new SimpleCSVHeader(data.take(1)(0))
  val rows = data.filter(line => header(line,0) != line(0)).map(s=>s.map(_.toDouble))
}

class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val name2column = header.zipWithIndex.toMap
  val column2name = name2column.map(_.swap)

  def apply(array:Array[String], key:String):String = array(name2column(key))
  def apply(array:Array[String], key:Int):String = array(key)

  def getIndex(key:String):Int = name2column(key)
  def getColumn(key:Int):String = column2name(key)
}
