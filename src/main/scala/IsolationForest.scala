import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

sealed trait ITree
case class ITreeBranch(left: ITree, right: ITree, split_column: Int, split_value: Double) extends ITree
case class ITreeLeaf(size: Long) extends ITree

case class IsolationForest(num_samples: Long, trees: Array[ITree]) {
  def predict(x:Array[Double]): Double = {
    val predictions = trees.map(s=>pathLength(x, s, 0)).toList
    println(predictions.mkString(","))
    math.pow(2, -(predictions.sum/predictions.size)/cost(num_samples))
  }

  def cost(num_items:Long): Int =
    (2*(math.log(num_items-1)+0.5772156649)-(2*(num_items-1)/num_items)).toInt

  @scala.annotation.tailrec
  final def pathLength(x:Array[Double], tree:ITree, path_length:Int): Double ={
    tree match{
      case ITreeLeaf(size) =>
        println("Size: "+size+" path_lenth: "+path_length)
        if (size > 1) path_length + cost(size)
        else path_length + 1

      case ITreeBranch(left, right, split_column, split_value) =>
        println("Column:"+split_column+" split_value: "+split_value)
        val sample_value = x(split_column)

        if (sample_value < split_value)
          pathLength(x, left, path_length+1)
        else
          pathLength(x, right, path_length+1)
    }
  }
}

object IsolationForest {

  def getRandomSubsample(data: RDD[Array[Double]], sampleRatio: Double, seed: Long = Random.nextLong): RDD[Array[Double]] = {
    data.sample(false, sampleRatio, seed=seed)
  }

  def buildForest(data: RDD[Array[Double]], numTrees: Int = 2, subSampleSize: Int = 256, seed: Long = Random.nextLong) : IsolationForest = {
    val numSamples = data.count()
    val numColumns = data.take(1)(0).size
    val maxHeight = math.ceil(math.log(subSampleSize)).toInt
    val trees = Array.fill[ITree](numTrees)(ITreeLeaf(1))

    val trainedTrees = trees.map(s=>growTree(getRandomSubsample(data, subSampleSize/numSamples.toDouble, seed), maxHeight, numColumns))

    IsolationForest(numSamples, trainedTrees)
  }

  def growTree(data: RDD[Array[Double]], maxHeight:Int, numColumns:Int, currentHeight:Int = 0): ITree = {
    val numSamples = data.count()
    if(currentHeight>=maxHeight || numSamples <= 1){
      return new ITreeLeaf(numSamples)
    }

    val split_column = Random.nextInt(numColumns)
    val column = data.map(s=>s(split_column))

    val col_min = column.min()
    val col_max = column.max()
    val split_value = col_min+Random.nextDouble()*(col_max-col_min)

    val X_left = data.filter(s=>s(split_column) < split_value).cache()
    val X_right = data.filter(s=>s(split_column) >= split_value).cache()

    println("COLUMN: "+split_column+"MIN: "+col_min+" MAX: "+col_max+" SPLIT VALUE: "+split_value+" LEFTSIZE: "+X_left.count()+" RIGHTSIZE: "+X_right.count())

    new ITreeBranch(growTree(X_left, maxHeight, numColumns, currentHeight+1),
                  growTree(X_right, maxHeight, numColumns, currentHeight+1),
                  split_column,
                  split_value)
  }
}


object Runner{
  def main(args:Array[String]): Unit ={
    Random.setSeed(1337)

    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("IsolationTree")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "1")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/cs-training.csv")

    val data =
      lines
        .map(line=>line.replace("NA", "-1.0"))
        .map(line => line.split(",")
        .map(elem => elem.trim))
        .map(s=>s.slice(1,s.length)) //lines in rows
    val header = new SimpleCSVHeader(data.take(1)(0)) // we build our header with the first line
    val rows = data.filter(line => line(0) != header.getColumn(0) ).map(s=>s.map(_.toDouble)) // filter the header out and first row

    println("Loaded CSV File...")
    println(header.name2column.keys.mkString("\n"))
    println(rows.take(10).deep.mkString("\n"))

    println("Building Points...")
    //val points = rows.map(s=>LabeledPoint(s(0), Vectors.dense(s.slice(1, s.length))))
    val vecs = rows.map(s=>LabeledPoint(s(0), Vectors.dense(s.slice(1, s.length))))

    val forest = IsolationForest.buildForest(rows, 2)

    val local_rows = rows.take(100)
    for(row <- local_rows){
      println("ForestScore", forest.predict(row.slice(1, row.length)), "Label", row(0))
    }
    println("Finished Isolation")

//  val splits = vecs.randomSplit(Array(0.8, 0.2), seed=1337)
//  val training = splits(0).cache()
//  val test = splits(1).cache()


//  val model = RandomForest.trainClassifier(training, 2, Map[Int, Int](), 250, "auto", "gini", 5, 32)
//  val scoreAndLabels = test.map({ point =>
//    val score = model.predict(point.features)
//    (score, point.label)
//  })

//  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//  val auROC = metrics.areaUnderROC()
//  println("Area under ROC = "+auROC)

  }

}

/*trait TextOps

trait CsvOps {

  def lines(filname: String): RDD[String]

  def lines(filename: String)(implicit sc: SparkContext): Array[String] = {
    sc.textFile(filename)
      .map(line => line.split(","))
  }

  lines("test").map()

}
*/

class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val name2column = header.zipWithIndex.toMap
  val column2name = name2column.map(_.swap)

  def apply(array:Array[String], key:String):String = array(name2column(key))
  def apply(array:Array[String], key:Int):String = array(key)

  def getIndex(key:String):Int = name2column(key)
  def getColumn(key:Int):String = column2name(key)
}
