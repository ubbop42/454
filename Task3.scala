import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => line.split(",").zipWithIndex.filter{entry => entry._2 != 0})
    	.map(entry => { if (entry._1.matches("^[1-5]$")) (entry._2, 1) else (entry._2, 0) })
    	.reduceByKey(_ + _)
    	.map(x => x._1 + "," + x._2)

    output.repartition(1).saveAsTextFile(args(1))
  }
}
