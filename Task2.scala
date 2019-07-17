import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)
	val textFile = sc.textFile(args(0))

    val output = sc.parallelize(
    	Array(textFile.flatMap(line => line.split(",").drop(1).filter{e => e != ""}).count()))

    output.repartition(1).saveAsTextFile(args(1))
  }
}
