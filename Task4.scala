import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.cartesian(textFile).filter { case (a, b) => a < b }.map { case (a, b) =>
      (a.split(",", 2) zip b.split(",", 2)) match {
        case Array(title, rankings) =>
          title._1 + "," + title._2 + "," + (rankings._1.split(",",-1) zip rankings._2.split(",",-1)).count(x => x._1 == x._2 && x._1 != "")
        case Array(title) =>
          title._1 + "," + title._2
      }
    }

    output.saveAsTextFile(args(1))
  }
}
