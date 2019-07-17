import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
    
    def func(line: String): String = {
      val ratings = line.split(",", -1).drop(1)
      val title = line.split(",", -1)(0)
      val maxRating = ratings.max
      val usersWithMaxRating = ratings.zipWithIndex.filter(entry => entry._1 == maxRating).map(_._2 + 1)
      title + "," + (usersWithMaxRating mkString ",")
  }
    
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
      
    val output = textFile.map(line => func(line))
      
    
    output.saveAsTextFile(args(1))
  }
}
