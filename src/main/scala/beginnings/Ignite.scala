package beginnings
import org.apache.spark.sql.SparkSession

//YARN Client mode deploy from IDE
object Ignite {

  def main(args: Array[String]) {
   val props = System.getProperties
    System.setProperty("HADOOP_USER_NAME", "root")
    println(System.getenv("SPARK_HOME"))
    props.entrySet().toArray.foreach(println)
    val sparkS = SparkSession.builder.appName("Spark-HW12622-Configurations")
      .master("yarn").config("spark.driver.memory","1g")
      .config("spark.submit.deployMode","client")
      .config("spark.driver.extraJavaOptions","-Dhdp.version=3.1.0.0-78")
      .config("spark.yarn.am.extraJavaOptions","-Dhdp.version=3.1.0.0-78")
      .config("spark.jars", "/Users/vwunnava/Documents/HWProjects/P72/BitBucket/spark_playdoe/target/spark_playdoe-1.0-SNAPSHOT.jar")
      .getOrCreate()

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sparkS.sparkContext.textFile("src/main/resources/somefile.xml")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    counts.saveAsTextFile("/tmp/somefile3.xml")
  }
}
