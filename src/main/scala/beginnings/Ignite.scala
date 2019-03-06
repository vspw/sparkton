package beginnings
import org.apache.spark.sql.SparkSession

//YARN Client mode deploy from IDE
//Add SPARK_HOME to Run configurations -> Env Variables: /<path to spark jars>/spark-2.3.2-bin-hadoop2.7
//https://community.hortonworks.com/questions/84654/how-to-run-spark-application-with-yarn-client-mode.html

case class Team (team:String, eliminated:String, origin: String, colors: String)

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
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","hdfs://alpha.hdp.com:8020/spark2-history/")
      .getOrCreate()


   //local mode
/*    val sparkS = SparkSession.builder.appName("Spark-HW12622-Configurations")
      .master("yarn")
      .config("spark.driver.memory","1g")
      .config("spark.submit.deployMode","client")
      .getOrCreate()*/

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sparkS.sparkContext.textFile("src/main/resources/somefile.xml")

/*    val myFistDF = sparkS.range(10,100)
    val myJsonDF = sparkS.read.json("file:///Users/vwunnava/Documents/HWProjects/P72/BitBucket/spark_playdoe/src/main/resources/jsonRecords.json")
    println(myJsonDF.printSchema())
    myJsonDF.collect().foreach(println)
    val myJsonDS = myJsonDF.as[Team]
    myJsonDS.show()*/

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());

    //use file prefix to store in local
    counts.saveAsTextFile("file:///tmp/somefile4.xml")



  }
}
