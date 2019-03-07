package beginnings
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.io.FileInputStream

//YARN Client mode deploy from IDE
//Add SPARK_HOME to Run configurations -> Env Variables: /<path to spark jars>/spark-2.3.2-bin-hadoop2.7
//https://community.hortonworks.com/questions/84654/how-to-run-spark-application-with-yarn-client-mode.html
//add hdp.version in yarn site OR add SPARK_JAVA_OPTS="-Dhdp.version=3.1.0.0-78"
//the yarn cluster should be able to connect to driver
/*/usr/hdp/current/spark2-client/bin/spark-submit --class beginnings.Ignite --master yarn-client
--num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1  ./spark_playdoe-1.0-SNAPSHOT.jar*/
//create the output directory in hdfs
//update project module settings to have the correct resource folder
//try adding the spark-jars.zip to an archive directory in the hdfs cluster

case class Team (team:String, eliminated:String, origin: String, colors: String)

object Ignite {

  def main(args: Array[String]) {
   val props = System.getProperties
    System.setProperty("HADOOP_USER_NAME", "root")
    println(System.getenv("SPARK_HOME"))
   println(System.getenv("SPARK_CONF_DIR"))
    props.entrySet().toArray.foreach(println)


    val confHouse: SparkConf = new SparkConf()
     .setMaster("yarn")
     .setAppName("spark-app-123")
     .set("spark.submit.deployMode","client")
     .set("spark.eventLog.dir","hdfs://alpha.hdp.com:8020/spark2-history/")
     .set("spark.eventLog.enabled","true")
     .set("spark.jars", "/Users/vwunnava/Documents/HWProjects/P72/BitBucket/spark_playdoe/target/spark_playdoe-1.0-SNAPSHOT.jar")
     .set("spark.yarn.archive","hdfs://alpha.hdp.com:8020/user/spark/spark-jars.zip")
     .set("spark.hadoop.yarn.resourcemanager.hostname","alpha.hdp.com")
     .set("spark.hadoop.yarn.resourcemanager.address","alpha.hdp.com:8050")
     .set("spark.yarn.access.namenodes", "hdfs://alpha.hdp.com:8020")
     .set("spark.yarn.stagingDir", "hdfs://alpha.hdp.com:8020/user/spark/tmp/")
      .set("spark.driver.extraJavaOptions","-Dhdp.version=3.1.0.0-78")
      .set("spark.yarn.am.extraJavaOptions","-Dhdp.version=3.1.0.0-78")


    val confSquadron: SparkConf = new SparkConf()
     .setMaster("yarn")
     .setAppName("spark-app-123")
     .set("spark.submit.deployMode","client")
     .set("spark.eventLog.dir","hdfs://hostname:8020/spark2-history/")
     .set("spark.eventLog.enabled","true")
     .set("spark.yarn.archive","hdfs://hostname:8020/user/spark/spark-jars.zip")


    val sparkS = SparkSession.builder.config(confSquadron).getOrCreate()
/*
      .config("spark.driver.extraJavaOptions","-Dhdp.version=3.1.0.0-78")
      .config("spark.yarn.am.extraJavaOptions","-Dhdp.version=3.1.0.0-78")
      .config("spark.jars", "/Users/vwunnava/Documents/HWProjects/P72/BitBucket/spark_playdoe/target/spark_playdoe-1.0-SNAPSHOT.jar")
*/

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
    counts.saveAsTextFile("file:///tmp/somefile5.xml")



  }
}
