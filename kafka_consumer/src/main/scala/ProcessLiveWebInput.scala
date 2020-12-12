import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SaveMode

import scala.math.pow
import scala.collection.mutable

object ProcessLiveWebInput {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  //all that this program does is update the sentiment analysis score in the hbase table
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("dasnes_view_as_hbase"))
  val spark = SparkSession.builder().getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(2))
  import spark.implicits._

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    val trainedModel = PipelineModel.load("hdfs:///tmp/dasnes-final-project/sample-data/models/")

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("topic_dasnes_json_reached_s3")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream_like_this_dasnes",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val report = serializedRecords.map(rec => mapper.readValue(rec, classOf[KeyFromLambdaHandler]))
    println(report)
    val batchStats = report.map(k => {
      val key = k.key
      val body = k.body

      // parse metadata encoded in the string

      val DD = key.substring(0, 2).toInt
      val Month = key.substring(2, 4).toInt
      val YYYY = key.substring(4, 8).toInt
      val HH = key.substring(8, 10).toInt
      val MinMin = key.substring(10, 12).toInt
      val SecSec= key.substring(12, 14).toInt

      var tmp = key.substring(key.indexOf('.') + 1, key.size)
      val parsedDept = tmp.substring(0, tmp.indexOf('.'))
      tmp = tmp.substring(tmp.indexOf('.') + 1, tmp.size)
      val parsedZone = tmp.substring(0, tmp.indexOf('.'))

      val zone = parsedZone
      val deptName = parsedDept
      var tod_str = "" // filled below
      var season = "" // filled below

      val tod = HH
      if (tod >= 0 && tod < 6) {tod_str = "latenight"}
      else if (tod >= 6 && tod < 12) {tod_str = "morn"}
      else if (tod >= 12 && tod < 18) {tod_str = "aftrn"}
      else {tod_str="night"}
      //now get the season too
      val date_region = Month
      if (date_region <= 3) season = "winter"
      else if (date_region <= 6) season = "spring"
      else if (date_region <= 9) season = "summer"
      else season = "fall"

      var rowKey = (deptName + zone + tod_str.toString + season.toString).toString
      println("writing to hbase row: " + rowKey)

      table.incrementColumnValue(
        rowKey.getBytes,
        "stats".getBytes,
        "sentiment_score_total".getBytes,
        1
      )

      if (body.isEmpty) {
        // key refers to the key to the s3 bucket storing the actual file
        // but there are two types of jsons we may have to handle - one for web audio input, one for web text input
        // encoded in the key is an indicator as to which type of json to expect
        // first we parse that from the key
        /*
        val isAudioJson = key.contains("yesAudio")
        val json_df = spark.read.json("s3://dasnes-mpcs53014/" + key)
        var text = ""

        if (!isAudioJson){
          // let's extract the text column, since that's all we need
          text = json_df.select("text").take(1).mkString
        } else {
          // hacky method to extract section of json that has transcript as a string
          val tmpText = json_df.select("results").take(1).mkString
          text = tmpText.substring(tmpText.lastIndexOf('[') + 1, tmpText.size - 4)
        }

         */
      } else {

        // now do inference
        // then can increment score sum too

        var prediction = 0
        /*
        if (!(body.isEmpty || Option(body) == null || body == null)) {

            val tmpRdd = sc.parallelize(List(body))
            //val tmpRdd = null
            if (tmpRdd != null) {
              val inpDF = tmpRdd.toDF("text")
              //here let's try to filter out none
              if (inpDF != null) {
                val intermed = trainedModel.transform(inpDF.withColumnRenamed("text", "review"))

                if (intermed != null){
                  val tmp = intermed.select("prediction").take(1)
                  if (tmp != null) {
                    if (tmp(0) != null && tmp(0)(0) != null){
                      prediction = tmp(0)(0).asInstanceOf[Double].toInt
                    }
                  }
                }
              }
            }

            //each prediction is 1 or 0
            if (prediction > 0){
              table.incrementColumnValue(
                rowKey.getBytes,
                "stats".getBytes,
                "sentiment_score_sum".getBytes,
                1
              )
            }
        }*/

      }
    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
