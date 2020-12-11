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
    val topicsSet = Set("topic_dasnes_web_upload_no_audio")
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
    val report = serializedRecords.map(rec => mapper.readValue(rec, classOf[WebInput]))
    println(report)
    val batchStats = report.map(wi => {
      val zone = wi.zone.toString
      val deptName = wi.dept_name.toString
      var tod_str = "" // filled below
      var season = "" // filled below

      val tod = wi.time.take(2).toInt
      if (tod >= 0 && tod < 6) {tod_str = "latenight"}
      else if (tod >= 6 && tod < 12) {tod_str = "morn"}
      else if (tod >= 12 && tod < 18) {tod_str = "aftrn"}
      else {tod_str="night"}
      //now get the season too
      val date_region = wi.date.take(7).takeRight(2).toInt
      if (date_region <= 3) season = "winter"
      else if (date_region <= 6) season = "spring"
      else if (date_region <= 9) season = "summer"
      else season = "fall"

      var rowKey = (deptName + zone + tod_str.toString + season.toString).toString

      // now do inference
      // then can increment score sum too

      var prediction = 0
      if (!(wi.text.isEmpty || Option(wi.text) == null || wi.text == null)) {
          //val myArr = Array(wi.text)
          println(wi.text)
          if (spark == null){
            println("spark is null")
          }else if( sc == null){
            println("context's null!")
          }
          val tmpRdd = sc.parallelize(List(wi.text))
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
      }

      table.incrementColumnValue(
        rowKey.getBytes,
        "stats".getBytes,
        "sentiment_score_total".getBytes,
        1
      )

    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
