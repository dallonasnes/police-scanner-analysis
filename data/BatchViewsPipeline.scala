import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, CountVectorizer, RegexTokenizer, StopWordsRemover, IDF}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, ArrayType, StructField, StructType}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.math.pow
import scala.collection.mutable

//load in the model
val trainedModel = PipelineModel.load("hdfs:///tmp/dasnes-final-project/sample-data/models/")

//load in the data
val sourceData = spark.table("dasnes_source_from_csv_as_orc")

/*
input schema:
id | dept name | zone | time of day | date of event | duration | text


output schema:
id string,
dept_name string,
zone string,
time_of_day string
season string,
most_common_words string,
least_common_words string,
sentiment_score_sum bigint,
sentiment_score_total bigint
*/

/*

do this in a second new DF
then i make a new DF grouping by those two props
and concat all the text strings

^ that will be done with hive queries

then for each row in the new df
get common words and sentiment scores


val schema = new StructType().
  add(StructField("id", StringType, false)).
  add(StructField("dept_name", StringType, true)).
  add(StructField("zone", StringType, true)).
  add(StructField("time_of_day", StringType, true)).
  add(StructField("date_of_event", StringType, true)).
  add(StructField("duration", DoubleType, true)).
  add(StructField("text", StringType, true))

*/

//now need to get rid of any null possibility in the sourceData
val validatedData = sourceData.filter(col("dept_name").isNotNull).
	filter(col("zone").isNotNull).filter(col("time_of_day").isNotNull).
	filter(col("date_of_event").isNotNull).filter(col("duration").isNotNull).
	filter(col("text").isNotNull)

//first i have to convert input from time into time_of_day (morn, eve, etc)
//and convert date into season
var mappedRdd = validatedData.rdd.map(row => {
       var tod = row.getAs[String](3).take(2).toInt
       var tod_str = ""
       if (tod >= 0 && tod < 6) {tod_str = "latenight"}
       else if (tod >= 6 && tod < 12) {tod_str = "morn"}
       else if (tod >= 12 && tod < 18) {tod_str = "aftrn"}
       else {tod_str="night"}
       //now get the season too
       var date_region = row.getAs[String](4).take(7).takeRight(2).toInt
       var season = ""
       if (date_region <= 3) season = "winter"
       else if (date_region <= 6) season = "spring"
       else if (date_region <= 9) season = "summer"
       else season = "fall"
       Row(row(0), row(1), row(2), tod_str, season, row(5), row.getAs[String](6).split(" ").map(_.replaceAll("[,.!?:;)( \t\n]", "").trim.toLowerCase).
filter(!_.isEmpty).filter(_.length > 2).mkString(" ")) })

val schema = new StructType().
  add(StructField("id", StringType, false)).
  add(StructField("dept_name", StringType, true)).
  add(StructField("zone", StringType, true)).
  add(StructField("time_of_day", StringType, true)).
  add(StructField("date_of_event", StringType, true)).
  add(StructField("duration", DoubleType, true)).
  add(StructField("text", StringType, false))

var mappedDf = spark.createDataFrame(mappedRdd, schema)

//now aggregate together all of the texts 
val result = mappedDf.groupBy("dept_name", "zone", "time_of_day", "date_of_event").agg(collect_list("text").as("text"))
/*val validatedResult = result.filter(col("dept_name").isNotNull).
	filter(col("zone").isNotNull).filter(col("time_of_day").isNotNull).
	filter(col("date_of_event").isNotNull).
	filter(col("text").isNotNull)*/
// NOW show most and least common words by zone

//fixes it
// each row in the dataframe is Array[String]

// this seems to work but has a long runtime
//var tmp = result.select("text").collect().map(x => x(0).asInstanceOf[Seq[String]].map(arr => sc.parallelize(arr.split(" ").map(word => (word, 1))).reduceByKey(_+_)))

// also working but not well
//var tmp = result.select("text").collect().map(x => x(0).asInstanceOf[Seq[String]].map(arr => arr.split(" ").map(word => (word, 1))))

//this gives me an array of strings
var tmp = result.select("text").collect().map(x => x(0).asInstanceOf[Seq[String]].mkString(" "))

//this one works -- assuming no split in the above line
//var newTmp = sc.parallelize(tmp).map( sent => sent.split(" ").map(word => (word, 1)))

// this also works
//var wordCountByRow = newTmp.map(x => sc.parallelize(x).reduceByKey(_+_))

//but the problem with the above two is that doing wordCOunt on a text stream is too big...

// SPARK-5063 prevents me from nesting RDDs and thus doing nested word count for each array

// so the only alternative I can think of is to do it in for loops

// sc.parallelize(sc.parallelize(tmp).take(2)(0).split(" ").map(word => (word, 1))).reduceByKey(_+_).collect()

var counter = 0
var arr = Array()
for (stringTemp <- tmp){
	// get an array of words sorted by usage in the string of words for that row
	arr = sc.parallelize(stringTemp.split(" ").map(word => (word, 1))).reduceByKey(_+_).sortBy(_._2, false).collect()
	//now filter out the stop words

	top5Words = arr.take(5)
	least5Words = arr.takeRight(5)
}

/*
id string,
dept_name string,
zone string,
time_of_day string
season string,
most_common_words string,
least_common_words string,
sentiment_score_sum bigint,
sentiment_score_total bigint
*/




var temp = sourceData.select("text").flatMap(el => el.getString(0).split(" ")).
map(_.replaceAll("[,.!?:;)( \t\n]", "").trim.toLowerCase).
filter(!_.isEmpty).filter(_.length > 2).
map(word => (word, 1)).
rdd.reduceByKey(_ + _).toDF

