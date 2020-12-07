import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, CountVectorizer, RegexTokenizer, StopWordsRemover, IDF}

import scala.math.pow
import scala.collection.mutable


//inspired by this source: https://classes.ischool.syr.edu/ist718/content/unit09/lab-sentiment_analysis/

val text = sc.textFile("hdfs:///tmp/dasnes-final-project/sample-data/test1-dasnes.txt")

val ss = SparkSession.builder.getOrCreate()

val sentiments_df = ss.read.parquet("hdfs:///tmp/dasnes-final-project/sample-data/sentiments.parquet")
val imdb_df = ss.read.parquet("hdfs:///tmp/dasnes-final-project/sample-data/imdb_reviews_preprocessed.parquet")

// RegexTokenizer extracts a sequence of matches from the input text.
val rTok = new RegexTokenizer().setGaps(false).setPattern("\\p{L}+").setInputCol("review").setOutputCol("words")

val reviewWordsDf = rTok.transform(imdb_df)

// ----------------
// skipping to the more accurate version

//get stop words as an array
val stop_words = sc.textFile("hdfs:///tmp/dasnes-final-project/sample-data/stop_words").collect().toArray

//stop_word filter
val sw_filter = new StopWordsRemover().setStopWords(stop_words).setCaseSensitive(false).setInputCol("words").setOutputCol("filtered")

// remove words that appear in 5 docs or less
val cv = new CountVectorizer().setInputCol("filtered").setOutputCol("tf").setVocabSize(scala.math.pow(2, 17).toInt).setMinDF(5).setMinTF(1)

val cv_pipeline = new Pipeline().setStages(Array(rTok, sw_filter, cv)).fit(imdb_df)

val idf = new IDF().setInputCol("tf").setOutputCol("tfidf")

val idf_pipeline = new Pipeline().setStages(Array(cv_pipeline, idf)).fit(imdb_df)

val tfidf_df = idf_pipeline.transform(imdb_df)


// split data into sets for training

val sets = imdb_df.randomSplit(Array[Double](0.6, 0.3, 0.1), 0)
val training_df = sets(0)
val validation_df = sets(1)
val testing_df = sets(2)

val lr = new LogisticRegression().
     setLabelCol("score").
     setFeaturesCol("tfidf").
     setRegParam(0.0).
     setMaxIter(10).
     setElasticNetParam(0)


val lr_pipeline = new Pipeline().setStages(Array(idf_pipeline, lr)).fit(training_df)

//regularization section + grid search for best model
val lambda_par = 0.02
val alpha_par = 0.3
val en_lr = new LogisticRegression().
     setLabelCol("score").
     setFeaturesCol("tfidf").
     setMaxIter(10).
     setElasticNetParam(alpha_par)

val en_lr_pipeline = new Pipeline().setStages(Array(idf_pipeline, en_lr)).fit(training_df)  

val en_lr_estimator = new Pipeline().setStages(Array(idf_pipeline, en_lr))

val grid = new ParamGridBuilder().
     addGrid(en_lr.regParam, Array(0.0, 0.01, 0.02)).
     addGrid(en_lr.elasticNetParam, Array(0.0, 0.2, 0.4)).
     build()


val all_models = scala.collection.mutable.MutableList[org.apache.spark.ml.PipelineModel]()
for (x <- grid) {
       all_models +=  en_lr_estimator.fit(training_df, x)
     }

val accuracies = scala.collection.mutable.MutableList[Double]()
for (m <- all_models) {
       accuracies +=  m.transform(validation_df).select(avg(expr("float(score = prediction)")).alias("accuracy")).first().getDouble(0)
     }

var arg_max_val = 0.0
for (a <- accuracies) {
       if (a > arg_max_val) {
         arg_max_val = a
       }
    }

val arg_max_idx = accuracies.indexOf(arg_max_val)
val best_model = all_models(arg_max_idx)

//best_model.save("pipelineModel.model")
best_model.save("hdfs:///tmp/dasnes-final-project/sample-data/models/")

// can load the model with PipelineModel.load("hdfs...")

//save best model to disk

//val text_df = text.toDF("text")
//val inferred = best_model.transform(dfA.withColumnRenamed("text", "review")).select("review", "prediction")

// DONE WITH INFERENCE


// APPEND ROW TO HBASE TABLE

