
case class SpeechAnalysis(
     id: String,
     dept_name: String,
     zone: String,
     time_of_day: String,
     date_of_event: String,
     most_common_words: String,
     least_common_words: String,
     sent_score: BigInt,
     sent_count: BigInt)


case class WebInput(
     id: String,
     dept_name: String,
     zone: String,
     date: String,
     time: String,
     duration: Double,
     text: String,
     recording: String
   )