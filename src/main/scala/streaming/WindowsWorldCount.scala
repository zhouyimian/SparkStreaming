package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowsWorldCount {
  def main(args: Array[String]): Unit = {

    val updateFunc = (values: Seq[Int],state: Option[Int]) =>{
      val currentCount = values.foldLeft(0)(_+_)
      val previousCount = state.getOrElse(0)
      Some(currentCount+previousCount)
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("mini1",9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word,1))

    val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(12),Seconds(6))

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
