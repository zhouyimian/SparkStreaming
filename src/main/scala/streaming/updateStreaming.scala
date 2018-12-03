package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStreaming {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[2]").setAppName("updateStreaming")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("mini1",9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word,1))

    val updateFunc = (values: Seq[Int],state: Option[Int]) =>{
      val currentCount = values.foldLeft(0)(_+_)
      val previousCount = state.getOrElse(0)
      Some(currentCount+previousCount)
    }
    val stateDstream = pairs.updateStateByKey[Int](updateFunc);

    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
