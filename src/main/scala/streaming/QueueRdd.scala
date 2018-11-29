package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable



object QueueRdd {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("QueueRdd")

    val ssc = new StreamingContext(conf,Seconds(2))

    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]

    val inputStream = ssc.queueStream(rddQueue)

    val mappedStream = inputStream.map(x => (x%10,1))
    val reduceStream = mappedStream.reduceByKey(_+_)

    reduceStream.print()

    ssc.start()

    for(i <- 1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
