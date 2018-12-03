package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SavefileWordCount {
  def main(args: Array[String]): Unit = {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      //计算当前批次相同key的单词总数
      val currentCount = values.foldLeft(0)(_ + _)
      //获取上一次保存的单词总数
      val previousCount = state.getOrElse(0)
      //返回新的单词总数
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("SvafileWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("mini1", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = pairs.updateStateByKey[Int](updateFunc)

    stateDstream.saveAsTextFiles("statful")


    ssc.start()
    ssc.awaitTermination()

  }
}
