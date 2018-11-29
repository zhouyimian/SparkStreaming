package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streamingwordcount extends App {

  //新建一个SparkConf变量，来提供Spark的配置
  val sparkConf = new SparkConf().setAppName("StreamingWordcount").setMaster("local[4]")

  //新建一个StreamingContext入口
  val ssc = new StreamingContext(sparkConf,Seconds(1))

  //mini1机器上的9999端口不断获取输入的文本数据
  val line = ssc.socketTextStream("mini1",9999)

  //将每一行进行分割
  val words = line.flatMap(_.split(","))

  val pairs = words.map((_,1))

  val result = pairs.reduceByKey(_+_)

  result.print()
  //启动程序
  ssc.start()

  ssc.awaitTermination()
}
