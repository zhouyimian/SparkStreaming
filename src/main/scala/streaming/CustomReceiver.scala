package streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

//Receiver需要提供一个类型参数，该类型参数是Receiver接受到的数据类型
class CustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

  //Receiver启动时需要调用的方法
  override def onStart(): Unit = {
    //定义一个新的线程去运行receive方法
    new Thread("customerThread"){
      override def run(): Unit = {receive()}
    }.start()
  }
  //receive方法
  def receive():Unit = {
    //新建socket连接
    var socket = new Socket(host,port)

    //获取socket的输入
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8))
    //获取第一条输入
    var lines = reader.readLine()
    //如果receive没有停止而且lines不是null
    while(!isStopped()&&lines!=null){
      //通过store方法将获取到的lines提交给Spark框架
      store(lines)
      lines = reader.readLine()
    }
    reader.close()
    socket.close()
  }
  //Receiver停止时需要调用的方法
  override def onStop(): Unit = {

  }
}
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    //新建一个SparkConf变量，来提供Spark的配置
    val sparkConf = new SparkConf().setAppName("CustomerStreamingWordcount").setMaster("local[4]")

    //新建一个StreamingContext入口
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //mini1机器上的9999端口不断获取输入的文本数据
    val line = ssc.receiverStream(new CustomReceiver("mini1",9999))

    //将每一行进行分割
    val words = line.flatMap(_.split(" "))

    val pairs = words.map((_,1))

    val result = pairs.reduceByKey(_+_)

    result.print()
    //启动程序
    ssc.start()

    ssc.awaitTermination()
  }
}
