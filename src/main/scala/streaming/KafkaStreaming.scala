package streaming

import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wuyufei on 06/09/2017.
  */


object createKafkaProducerPool{

  def apply(brokerList: String, topic: String):  GenericObjectPool[KafkaProducerProxy] = {
    val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
  }
}

object KafkaStreaming{



  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    //创建topic
    val brobrokers = "192.168.145.128:9092,192.168.145.129:9092"
    val sourcetopic="source";
    val targettopic="target1";

    //创建消费者组
    var group="con-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );



    //ssc.sparkContext.broadcast(pool)

    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))


    //每一个stream都是一个ConsumerRecord
    stream.map(s =>("id:" + s.key(),">>>>:"+s.value())).foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        // Get a producer from the shared pool
        val pool = createKafkaProducerPool(brobrokers, targettopic)
        val p = pool.borrowObject()

        partitionOfRecords.foreach {message => System.out.println(message._2);p.send(message._2,Option(targettopic))}

        // Returning the producer to the pool also shuts it down
        pool.returnObject(p)

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
