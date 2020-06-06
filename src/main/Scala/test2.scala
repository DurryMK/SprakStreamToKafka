import com.google.common.eventbus.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.guava.eventbus.Subscribe


object test2  extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
  val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(2))

  ssc.checkpoint("./chpoint")   //以后是一个hdfs 对于窗口和有状态的操作必须checkpoint，通过StreamingContext的checkpoint来指定目录，

  //当前是consumer端，要反序列化消息
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "node4:9092,node4:9093,node4:9094",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "streaming74",    //消费者组编号
    "auto.offset.reset" -> "latest",       //消息从哪里开始读取  latest 从头
    "enable.auto.commit" -> (true: java.lang.Boolean)   //消息的位移提交方式
  )
  //要订阅的主题
  val topics = Array("topicAAA")
  //创建DStream
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)    //     SubscribePattern:主题名由正则表示    , Subscribe:主题名固定   Assign:固定分区
  )
  //    ConsumerRecord
  val lines:DStream[String]=stream.map(record => (   record.value)  )
  val words:DStream[String]=lines.flatMap(   _.split(" "))
  val wordAndOne:DStream[(String,Int)]=words.map(   (_,1) )

  //val reduced:DStream[  (String,Int) ]=wordAndOne.reduceByKey( _+_)
  val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

  result.print()
  ssc.start()
  ssc.awaitTermination()

}

