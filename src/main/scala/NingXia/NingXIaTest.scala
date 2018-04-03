package NingXia

import java.util

import NingXia.kafka.SparkSessionSingleton
import NingXia.kudu.NingXiaTest.{avgSpeed, maxSpeed}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Properties

/**
  * Created by toby on 2017/11/15.
  * 用于测试宁夏POC KAFKA-SPARK-KUDU 整体流程
  */
object NingXiaTest {
  def main(args: Array[String]): Unit = {

    //    if(args.length<1){
    //      println("Lack of configuration files")
    //      System.exit(0)
    //    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("kafka test")
      .setMaster("local[6]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> " 172.16.40.14:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group"
    )
    val file:String = "D:\\software\\IDEA\\project\\spark210\\src\\main\\scala\\NingXia\\config.properties"
//    val pro = profile.getpro(args(0))
//    val value = profile.getvalue(args(0))
//    val set = profile.gettopics(args(0))
    val pro = profile.getpro(file)
    val value = profile.getvalue(file)
    val set = profile.gettopics(file)

    import scala.collection.JavaConversions._
    var topics: Set[String] = Set()
    for (s <- set) {
      topics += s
    }

    println(topics)

    val lines: DStream[(String, String, Double)] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map { record => (record.key, record.value()) }
      .map(_._2.split(","))
      .map { x: Array[String] =>
        var id = x(1)
        var index = value.get(id)
        var field = Integer.parseInt(index.values().toArray()(0).toString)
        println(id + " " + index + " " + field)
        (x(1), "2018-01-01 01:01:01", x(field).toDouble)
      }


    lines.foreachRDD {
      rdd: RDD[(String, String, Double)] => {
        avgSpeed(rdd)
        maxSpeed(rdd)
      }
    }
    ssc.start
    ssc.awaitTermination()
  }

  def avgSpeed(rdd: RDD[(String, String, Double)]) = {
    val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    import spark.implicits._
    val sc = rdd.sparkContext
    val rdd1 = rdd.map(x => (x._2, x._3))
    val updateFrame = rdd1.toDF("time", "speed1")
    updateFrame.registerTempTable("temp")
    val df = spark.sql("select time,'speedID' id,avg(speed1) speed_avg from temp group by time")
    val kuduContext = new KuduContext("172.16.40.14:7051", sc)
    kuduContext.upsertRows(df, "impala::default.avgSpeed")
    import org.apache.kudu.spark.kudu._
    val df2 = spark.read.options(Map("kudu.master" -> "172.16.40.14:7051", "kudu.table" -> "impala::default.avgSpeed")).kudu
    df2.show
  }

  def maxSpeed(rdd: RDD[(String, String, Double)]) = {
    val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    import spark.implicits._
    val sc = rdd.sparkContext
    val rdd1 = rdd.map(x => (x._2, x._3))
    val updateFrame = rdd1.toDF("time", "speed1")
    updateFrame.registerTempTable("temp")
    val df = spark.sql("select time,'speedID' id,max(speed1) speed_max from temp group by time")
    val kuduContext = new KuduContext("172.16.40.14:7051", sc)
    kuduContext.upsertRows(df, "impala::default.maxSpeed")
    import org.apache.kudu.spark.kudu._
    val df2 = spark.read.options(Map("kudu.master" -> "172.16.40.14:7051", "kudu.table" -> "impala::default.maxSpeed")).kudu
    df2.show
  }
}

object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
