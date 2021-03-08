package com.bjvca.commonutils

import com.typesafe.config.ConfigFactory

class ConfUtils(confFile:String) extends Serializable {

  val load = ConfigFactory.load(confFile)

  // adx - streaming
  // spark
  val adxStreamingSparkMaster = load.getString("adx.streaming.spark.master")
  val adxStreamingSparkDuration = load.getLong("adx.streaming.spark.duration")

  // kafka
  val adxStreamingKafkaHost: String = load.getString("adx.streaming.kafka.host")
  val adxStreamingKafkaGroupid = load.getString("adx.streaming.kafka.groupid")

  // redis
  val adxStreamingRedisHost = load.getString("adx.streaming.redis.host")
  val adxStreamingRedisPassword = load.getString("adx.streaming.redis.password")
  val adxStreamingRedisDB = load.getInt("adx.streaming.redis.db")

  // es
  val adxStreamingEsHost = load.getString("adx.streaming.es.host")
  val adxStreamingEsUser = load.getString("adx.streaming.es.user")
  val adxStreamingEsPassword = load.getString("adx.streaming.es.password")
  val adxStreamingEsIndex = load.getString("adx.streaming.es.index")

  // mysql
  val adxStreamingMysqlHost = load.getString("adx.streaming.mysql.host")
  val adxStreamingMysqlUser = load.getString("adx.streaming.mysql.user")
  val adxStreamingMysqlPassword = load.getString("adx.streaming.mysql.password")

//  hdfs
  val adxBatchHDFSHost = load.getString("adx.batch.hdfs.host")

  // adseat
//  val adseatMysqlHost = load.getString("adseat.mysql.host")
//  val adseatMysqlUser = load.getString("adseat.mysql.user")
//  val adseatMysqlPassword = load.getString("adseat.mysql.password")

  // videocut
  val videocutMysqlHost = load.getString("videocut.mysql.host")
  val videocutMysqlUser = load.getString("videocut.mysql.user")
  val videocutMysqlPassword = load.getString("videocut.mysql.password")
  val videocutMysqlDb = load.getString("videocut.mysql.db")

  var nowTime = "0"

  // 权重
  val a: Int = load.getInt("a")
  val b: Int = load.getInt("b")
  val c: Int = load.getInt("c")
}