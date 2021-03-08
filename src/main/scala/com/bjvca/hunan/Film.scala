package com.bjvca.hunan

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bjvca.bean.{Story1, Story2, Story3}
import com.bjvca.commonutils.{ConfUtils, DataSourceUtil, SqlProxy}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import org.elasticsearch.spark._

import scala.collection.JavaConverters
import scala.collection.mutable.ListBuffer


/**
 * 分镜头
 * 加入OCR判断逻辑
 */
object Film extends Logging {

  def main(args: Array[String]): Unit = {


    logWarning("Demo开始运行")


    val confUtil = new ConfUtils("application.conf")

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .config("es.net.proxy.http.use.system.props", "false")
      .config("spark.debug.maxToStringFields", "200")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val taskJson = sc.collectionAccumulator[String]("taskJson")

    // 0.读取任务表
    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "scenario_task"
      ))
      .load()
      .createOrReplaceTempView("scenario_task")

    spark.sql("cache table scenario_task")


    // 1.读取需要用到的表
          spark.read.format("jdbc")
            .options(Map(
              "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
              "driver" -> "com.mysql.jdbc.Driver",
              "user" -> confUtil.videocutMysqlUser,
              "password" -> confUtil.videocutMysqlPassword,
              "dbtable" -> "recognition2_ocr"
            ))
            .load()
            .createOrReplaceTempView("recognition2_ocr")

    spark.sql("cache table recognition2_ocr")


    spark.sql(
      """
        |select *
        |from scenario_task
        |""".stripMargin)
      .toJSON
      .rdd
      .foreach(json => {
        taskJson.add(json)
      })

    val taskList = taskJson.value

    for (i <- 0 until taskList.size) {
      val nObject = JSON.parseObject(taskList.get(i))

      val id = nObject.getInteger("id")
      val first = nObject.getString("word1")
      val second = nObject.getString("word2")
      val third = nObject.getString("word3")
      val end_first = nObject.getString("1word")
      val end_second = nObject.getString("2word")
      val end_third = nObject.getString("3word")


      spark.sql(
        s"""
           |select *
           |from recognition2_ocr
           |where OCR_content like '%$first%'
           |""".stripMargin)
        .createOrReplaceTempView("first")
//              .show(1000,false)

      spark.sql(
        s"""
           |select *
           |from recognition2_ocr
           |where OCR_content like '%$second%'
           |""".stripMargin)
        .createOrReplaceTempView("second")
      //      .show(1000,false)

      spark.sql(
        s"""
           |select *
           |from recognition2_ocr
           |where OCR_content like '%$third%'
           |""".stripMargin)
        .createOrReplaceTempView("third")
      //          .show(1000,false)

      spark.sql(
        """
          |select first.media_id,first.lines_start,third.lines_end,first.OCR_content,second.OCR_content,third.OCR_content
          |from first
          |full join second
          |full join third
          |on first.media_id = second.media_id
          |and first.media_id = third.media_id
          |where second.lines_start - first.lines_end < 30000
          |and second.lines_start - first.lines_start >= 0
          |and third.lines_start - second.lines_end < 30000
          |and third.lines_start - second.lines_start >= 0
          |""".stripMargin)
//        .limit(1)
        .createOrReplaceTempView("head")
//              .show(1000,false)


      spark.sql(
        s"""
           |select *
           |from recognition2_ocr
           |where OCR_content like '%$end_first%'
           |""".stripMargin)
        .createOrReplaceTempView("end_first")
//              .show(1000,false)

      spark.sql(
        s"""
           |select *
           |from recognition2_ocr
           |where OCR_content like '%$end_second%'
           |""".stripMargin)
        .createOrReplaceTempView("end_second")
//            .show(1000,false)

      spark.sql(
        s"""
           |select *
           |from recognition2_ocr
           |where OCR_content like '%$end_third%'
           |""".stripMargin)
        .createOrReplaceTempView("end_third")
//                .show(1000,false)

      spark.sql(
        """
          |select end_first.media_id,end_first.lines_start,end_third.lines_end,end_first.OCR_content,end_second.OCR_content,end_third.OCR_content
          |from end_first
          |full join end_second
          |full join end_third
          |on end_first.media_id = end_second.media_id
          |and end_first.media_id = end_third.media_id
          |where end_second.lines_start - end_first.lines_end < 30000
          |and end_second.lines_start - end_first.lines_start >= 0
          |and end_third.lines_start - end_second.lines_end < 30000
          |and end_third.lines_start - end_second.lines_start >= 0
          |""".stripMargin)
//        .limit(1)
        .createOrReplaceTempView("tail")
//            .show(1000,false)

      spark.sql(
        s"""
          |select $id,head.media_id,head.lines_start,tail.lines_end,
          |concat_ws(':',cast(cast(head.lines_start/1000 as int)/60 as int),cast(head.lines_start/1000 as int)%60) start_time,
          |concat_ws(':',floor(tail.lines_end/1000/60),floor((tail.lines_end/1000/60-floor(tail.lines_end/1000/60))*60)) end_time
          |from head
          |join tail
          |on head.media_id = tail.media_id
          |""".stripMargin)
        .limit(1)
        .show(1000,false)
//        .foreach(x => {
//        val id = x.getInt(0)
//        val media_id = x.getString(1)
//        val lines_start = x.getInt(2)
//        val lines_end = x.getInt(3)
//
//        val sqlProxy = new SqlProxy()
//        val client = DataSourceUtil.getConnection
//        try {
//          sqlProxy.executeUpdate(client, "insert `scenario_result` set id=?,media_id=?,lines_start=?,lines_end=?",
//            Array(id, media_id, lines_start,lines_end))
//        }
//        catch {
//          case e: Exception => e.printStackTrace()
//        } finally {
//          sqlProxy.shutdown(client)
//        }
//
//      })

    }

    spark.close()


  }

}
