package com.bjvca.filmedit

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.bjvca.commonutils.{ConfUtils, DataSourceUtil, SqlProxy, TableRegister}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Editing_new extends Logging {
  def main(args: Array[String]): Unit = {

//    while (true) {

      logWarning("Editing")

      val confUtil = new ConfUtils("application.conf")

      val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Editing")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.debug.maxToStringFields", "200")
        .set("es.mapping.date.rich", "false") //日期富类型
        // es
        .set("es.read.field.as.array.include", "true")
        .set("es.read.field.as.array.include", "string_class3_list,string_class_img_list") //数组

      val spark = SparkSession.builder.config(conf).getOrCreate()

      // 读取mysql主表作为任务表
      spark.read.format("jdbc")
        .options(Map(
          "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
          "driver" -> "com.mysql.jdbc.Driver",
          "user" -> confUtil.videocutMysqlUser,
          "password" -> confUtil.videocutMysqlPassword,
          "dbtable" -> "clip_tpl"
        ))
        .load()
        .where("status = 0")
        .createOrReplaceTempView("clip_task")

      spark.sql("cache table clip_task")

      //注册mysql的标签位信息
      TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
        confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "clip_tpl_class", "clip_tpl_class")

      // 加载Es中所有片段信息
      TableRegister.registEsTable(spark, confUtil.adxStreamingEsHost, "9200",
        confUtil.adxStreamingEsUser, confUtil.adxStreamingEsPassword, confUtil.adxStreamingEsIndex, "video_wave")


      // 关联任务表与标签位信息表，得到将要查询的标签
      val temp = spark.sql(
        """
          |select clip_tpl_class.tpl_id tpl_id,
          |       label_id,
          |       split(class3_name,',') as arr,
          |       duration timeLong,
          |       total_duration totalLong,
          |       seat_num
          |from clip_tpl_class
          |join clip_task
          |on clip_task.tpl_id=clip_tpl_class.tpl_id
          |""".stripMargin)
      temp.createOrReplaceTempView("target")
//            .show()

      // 搜索(根据限制条件关联mysql和ES),挑选时长符合的片段
      spark.sql(
        s"""
           |select t3.tpl_id,
           |       label_id,
           |       string_vid,
           |       media_name,
           |       string_class3_list,
           |       string_time_long,
           |       string_time,
           |       resolution,
           |       frame,
           |       timeLong,
           |       totalLong,
           |       seat_num,
           |       substring(t3.string_class_img_list1,1,instr(t3.string_class_img_list1,'.png')+3) string_class_img_list
           |from (
           |  select *,tpl_id,substring(string_class_img_list,2,length(t2.string_class_img_list)-2) string_class_img_list1
           |  from (
           |    select *
           |    from (
           |      select tpl_id,
           |             label_id,
           |             string_vid,
           |             media_name,
           |             arr string_class3_list,
           |             string_time_long,
           |             string_time,
           |             video_wave.resolution,
           |             video_wave.frame,
           |             timeLong*1000 timeLong,
           |             totalLong*1000 totalLong,
           |             seat_num,
           |             cast(video_wave.string_class_img_list as string) string_class_img_list
           |      from video_wave
           |      join target
           |      on array_intersect(string_class3_list,arr)=arr
           |      ) b
           |    where string_time_long >= timeLong-1000
           |    and string_time_long <= timeLong+1000
           |    order by tpl_id,label_id) t2) t3
           |""".stripMargin)
        .createOrReplaceTempView("ranked")
//              .show(1000,false)

    spark.udf.register("arr_size",(s:scala.collection.mutable.WrappedArray[String])=>s.size)


    spark.sql(
      """
        |select * from (
        |select tpl_id,resolution,first(seat_num) seat_num,arr_size(collect_set(label_id)) count
        |from ranked
        |group by tpl_id,resolution)
        |where seat_num=count
        |""".stripMargin)
        .createOrReplaceTempView("resolution_target")

    spark.sql(
      """
        |select ranked.*,count
        |from ranked
        |join resolution_target
        |on resolution_target.tpl_id=ranked.tpl_id
        |and resolution_target.resolution=ranked.resolution
        |order by tpl_id,label_id
        |""".stripMargin)
        .createOrReplaceTempView("ranked1")
//      .show(1000,false)


    spark.sql(
      """
        |select *,row_number() OVER (PARTITION BY tpl_id,label_id ORDER BY label_id DESC) rank
        |from ranked1
        |""".stripMargin)
//      .show(1000,false)
        .createOrReplaceTempView("resolution_ranked")

    spark.sql(
      """
        |select tpl_id,min(max) shortSlab
        |from (
        |   select tpl_id,label_id,max(rank) max
        |   from resolution_ranked
        |   group by tpl_id,label_id) t1
        |group by tpl_id
        |""".stripMargin)
//      .show(false)
        .createOrReplaceTempView("shortSlab")

    spark.sql(
      """
        |select resolution_ranked.*,shortSlab
        |from resolution_ranked
        |join shortSlab
        |on resolution_ranked.tpl_id = shortSlab.tpl_id
        |where rank <= shortSlab
        |""".stripMargin)
        .createOrReplaceTempView("shortSlabed")
//        .show(1000,false)


    val rstDF = spark.sql(
      """
        |select tpl_id,
        |       label_id,
        |       cast(pid as int),
        |       media_name,
        |       resolution,
        |       concat_ws('/',string_class3_list) string_class3_list,
        |       string_class_img_list,
        |       string_time,
        |       string_time_long,
        |       string_vid,
        |       rank
        |from (
        |   select tpl_id,
        |          label_id,
        |          if(seat_num=1,floor(rank/floor(totalLong/timeLong)+0.99),rank) pid,
        |          media_name,
        |          resolution,
        |          concat_ws('/',string_class3_list) string_class3_list,
        |          string_class_img_list,
        |          string_time,
        |          string_time_long,
        |          string_vid,
        |          rank,
        |          shortSlab,
        |          totalLong,
        |          timeLong,
        |          seat_num
        |   from shortSlabed) b
        |where seat_num != 1
        |or pid <= floor(shortSlab/floor(totalLong/timeLong)) and seat_num = 1
        |""".stripMargin)

//        .show(100,false)
//
        rstDF.foreachPartition(iterator => {

        var conn: Connection = null
        var ps: PreparedStatement = null

        try {
          conn = DriverManager.getConnection("jdbc:mysql://vcasltdb.mysql.rds.aliyuncs.com:3306/video_wave?serverTimezone=GMT%2B8", "video_cut_user", "Slt_2020")
          conn.setAutoCommit(false)

          ps = conn.prepareStatement(
            """INSERT INTO clip_tpl_result(`id`,`tpl_id`,`label_id`,`pid`,`media_name`,`resolution`,`string_class3_list`,`string_class_img_list`,`string_time`,`string_time_long`,`string_vid`,`create_time`)
              |VALUES(null,?,?,?,?,?,?,?,?,?,?,null)
              |""".stripMargin)
          var row = 0
          iterator.foreach(it => {
            ps.setInt(1, it.getAs[Int]("tpl_id"))
            ps.setString(2, it.getAs[String]("label_id"))
            ps.setInt(3, it.getAs[Int]("pid"))
            ps.setString(4, it.getAs[String]("media_name"))
            ps.setString(5, it.getAs[String]("resolution"))
            ps.setString(6, it.getAs[String]("string_class3_list"))
            ps.setString(7, it.getAs[String]("string_class_img_list"))
            ps.setString(8, it.getAs[String]("string_time"))
            ps.setInt(9, it.getAs[Int]("string_time_long"))
            ps.setString(10, it.getAs[String]("string_vid"))

            ps.addBatch()
            row = row + 1
            if (row % 1000 == 0) {
              ps.executeBatch()
              row = 0
            }
          })
          if (row > 0)
            ps.executeBatch()
          conn.commit()

        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })

    rstDF.rdd.groupBy(row => {
//      val str = row.mkString
//      val i = str.indexOf("tpl_id")
//      val tpl_id = str.substring(i + 9, i + 10)
      val tpl_id = row.getInt(0)
      tpl_id
      //        JSON.parseObject(str).getString("tpl_id")
    })

      .foreach(x => {
        val tpl_id = x._1
        val num = x._2.toList.size

        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          sqlProxy.executeUpdate(client, "update `clip_tpl` set num=?,status=1 where tpl_id = ?",
            Array(num, tpl_id))
        }
        catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }

      })


      spark.close()
      logWarning("End")

//    }

  }
}
