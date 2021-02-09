package com.bjvca.filmedit

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import com.bjvca.bean.Film
import com.bjvca.commonutils.{ConfUtils, ConfigurationManager, DataSourceUtil, SqlProxy, TableRegister}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * 只有台词的卡点搜索,未测试
 */

object ocrPoint extends Logging {
  def main(args: Array[String]): Unit = {

    logWarning("Editing")

    val confUtil = new ConfUtils("application.conf")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Editing")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.debug.maxToStringFields", "2000")
      .set("es.mapping.date.rich", "false") //日期富类型
      // es
      .set("es.read.field.as.array.include", "true")
      .set("es.read.field.as.array.include", "string_class_img_list") //数组

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._

    // (单标签)读取mysql主表作为任务表
    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "clip_tpl"
      ))
      .load()
      .where("isPoint = 1")
      .where("status = 0")
      .where("seat_num = 1")
      .createOrReplaceTempView("clip_task_sig")

    // (多标签)读取mysql主表作为任务表
    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "clip_tpl"
      ))
      .load()
      .where("isPoint = 1")
      .where("status = 0")
      .where("seat_num != 1")
      .createOrReplaceTempView("clip_task_mul")


    spark.sql("cache table clip_task_sig")
    spark.sql("cache table clip_task_mul")


    //注册mysql的标签位信息
    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "clip_tpl_class", "clip_tpl_class")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_ocr", "recognition2_ocr")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "kukai_videos", "kukai_videos")

//    spark.sql(
//      """
//        |select *
//        |from clip_task_sig
//        |""".stripMargin).show(1000,false)

    val l1 = spark.sql(
      """
        |select *
        |from clip_task_sig
        |""".stripMargin)
      .count()
    if (l1 != 0) {

      println("单标签任务:" + l1)

      // 改变status为进行中
      spark.sql(
        """
          |select *
          |from clip_task_sig
          |""".stripMargin)
        .rdd
        .foreach(x => {
          val tpl_id = x.getInt(0)

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update `clip_tpl` set status=-2 where tpl_id = ?",
              Array(tpl_id))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }

        })

      spark.sql(
        """
          |select clip_task_sig.*,department
          |from clip_task_sig
          |join clip_tpl_class
          |on clip_task_sig.tpl_id=clip_tpl_class.tpl_id
          |""".stripMargin)
        //      .show(1000,false)
        .createOrReplaceTempView("target_sig")

      val searched = spark.sql(
        """
          |select tpl_id,
          |       videoName,
          |       concat_ws('_', lines_start, lines_end) string_time,
          |       lines_end - lines_start string_time_long,
          |       media_id string_vid,
          |       project_id,
          |       total_duration_min*1000 totalLong
          |from recognition2_ocr
          |join kukai_videos
          |on videoId = media_id
          |join target_sig
          |on recognition2_ocr.OCR_content rlike target_sig.ocr_arr
          |where department = department_id
          |""".stripMargin)
      //        .createOrReplaceTempView("rst")
      //        .show(1000,false)

      var last_tpl = 0
      var cur_timeLong = 0
      var pid = 1

      searched.coalesce(1)
        .map(row => {
          if (last_tpl == row.getInt(0) && cur_timeLong < row.getInt(6)) {
            cur_timeLong += row.getInt(3)
          } else {
            if (last_tpl != row.getInt(0)) {
              pid = 0
              last_tpl = row.getInt(0)
            } else {
              pid += 1
              cur_timeLong = row.getInt(3)
            }
          }
          Film(
            row.getInt(0),
            "label0",
            row.getString(1),
            null,
            row.getInt(3),
            row.getInt(6),
            row.getString(2),
            "1280*720",
            row.getString(5),
            row.getString(4),
            null,
            pid
          )
        })
        //      .show(1000,false)
        .createOrReplaceTempView("pid_ed")



      //group by (tpl_id,pid),求sum(string_time_long),where sum >= totalLong
      spark.sql(
        """
          |select tpl_id,ppid
          |from (
          |  select tpl_id,pid ppid,sum(string_time_long) sumLong,first(totalLong) totalLong
          |  from pid_ed
          |  group by tpl_id,pid) t1
          |where sumLong >= totalLong
          |""".stripMargin)
        .createOrReplaceTempView("pid_target")
      //          .show(1000, false)

      spark.sql("cache table pid_target")

      // pid_ed join pid_target
      val rst_sig = spark.sql(
        """
          |select pid_ed.tpl_id tpl_id,
          |       label_id,
          |       pid_ed.pid,
          |       media_name,
          |       resolution,
          |       string_class3_list,
          |       string_class_img_list,
          |       string_time,
          |       string_time_long,
          |       string_vid,
          |       project_id
          |from pid_ed
          |join pid_target
          |where pid_ed.tpl_id = pid_target.tpl_id
          |and pid_ed.pid = pid_target.ppid
          |""".stripMargin)
      rst_sig.createOrReplaceTempView("rst_sig")

      //              .show(1000,false)

      // 存入mysql
      rst_sig.foreachPartition(iterator => {

        var conn: Connection = null
        var ps: PreparedStatement = null

        try {
          conn = DriverManager.getConnection(ConfigurationManager.getProperty("jdbc.url"), "video_cut_user", "Slt_2020")
          conn.setAutoCommit(false)

          ps = conn.prepareStatement(
            """INSERT INTO clip_tpl_result(`id`,`tpl_id`,`label_id`,`pid`,`media_name`,`resolution`,`string_class3_list`,`string_class_img_list`,`string_time`,`string_time_long`,`string_vid`,`create_time`,`project_id`)
              |VALUES(null,?,?,?,?,?,?,?,?,?,?,null,?)
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
            ps.setString(11, it.getAs[String]("project_id"))

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

      // 得到num
      spark.sql(
        """
          |select tpl_id,count(tpl_id) num
          |from (
          | select tpl_id,pid
          | from rst_sig
          | group by tpl_id,pid) t1
          |group by tpl_id
          |""".stripMargin)
        .createOrReplaceTempView("numTab")
      //        .show(1000,false)

      //    rst_sig.groupBy("tpl_id").max("pid").select("tpl_id","max(pid)").createOrReplaceTempView("numTab")

      spark.sql(
        """
          |select *
          |from numTab
          |""".stripMargin)
        .toJSON
        .rdd
        .foreach(str => {
          val nObject = JSON.parseObject(str)
          val tpl_id = nObject.getString("tpl_id")
          val num = nObject.getString("num")

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update clip_tpl set num=? where tpl_id=?",
              Array(num, tpl_id))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })


      // 改变status为已完成
      spark.sql(
        """
          |select *
          |from clip_task_sig
          |""".stripMargin)
        .rdd
        .foreach(x => {
          val tpl_id = x.getInt(0)

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update `clip_tpl` set status=1 where tpl_id = ?",
              Array(tpl_id))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }

        })
    }


///////////////////////////////多标签////////////////////////////
    spark.sql(
      """
        |select clip_tpl_class.tpl_id tpl_id,
        |       label_id,
        |       class3_name arr,
        |       ocr,
        |       minT*1000 minT,
        |       maxT*1000 maxT,
        |       duration timeLong,
        |       total_duration totalLong,
        |       seat_num,
        |       department,
        |       videoName,
        |       category,
        |       area,
        |       year,
        |       classify,
        |       total_duration_min
        |from clip_tpl_class
        |join clip_task_mul
        |on clip_task_mul.tpl_id=clip_tpl_class.tpl_id
        |""".stripMargin)
//      .show(1000,false)
      .createOrReplaceTempView("target_mul")

    val l = spark.sql(
      """
        |select *
        |from target_mul
        |""".stripMargin)
      .count()

    if (l != 0) {

      println("多标签任务:" + l)

      // 改变status为进行中
      spark.sql(
        """
          |select *
          |from clip_task_mul
          |""".stripMargin)
        .rdd
        .foreach(x => {
          val tpl_id = x.getInt(0)

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update `clip_tpl` set status=-2 where tpl_id = ?",
              Array(tpl_id))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }

        })

      spark.sql(
        """
          |select tpl_id,
          |       label_id,
          |       kukai_videos.videoName media_name,
          |       lines_end - lines_start string_time_long,
          |       total_duration_min*1000 totalLong,
          |       concat_ws('_', lines_start, lines_end) string_time,
          |       project_id,
          |       media_id string_vid
          |from recognition2_ocr
          |join kukai_videos
          |on videoId = media_id
          |join target_mul
          |on recognition2_ocr.OCR_content rlike target_mul.ocr
          |""".stripMargin)
        .createOrReplaceTempView("ranked")
      //        .show(1100,false)


      spark.sql(
        """
          |select *,row_number() OVER (PARTITION BY tpl_id,label_id ORDER BY label_id DESC) rank
          |from ranked
          |""".stripMargin)
        //            .show(1000,false)
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
        //            .show(false)
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
      //            .show(1000,false)

      val rstDF = spark.sql(
        """
          |select tpl_id,
          |       label_id,
          |       rank pid,
          |       media_name,
          |       string_time,
          |       string_time_long,
          |       string_vid,
          |       shortSlab,
          |       totalLong,
          |       project_id
          |from shortSlabed
          |""".stripMargin)
      rstDF.createOrReplaceTempView("rst")
      //                .show(100,false)


      rstDF.foreachPartition(iterator => {

        var conn: Connection = null
        var ps: PreparedStatement = null

        try {
          conn = DriverManager.getConnection(ConfigurationManager.getProperty("jdbc.url"), "video_cut_user", "Slt_2020")
          conn.setAutoCommit(false)

          ps = conn.prepareStatement(
            """INSERT INTO clip_tpl_result(`id`,`tpl_id`,`label_id`,`pid`,`media_name`,`string_time`,`string_time_long`,`string_vid`,`project_id`)
              |VALUES(null,?,?,?,?,?,?,?,?)
              |""".stripMargin)
          var row = 0
          iterator.foreach(it => {
            ps.setInt(1, it.getAs[Int]("tpl_id"))
            ps.setString(2, it.getAs[String]("label_id"))
            ps.setInt(3, it.getAs[Int]("pid"))
            ps.setString(4, it.getAs[String]("media_name"))
            ps.setString(5, it.getAs[String]("string_time"))
            ps.setInt(6, it.getAs[Int]("string_time_long"))
            ps.setString(7, it.getAs[String]("string_vid"))
            ps.setString(8, it.getAs[String]("project_id"))

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
      spark.sql(
        """
          |select tpl_id,max(rst.pid) num
          |from rst
          |group by tpl_id
          |""".stripMargin)
        .createOrReplaceTempView("numTab")
      //          .show(100,false)


      spark.sql(
        """
          |select *
          |from numTab
          |""".stripMargin)
        .toJSON
        .rdd
        .foreach(str => {
          val nObject = JSON.parseObject(str)
          val tpl_id = nObject.getString("tpl_id")
          val num = nObject.getString("num")

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update clip_tpl set num=? where tpl_id=?",
              Array(num, tpl_id))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })



      spark.sql(
        """
          |select *
          |from clip_task_mul
          |""".stripMargin)
        .rdd
        .foreach(x => {
          val tpl_id = x.getInt(0)

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update `clip_tpl` set status=1 where tpl_id = ?",
              Array(tpl_id))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }

        })

    }

    spark.close()
    logWarning("End")

  }
}
