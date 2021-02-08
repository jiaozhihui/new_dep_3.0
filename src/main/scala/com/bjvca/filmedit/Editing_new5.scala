package com.bjvca.filmedit

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import com.bjvca.bean.Film
import com.bjvca.commonutils.{ConfUtils, ConfigurationManager, DataSourceUtil, SqlProxy, TableRegister}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * 卡点搜索,未测试
 */

object Editing_new5 extends Logging {
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
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_behavior", "recognition2_behavior")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_face", "recognition2_face")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_scene", "recognition2_scene")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_object", "recognition2_object")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_class", "recognition2_class")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "kukai_videos", "kukai_videos")


    val l1 = spark.sql(
      """
        |select *
        |from clip_task_sig
        |""".stripMargin)
      .count()
    if (l1 != 0) {
      println("单标签任务:" + l1)

      spark.sql(
        """
          |select recognition2_behavior.media_id string_vid,
          |       kukai_videos.albumId project_id,
          |       kukai_videos.department_id department_id,
          |       kukai_videos.videoName media_name,
          |       recognition2_behavior.time_start ad_seat_b_time,
          |       recognition2_behavior.time_end ad_seat_e_time,
          |       recognition2_behavior.time_end - recognition2_behavior.time_start string_time_long,
          |       concat_ws('_', recognition2_behavior.time_start, recognition2_behavior.time_end) string_time,
          |       concat_ws('*', videoWidth, videoHeight) resolution,
          |       frame,
          |       kukai_videos.category string_drama_name,
          |       kukai_videos.classify string_drama_type_name,
          |       kukai_videos.area string_media_area_name,
          |       kukai_videos.year year,
          |       recognition2_class.class1_name class2_name,
          |       recognition2_class.class_type class_type_id,
          |       recognition2_class.class2_name string_class3_list,
          |       recognition2_behavior.object_img string_class_img_list
          |from recognition2_behavior
          |join recognition2_class
          |    on recognition2_behavior.class_id = recognition2_class.class_id
          |join kukai_videos
          |on kukai_videos.videoId = media_id
          |union all
          |select recognition2_face.media_id string_vid,
          |       kukai_videos.albumId project_id,
          |       kukai_videos.department_id department_id,
          |       kukai_videos.videoName media_name,
          |       recognition2_face.time_start ad_seat_b_time,
          |       recognition2_face.time_end ad_seat_e_time,
          |       recognition2_face.time_end - recognition2_face.time_start string_time_long,
          |       concat_ws('_', recognition2_face.time_start, recognition2_face.time_end) string_time,
          |       concat_ws('*', videoWidth, videoHeight) resolution,
          |       frame,
          |       kukai_videos.category string_drama_name,
          |       kukai_videos.classify string_drama_type_name,
          |       kukai_videos.area string_media_area_name,
          |       kukai_videos.year year,
          |       recognition2_class.class1_name class2_name,
          |       recognition2_class.class_type class_type_id,
          |       recognition2_class.class2_name string_class3_list,
          |       recognition2_face.object_img string_class_img_list
          |from recognition2_face
          |join recognition2_class
          |    on recognition2_face.class_id = recognition2_class.class_id
          |join kukai_videos
          |on kukai_videos.videoId = media_id
          |union all
          |select recognition2_object.media_id string_vid,
          |       kukai_videos.albumId project_id,
          |       kukai_videos.department_id department_id,
          |       kukai_videos.videoName media_name,
          |       recognition2_object.time_start ad_seat_b_time,
          |       recognition2_object.time_end ad_seat_e_time,
          |       recognition2_object.time_end - recognition2_object.time_start string_time_long,
          |       concat_ws('_', recognition2_object.time_start, recognition2_object.time_end) string_time,
          |       concat_ws('*', videoWidth, videoHeight) resolution,
          |       frame,
          |       kukai_videos.category string_drama_name,
          |       kukai_videos.classify string_drama_type_name,
          |       kukai_videos.area string_media_area_name,
          |       kukai_videos.year year,
          |       recognition2_class.class1_name class2_name,
          |       recognition2_class.class_type class_type_id,
          |       recognition2_class.class2_name string_class3_list,
          |       recognition2_object.object_img string_class_img_list
          |from recognition2_object
          |join recognition2_class
          |    on recognition2_object.class_id = recognition2_class.class_id
          |join kukai_videos
          |on kukai_videos.videoId = media_id
          |union all
          |select recognition2_scene.media_id string_vid,
          |       kukai_videos.albumId project_id,
          |       kukai_videos.department_id department_id,
          |       kukai_videos.videoName media_name,
          |       recognition2_scene.time_start ad_seat_b_time,
          |       recognition2_scene.time_end ad_seat_e_time,
          |       recognition2_scene.time_end - recognition2_scene.time_start string_time_long,
          |       concat_ws('_', recognition2_scene.time_start, recognition2_scene.time_end) string_time,
          |       concat_ws('*', videoWidth, videoHeight) resolution,
          |       frame,
          |       kukai_videos.category string_drama_name,
          |       kukai_videos.classify string_drama_type_name,
          |       kukai_videos.area string_media_area_name,
          |       kukai_videos.year year,
          |       recognition2_class.class1_name class2_name,
          |       recognition2_class.class_type class_type_id,
          |       recognition2_class.class2_name string_class3_list,
          |       recognition2_scene.object_img string_class_img_list
          |from recognition2_scene
          |join recognition2_class
          |    on recognition2_scene.class_id = recognition2_class.class_id
          |join kukai_videos
          |on kukai_videos.videoId = media_id
          |""".stripMargin)
        .createOrReplaceTempView("video_wave")


      //    // 加载Es中所有片段信息
      //    TableRegister.registEsTable(spark, confUtil.adxStreamingEsHost, "9200",
      //      confUtil.adxStreamingEsUser, confUtil.adxStreamingEsPassword, confUtil.adxStreamingEsIndex, "video_wave")


      /**
       * 单标签位合成逻辑
       */
      spark.sql(
        """
          |select clip_tpl_class.tpl_id tpl_id,
          |       label_id,
          |       class3_name arr,
          |       ocr,
          |       minT*1000 minT,
          |       maxT*1000 maxT,
          |       total_duration_min totalLong,
          |       seat_num,
          |       department,
          |       videoName,
          |       category,
          |       area,
          |       year,
          |       classify
          |from clip_tpl_class
          |join clip_task_sig
          |on clip_task_sig.tpl_id=clip_tpl_class.tpl_id
          |""".stripMargin)
        .createOrReplaceTempView("target_sig")
      //              .show()

      var last_tpl = 0
      var cur_timeLong = 0
      var pid = 1

      // 搜索(根据限制条件关联mysql和ES),挑选时长符合的片段
      spark.sql(
        s"""
           |select t3.tpl_id,
           |       label_id,
           |       media_name,
           |       string_class3_list,
           |       ocr,
           |       string_time_long,
           |       totalLong,
           |       string_time,
           |       substr(string_time,1,instr(string_time,'_')-1) bT,
           |       substr(string_time,instr(string_time,'_')+1) eT,
           |       resolution,
           |       project_id,
           |       string_vid,
           |       substring(t3.string_class_img_list1,1,instr(t3.string_class_img_list1,'.png')+3) string_class_img_list,
           |       seat_num
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
           |             ocr,
           |             string_time_long,
           |             string_time,
           |             video_wave.resolution,
           |             video_wave.frame,
           |             totalLong*1000 totalLong,
           |             seat_num,
           |             cast(video_wave.string_class_img_list as string) string_class_img_list,
           |             project_id,
           |             minT,
           |             maxT
           |      from video_wave
           |      join target_sig
           |      on string_class3_list like CONCAT('%',arr,'%')
           |      and video_wave.media_name rlike target_sig.videoName
           |      and video_wave.string_media_area_name rlike target_sig.area
           |      and video_wave.string_drama_type_name rlike target_sig.classify
           |      and video_wave.string_drama_name rlike target_sig.category
           |      and video_wave.year rlike target_sig.year
           |      and department = department_id
           |      ) b
           |    where string_time_long >= minT
           |    and string_time_long <= maxT
           |    order by tpl_id,label_id) t2) t3
           |""".stripMargin)
        .createOrReplaceTempView("search1")
      //                    .show(1050,false)

      // 加台词过滤
      val searched = spark.sql(
        """
          |select search1.tpl_id,
          |       search1.label_id,
          |       search1.media_name,
          |       search1.string_class3_list,
          |       search1.string_time_long,
          |       search1.totalLong,
          |       search1.string_time,
          |       search1.resolution,
          |       search1.project_id,
          |       search1.string_vid,
          |       search1.string_class_img_list,
          |       search1.seat_num
          |from search1
          |join recognition2_ocr
          |on search1.string_vid = recognition2_ocr.media_id
          |and search1.project_id = recognition2_ocr.project_id
          |and search1.bT < recognition2_ocr.lines_start
          |and search1.eT > recognition2_ocr.lines_end
          |and OCR_content like CONCAT('%',ocr,'%')
          |group by tpl_id,label_id,media_name,string_class3_list,string_time_long,totalLong,string_time,resolution,search1.project_id,string_vid,string_class_img_list,seat_num
          |""".stripMargin)
      //      .createOrReplaceTempView("search")
      //        .show(1050,false)

      // 加pid
      searched.coalesce(1)
        .map(row => {
          if (last_tpl == row.getInt(0) && cur_timeLong < row.getInt(5)) {
            cur_timeLong += row.getInt(4)
          } else {
            if (last_tpl != row.getInt(0)) {
              pid = 0
              last_tpl = row.getInt(0)
            } else {
              pid += 1
              cur_timeLong = row.getInt(4)
            }
          }
          Film(
            row.getInt(0),
            row.getString(1),
            row.getString(2),
            row.get(3).toString.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
            row.getInt(4),
            row.getInt(5),
            row.getString(6),
            row.getString(7),
            row.getString(8),
            row.getString(9),
            row.getString(10),
            pid
          )
        })
        .createOrReplaceTempView("pid_ed")
      //          .show(1000,false)

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


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val l2 = spark.sql(
      """
        |select *
        |from clip_task_mul
        |""".stripMargin)
      .count()
    if (l2 != 0) {
      println("多标签任务:" + l2)

      /**
       * 多标签位合成逻辑
       */

      // 关联任务表与标签位信息表，得到将要查询的标签
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
          |       classify
          |from clip_tpl_class
          |join clip_task_mul
          |on clip_task_mul.tpl_id=clip_tpl_class.tpl_id
          |""".stripMargin)
        .createOrReplaceTempView("target_mul")
      //            .show()

      // 搜索(根据限制条件关联mysql和ES),挑选时长符合的片段
      spark.sql(
        s"""
           |select t3.tpl_id,
           |       label_id,
           |       string_vid,
           |       media_name,
           |       string_class3_list,
           |       ocr,
           |       string_time_long,
           |       string_time,
           |       substr(string_time,1,instr(string_time,'_')-1) bT,
           |       substr(string_time,instr(string_time,'_')+1) eT,
           |       resolution,
           |       frame,
           |       timeLong,
           |       totalLong,
           |       project_id,
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
           |             ocr,
           |             string_time_long,
           |             string_time,
           |             video_wave.resolution,
           |             video_wave.frame,
           |             timeLong*1000 timeLong,
           |             totalLong*1000 totalLong,
           |             seat_num,
           |             cast(video_wave.string_class_img_list as string) string_class_img_list,
           |             project_id,
           |             minT,
           |             maxT
           |      from video_wave
           |      join target_mul
           |      on string_class3_list=arr
           |      and video_wave.media_name rlike target_mul.videoName
           |      and video_wave.string_media_area_name rlike target_mul.area
           |      and video_wave.string_drama_type_name rlike target_mul.classify
           |      and video_wave.string_drama_name rlike target_mul.category
           |      and video_wave.year rlike target_mul.year
           |      where department = department_id
           |      ) b
           |    where string_time_long >= minT
           |    and string_time_long <= maxT
           |    order by tpl_id,label_id) t2) t3
           |""".stripMargin)
        .createOrReplaceTempView("search1")
      //              .show(1000,false)

      // 加台词过滤
      spark.sql(
        """
          |select search1.tpl_id,
          |       search1.label_id,
          |       search1.media_name,
          |       search1.string_class3_list,
          |       search1.string_time_long,
          |       search1.totalLong,
          |       search1.string_time,
          |       search1.resolution,
          |       search1.project_id,
          |       search1.string_vid,
          |       search1.string_class_img_list,
          |       search1.seat_num,
          |       search1.timeLong
          |from search1
          |join recognition2_ocr
          |on search1.string_vid = recognition2_ocr.media_id
          |and search1.project_id = recognition2_ocr.project_id
          |and search1.bT < recognition2_ocr.lines_start
          |and search1.eT > recognition2_ocr.lines_end
          |and OCR_content like CONCAT('%',ocr,'%')
          |group by tpl_id,label_id,media_name,string_class3_list,string_time_long,totalLong,string_time,resolution,search1.project_id,string_vid,string_class_img_list,seat_num,timeLong
          |""".stripMargin)
        .createOrReplaceTempView("ranked")
      //        .show(1050,false)

      spark.udf.register("arr_size", (s: scala.collection.mutable.WrappedArray[String]) => s.size)


      spark.sql(
        """
          |select * from (
          |select tpl_id,resolution,first(seat_num) seat_num,arr_size(collect_set(label_id)) count
          |from ranked
          |group by tpl_id,resolution)
          |where seat_num=count
          |""".stripMargin)
        //      .show(1000,false)
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
          |select *,row_number() OVER (PARTITION BY tpl_id,label_id,resolution ORDER BY label_id DESC) rank
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
          |       project_id,
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
          |          project_id,
          |          seat_num
          |   from shortSlabed) b
          |where seat_num != 1
          |or pid <= floor(shortSlab/floor(totalLong/timeLong)) and seat_num = 1
          |""".stripMargin)

      rstDF.createOrReplaceTempView("rst")
      //            .show(100,false)


      rstDF.foreachPartition(iterator => {

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
