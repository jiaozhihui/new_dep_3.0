package com.bjvca.storycut

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bjvca.bean.{Story1, Story2, Story3}
import com.bjvca.commonutils.{ConfUtils, DataSourceUtil, SqlProxy}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

import scala.collection.JavaConverters


/**
 * 分镜头
 * 加入OCR判断逻辑
 */
object SubCut3 extends Logging {

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

    // 0.读取任务表
          spark.read.format("jdbc")
            .options(Map(
              "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
              "driver" -> "com.mysql.jdbc.Driver",
              "user" -> confUtil.videocutMysqlUser,
              "password" -> confUtil.videocutMysqlPassword,
              "dbtable" -> "task"
            ))
            .load()
            .where("status = 0")
            .createOrReplaceTempView("task")

    spark.sql("cache table task")

    // 读取将要用到的表
    // 1.recognition2_behavior
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_behavior"
      ))
      .load()
      .createOrReplaceTempView("recognition2_behavior")
    // 2.recognition2_face
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_face"
      ))
      .load()
      .createOrReplaceTempView("recognition2_face")
    // 3.recognition2_object
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_object"
      ))
      .load()
      .createOrReplaceTempView("recognition2_object")
    // 4.recognition2_scene
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_scene"
      ))
      .load()
      .createOrReplaceTempView("recognition2_scene")
    // 5.class
    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_class"
      ))
      .load()
      .createOrReplaceTempView("recognition2_class")

    // 6.kukai_videos
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "kukai_videos"
      ))
      .load()
      .createOrReplaceTempView("kukai_videos")

    // 7.videostory
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_videostory"
      ))
      .load()
      .createOrReplaceTempView("recognition2_videostory")

    // 8.OCR
    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "recognition2_ocr"
      ))
      .load()
      .createOrReplaceTempView("recognition2_ocr")


    // lastStory:存储上一个片段
    var lastStory1 = Story1("", "", "", 0, 0, List(0), "")
    var lastStory2 = Story2("", "", "", 0, 1, 0, "")
    var lastStory3 = Story2("", "", "", 0, 1, 0, "")
    var lastStory4 = Story3("", "", "", 0, 1, "", 0, 0)

    // storyList:存储最终合成出来的分镜头
    val storyList1 = sc.collectionAccumulator[Story1]("rst1")
    val storyList2 = sc.collectionAccumulator[Story2]("rst2")
    val storyList3 = sc.collectionAccumulator[Story2]("rst3")
    val storyList4 = sc.collectionAccumulator[Story3]("rst4")

    // 获取每个片段后三秒的标签
    spark.sql(
      """
        |select first(t1.platform_id) platform_id,first(t1.project_id) project_id,t1.media_id media_id,story_start,story_end,collect_set(class_id) class_id,first(image) image
        |from
        |(select recognition2_videostory.platform_id platform_id,recognition2_videostory.project_id project_id,recognition2_videostory.media_id media_id,story_start,story_end,class_id,time_start,time_end,image
        |from recognition2_videostory
        |left join recognition2_behavior
        |on recognition2_behavior.media_id=recognition2_videostory.media_id
        |and time_start > story_start
        |and time_end < story_end
        |and time_start < story_end - 3000
        |union all
        |select recognition2_videostory.platform_id platform_id,recognition2_videostory.project_id project_id,recognition2_videostory.media_id media_id,story_start,story_end,class_id,time_start,time_end,image
        |from recognition2_videostory
        |left join recognition2_object
        |on recognition2_object.media_id=recognition2_videostory.media_id
        |and time_start > story_start
        |and time_end < story_end
        |and time_start < story_end - 3000
        |union all
        |select recognition2_videostory.platform_id platform_id,recognition2_videostory.project_id project_id,recognition2_videostory.media_id media_id,story_start,story_end,class_id,time_start,time_end,image
        |from recognition2_videostory
        |left join recognition2_face
        |on recognition2_face.media_id=recognition2_videostory.media_id
        |and time_start > story_start
        |and time_end < story_end
        |and time_start < story_end - 3000
        |union all
        |select recognition2_videostory.platform_id platform_id,recognition2_videostory.project_id project_id,recognition2_videostory.media_id media_id,story_start,story_end,class_id,time_start,time_end,image
        |from recognition2_videostory
        |left join recognition2_scene
        |on recognition2_scene.media_id=recognition2_videostory.media_id
        |and time_start > story_start
        |and time_end < story_end
        |and time_start < story_end - 3000) t1
        |join task
        |on task.video_id = t1.media_id
        |group by media_id, story_start, story_end
        |order by media_id,story_start
        |""".stripMargin)
//                          .show(1000,false)

      .rdd
      // 封装样例类Story
      .map(row => Story1(
        row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getInt(3),
        row.getInt(4),
        row.getAs[Seq[Int]](5).toList,
        row.getString(6)
      ))
      .coalesce(1)

      /**
       * 核心处理逻辑
       * thisStory.class_id
       */
      .foreach(thisStory => {
        if (thisStory.class_id.intersect(lastStory1.class_id).nonEmpty && thisStory.media_id == lastStory1.media_id) {
          // 有交集
          lastStory1 = lastStory1.copy(story_end = thisStory.story_end, class_id = thisStory.class_id)
        } else {
          // 无交集
          storyList1.add(lastStory1)
          lastStory1 = thisStory
        }
      })

    val value1 = storyList1.value

    // 第一步完成
    val firstDF = JavaConverters.asScalaIteratorConverter(value1.iterator).asScala.toSeq.toDS.filter(_.story_end != 0).select("platform_id", "project_id", "media_id", "story_start", "story_end", "image")
//        firstDF.show(1000, truncate = false)


    firstDF.createOrReplaceTempView("firstDF")

    spark.sql(
      """
        |select *, story_end - story_start timeLong
        |from firstDF
        |""".stripMargin)
//              .show(1000,false)
      .rdd
      .map(row => Story2(
        row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getInt(3),
        row.getInt(4),
        row.getInt(6),
        row.getString(5)
      ))
      .coalesce(1)
      .foreach(thisStory => {
        if (thisStory.timeLong < 20000 ) {
          if (lastStory2.story_end == thisStory.story_start) {
            lastStory2 = lastStory2.copy(media_id = thisStory.media_id, story_end = thisStory.story_end, timeLong = lastStory2.timeLong + thisStory.timeLong)
          } else {
            lastStory2 = thisStory
          }
        } else {
          if (lastStory2.story_end == thisStory.story_start) {
            storyList2.add(lastStory2)
          }
          storyList2.add(thisStory)
        }
      })
    val value2 = storyList2.value

    // 第二步完成
    val secondDF = JavaConverters.asScalaIteratorConverter(value2.iterator).asScala.toSeq.toDS.filter(_.story_end != 0)
//      .select("platform_id","project_id","media_id","story_start","story_end","image")
//    secondDF.createOrReplaceTempView("secondDF")
//        secondDF.show(1000, false)



    secondDF.coalesce(1)
        .foreach(thisStory => {
          if (thisStory.timeLong < 20000){
            // 放入缓存区
            lastStory3 = thisStory
          } else {
            // 判断缓存区有无数据，有则连接并加入，并清空缓存区，无则加入本条数据到结果
            if (lastStory3.story_end == 1){
              // 无数据
              storyList3.add(thisStory)
            } else {
              // 有数据
              lastStory3 = lastStory3.copy(media_id = thisStory.media_id,story_end = thisStory.story_end,timeLong = thisStory.story_end - lastStory3.story_start)
              storyList3.add(lastStory3)
              lastStory3 = lastStory3.copy(media_id = "",platform_id = "",project_id = "",story_start = 0,story_end = 1,timeLong = 0,image = "")
            }
          }
        })

    val value3 = storyList3.value

    // 第三步完成
    val thirdDF = JavaConverters.asScalaIteratorConverter(value3.iterator).asScala.toSeq.toDS.filter(_.story_end != 1).select("platform_id","project_id","media_id","story_start","story_end","image")
    thirdDF.createOrReplaceTempView("thirddf")

    // 添加headHaveLines,tailHaveLines字段,用于判断每个片段是头和尾是否有台词
    spark.sql(
      """
        |select platform_id,project_id,media_id,story_start,story_end,image,if(sum(head)>0,1,0) headHaveLines,if(sum(tail)>0,1,0) tailHaveLines
        |from (
        |       select thirddf.platform_id platform_id,
        |              thirddf.project_id project_id,
        |              thirddf.media_id media_id,
        |              thirddf.story_start story_start,
        |              thirddf.story_end story_end,
        |              thirddf.image image,
        |              if(recognition2_ocr.lines_start < thirddf.story_start + 2500,1,0) head,
        |              if(recognition2_ocr.lines_end > thirddf.story_end - 2500,1,0) tail
        |       from thirddf
        |       left join recognition2_ocr
        |       on thirddf.platform_id = recognition2_ocr.platform_id
        |       and thirddf.project_id = recognition2_ocr.project_id
        |       and thirddf.media_id = recognition2_ocr.media_id
        |       and recognition2_ocr.lines_start < thirddf.story_end
        |       and recognition2_ocr.lines_end > thirddf.story_start) t1
        |group by platform_id,project_id,media_id,story_start,story_end,image
        |""".stripMargin)
//        .show(1000,false)
      .map(row => Story3(
        row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getInt(3),
        row.getInt(4),
        row.getString(5),
        row.getInt(6),
        row.getInt(7)
      ))
        .coalesce(1)
        .foreach(thisStory => {
          if (thisStory.headHaveLines == 0){
            // 输出缓存区,把thisStory放入缓存区
            storyList4.add(lastStory4)
            lastStory4 = thisStory
          } else {
            if (lastStory4.tailHaveLines == 1){
              // 修改lastStory4.story_end = thisStory.story_end,head,tail
              lastStory4 = lastStory4.copy(media_id = thisStory.media_id,story_end = thisStory.story_end,headHaveLines = thisStory.headHaveLines,tailHaveLines = thisStory.tailHaveLines)
            } else {
              // 输出缓存区,把thisStory放入缓存区
              storyList4.add(lastStory4)
              lastStory4 = thisStory
            }
          }
        })

    val value4 = storyList4.value

    val fourthDF = JavaConverters.asScalaIteratorConverter(value4.iterator).asScala.toSeq.toDS.filter(_.story_end != 1).select("platform_id","project_id","media_id","story_start","story_end","image")
    fourthDF.createOrReplaceTempView("fourthdf")
//            .show(1000,false)



    /**
     * 分镜头片段表：fourthDF
     * 下面正式处理分镜头逻辑
     */


    // 1.在分镜头内打签
    spark.sql(
      """
        |select video_id,
        |             media_name,
        |             project_id,
        |             department_id,
        |             ad_seat_b_time,
        |             ad_seat_e_time,
        |             drama_name,
        |             drama_type_name,
        |             media_area_name,
        |             class2_name,
        |             class_type_id,
        |             class3_name,
        |             ad_seat_img,
        |             story_start,
        |             story_end,
        |             duration,
        |             confidence,
        |             CONCAT_WS('*',Width,Height) resolution,
        |             frame,
        |             concat_ws('_',ad_seat_b_time,ad_seat_e_time) class3Time
        |      from (select recognition2_behavior.media_id   video_id,
        |                   kukai_videos.videoName           media_name,
        |                   recognition2_behavior.time_start ad_seat_b_time,
        |                   recognition2_behavior.time_end   ad_seat_e_time,
        |                   kukai_videos.category            drama_name,
        |                   kukai_videos.classify            drama_type_name,
        |                   kukai_videos.area                media_area_name,
        |                   recognition2_class.class1_name   class2_name,
        |                   recognition2_class.class_type    class_type_id,
        |                   recognition2_class.class2_name   class3_name,
        |                   recognition2_behavior.object_img ad_seat_img,
        |                   story_start,
        |                   story_end,
        |                   kukai_videos.albumId project_id,
        |                   kukai_videos.department_id department_id,
        |                   kukai_videos.duration duration,
        |                   recognition2_behavior.score confidence,
        |                   kukai_videos.videoWidth Width,
        |                   kukai_videos.videoHeight Height,
        |                   kukai_videos.frame frame
        |            from recognition2_behavior
        |                     join recognition2_class
        |                          on recognition2_behavior.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_behavior.media_id
        |                     right join fourthDF
        |                          on recognition2_behavior.media_id = fourthDF.media_id
        |            union all
        |            select recognition2_face.media_id     video_id,
        |                   kukai_videos.videoName         media_name,
        |                   recognition2_face.time_start   ad_seat_b_time,
        |                   recognition2_face.time_end     ad_seat_e_time,
        |                   kukai_videos.category          drama_name,
        |                   kukai_videos.classify          drama_type_name,
        |                   kukai_videos.area              media_area_name,
        |                   recognition2_class.class1_name class2_name,
        |                   recognition2_class.class_type  class_type_id,
        |                   recognition2_class.class2_name class3_name,
        |                   recognition2_face.object_img   ad_seat_img,
        |                   story_start,
        |                   story_end,
        |                   kukai_videos.albumId project_id,
        |                   kukai_videos.department_id department_id,
        |                   kukai_videos.duration duration,
        |                   recognition2_face.score confidence,
        |                   kukai_videos.videoWidth Width,
        |                   kukai_videos.videoHeight Height,
        |                   kukai_videos.frame frame
        |            from recognition2_face
        |                     join recognition2_class
        |                          on recognition2_face.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_face.media_id
        |                     right join fourthDF
        |                          on recognition2_face.media_id = fourthDF.media_id
        |            union all
        |            select recognition2_object.media_id   video_id,
        |                   kukai_videos.videoName         media_name,
        |                   recognition2_object.time_start ad_seat_b_time,
        |                   recognition2_object.time_end   ad_seat_e_time,
        |                   kukai_videos.category          drama_name,
        |                   kukai_videos.classify          drama_type_name,
        |                   kukai_videos.area              media_area_name,
        |                   recognition2_class.class1_name class2_name,
        |                   recognition2_class.class_type  class_type_id,
        |                   recognition2_class.class2_name class3_name,
        |                   recognition2_object.object_img ad_seat_img,
        |                   story_start,
        |                   story_end,
        |                   kukai_videos.albumId project_id,
        |                   kukai_videos.department_id department_id,
        |                   kukai_videos.duration duration,
        |                   recognition2_object.score confidence,
        |                   kukai_videos.videoWidth Width,
        |                   kukai_videos.videoHeight Height,
        |                   kukai_videos.frame frame
        |            from recognition2_object
        |                     join recognition2_class
        |                          on recognition2_object.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_object.media_id
        |                     right join fourthDF
        |                          on recognition2_object.media_id = fourthDF.media_id
        |            union all
        |            select recognition2_scene.media_id    video_id,
        |                   kukai_videos.videoName         media_name,
        |                   recognition2_scene.time_start  ad_seat_b_time,
        |                   recognition2_scene.time_end    ad_seat_e_time,
        |                   kukai_videos.category          drama_name,
        |                   kukai_videos.classify          drama_type_name,
        |                   kukai_videos.area              media_area_name,
        |                   recognition2_class.class1_name class2_name,
        |                   recognition2_class.class_type  class_type_id,
        |                   recognition2_class.class2_name class3_name,
        |                   recognition2_scene.object_img  ad_seat_img,
        |                   story_start,
        |                   story_end,
        |                   kukai_videos.albumId project_id,
        |                   kukai_videos.department_id department_id,
        |                   kukai_videos.duration duration,
        |                   recognition2_scene.score confidence,
        |                   kukai_videos.videoWidth Width,
        |                   kukai_videos.videoHeight Height,
        |                   kukai_videos.frame frame
        |            from recognition2_scene
        |                     join recognition2_class
        |                          on recognition2_scene.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_scene.media_id
        |                     right join fourthDF
        |                          on recognition2_scene.media_id = fourthDF.media_id
        |           ) b
        |           where ad_seat_b_time >= story_start
        |              and ad_seat_e_time <= story_end
        |""".stripMargin)
//        .show(1000,false)
      .createOrReplaceTempView("a0")

    spark.sql(
      """
        |select a0.*
        | from a0 join task
        | on a0.video_id=task.video_id
        |""".stripMargin)
      .createOrReplaceTempView("a01")

    // 分组求和
    spark.sql(
      """
        |select a01.video_id, media_name, drama_name, drama_type_name, media_area_name, class2_name, class_type_id, first(ad_seat_img) ad_seat_img, first(ad_seat_b_time) ad_seat_b_time, first(ad_seat_e_time) ad_seat_e_time, story_start, story_end, sum(ad_seat_e_time - ad_seat_b_time) totaltime,
        |       concat_ws('',concat_ws(',',collect_set(class_type_id)),concat_ws(',',collect_set(class3_name))) as class3_name,
        |       first(project_id) project_id,
        |       first(department_id) department_id,
        |       first(duration) duration,
        |       first(confidence) confidence,
        |       first(resolution) resolution,
        |       first(frame) frame,
        |       collect_set(class3Time) class3Time_tmp
        |from a01
        |group by a01.video_id, media_name, drama_name, drama_type_name, class2_name, media_area_name, class_type_id, class3_name, story_start, story_end
        |""".stripMargin)
      .createOrReplaceTempView("a1")

    // 加权重
    spark.sql(
      s"""
         |select *,${confUtil.a}*totaltime/(story_end-story_start)+${confUtil.b}*totaltime/duration+${confUtil.c}*confidence score
         |from a1
         |""".stripMargin)
      .createOrReplaceTempView("a11")


    // 组内排序
    spark.sql(
      """
        |select *, row_number() OVER (PARTITION BY story_start, story_end, class_type_id ORDER BY score) rank
        |from a11
        |""".stripMargin)
      .createOrReplaceTempView("a2")

    // 取组内TOP10
    spark.sql(
      """
        |select *,concat_ws(':',substr(class3_name,2),class3Time) classToTime
        |from (
        |   select video_id,media_name,drama_name,drama_type_name,media_area_name,class2_name,class_type_id,ad_seat_img,ad_seat_b_time,ad_seat_e_time,story_start,story_end,totaltime,class3_name,project_id,department_id,duration,confidence,resolution,frame,score,rank,explode(class3Time_tmp) class3Time
        |   from (
        |      select *
        |      from a2
        |      where class_type_id = 1 and rank <= 3
        |      union all
        |      select *
        |      from a2
        |      where class_type_id = 2 and rank <= 3
        |      union all
        |      select *
        |      from a2
        |      where class_type_id = 3 and rank <= 2
        |      union all
        |      select *
        |      from a2
        |      where class_type_id = 4 and rank <= 2) t1) t2
        |""".stripMargin)
      .createOrReplaceTempView("a3")
    //        .show(1000,false)

    // useful
    spark.sql(
      """
        |SELECT drama_name string_drama_name,
        |       drama_type_name string_drama_type_name,
        |       concat_ws('_',story_start,story_end) string_time,
        |       first(project_id) project_id,
        |       first(department_id) department_id,
        |       video_id string_vid,
        |       media_area_name string_media_area_name,
        |       story_end - story_start string_time_long,
        |       media_name,
        |       concat_ws(',',collect_set(class_type_id)) as class_type_id,
        |       concat_ws(',',collect_set(class3_name)) as class3_name,
        |       concat_ws(',',collect_set(ad_seat_img)) as ad_seat_img,
        |       concat_ws(',',collect_set(class2_name)) as class2_name,
        |       first(resolution) resolution,
        |       first(frame) frame,
        |       concat_ws(',',collect_set(classToTime)) classToTime
        |FROM a3
        |GROUP BY drama_name,drama_type_name,video_id,media_area_name,story_start,story_end,media_name
        |""".stripMargin)
      .createOrReplaceTempView("a4")
    //        .show(1000,false)

    spark.sql(
      """
        |select string_vid,SUBSTRING_INDEX(string_time,'_',1) bt
        |from a4
        |""".stripMargin)
      .createOrReplaceTempView("a")

    spark.sql(
      """
        |select media_id,story_start
        |from fourthDF
        |join task
        |on media_id=video_id
        |""".stripMargin)
      .createOrReplaceTempView("b")


    spark.sql(
      """
        |select media_id,story_start
        |from b
        |left join a
        |on bt=story_start
        |where bt is null
        |""".stripMargin)
      .createOrReplaceTempView("useless")

    spark.sql(
      """
        |select kukai_videos.category  string_drama_name,
        |       kukai_videos.classify   string_drama_type_name,
        |       concat_ws('_',fourthDF.story_start,fourthDF.story_end)  string_time,
        |       fourthDF.media_id  string_vid,
        |       kukai_videos.area string_media_area_name,
        |       fourthDF.story_end-fourthDF.story_start string_time_long,
        |       kukai_videos.videoName  media_name,
        |       kukai_videos.albumId  project_id,
        |       kukai_videos.department_id  department_id,
        |       fourthDF.image image
        |from useless
        |left join fourthDF
        |on useless.media_id=fourthDF.media_id
        |and useless.story_start=fourthDF.story_start
        |left join kukai_videos
        |on useless.media_id=kukai_videos.videoId
        |""".stripMargin)
      .createOrReplaceTempView("ulstory")


    val rst3 = spark.sql(
      """
        |select *
        |from ulstory
        |""".stripMargin)
      .toJSON
      .rdd
      .map(x => {
        val newObject = new JSONObject()

        val oldObject = JSON.parseObject(x)

        val string_drama_name = oldObject.getString("string_drama_name")
        val string_drama_type_name = oldObject.getString("string_drama_type_name")
        val string_time = oldObject.getString("string_time")
        val project_id = oldObject.getString("project_id")
        val department_id = oldObject.getString("department_id")
        val string_vid = oldObject.getString("string_vid")
        val string_media_area_name = oldObject.getString("string_media_area_name")
        val string_time_long = oldObject.getInteger("string_time_long")
        val media_name = oldObject.getString("media_name")
        val image = oldObject.getString("image")

        val imglist = new JSONArray()
        imglist.add(image)

        newObject.put("string_vid", string_vid)
        newObject.put("media_name", media_name)
        newObject.put("project_id", project_id)
        newObject.put("department_id", department_id)
        newObject.put("string_drama_name", string_drama_name)
        newObject.put("string_drama_type_name", string_drama_type_name)
        newObject.put("string_media_area_name", string_media_area_name)
        newObject.put("b_t", string_time.split('_').head.toLong)
        newObject.put("string_time", string_time)
        newObject.put("string_time_long", string_time_long)
        newObject.put("string_class3_list", new JSONArray())
        newObject.put("string_class3", "")
        newObject.put("string_man_list", new JSONArray())
        newObject.put("string_object_list", new JSONArray())
        newObject.put("string_action_list", new JSONArray())
        newObject.put("string_sence_list", new JSONArray())
        newObject.put("string_class2_list", new JSONArray())
        newObject.put("string_class_img_list", imglist)
        newObject.put("resolution", "1*1")
        newObject.put("frame", 1)
        newObject.put("class3Time", new JSONArray().toString)


        newObject.put("resourceId", "2")

        newObject.toString
      })

    rst3.saveJsonToEs("video_wave/doc", Map(
      //            "es.index.auto.create" -> "true",
      "es.nodes" -> confUtil.adxStreamingEsHost,
      "es.user" -> confUtil.adxStreamingEsUser,
      "es.password" -> confUtil.adxStreamingEsPassword,
      "es.port" -> "9200"
    ))
    logWarning("空标签分镜头存入ES成功")


    rst3.groupBy(str => {
      JSON.parseObject(str).getString("string_vid")
    }).foreach(x => {
      val vid = x._1
      val storyNum = x._2.toList.size

      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        sqlProxy.executeUpdate(client, "update `task` set story=?,total=total+?,status=1 where video_id = ?",
          Array(storyNum, storyNum, vid))
      }
      catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }

    })
    logWarning("更新task表分镜头数量成功")


    val rst2 = spark.sql(
      """
        |select *
        |from a4
        |""".stripMargin)
      .toJSON
      .rdd
      .map(x => {
        val newObject = new JSONObject()

        val oldObject = JSON.parseObject(x)

        val string_drama_name = oldObject.getString("string_drama_name")
        val string_drama_type_name = oldObject.getString("string_drama_type_name")
        val string_time = oldObject.getString("string_time")
        val project_id = oldObject.getString("project_id")
        val department_id = oldObject.getString("department_id")
        val string_vid = oldObject.getString("string_vid")
        val string_media_area_name = oldObject.getString("string_media_area_name")
        val string_time_long = oldObject.getInteger("string_time_long")
        val media_name = oldObject.getString("media_name")
        val resolution = oldObject.getString("resolution")
        val frame = oldObject.getString("frame")

        val class3_list = oldObject.getString("class3_name").split(',').toList
        val class_img_list = oldObject.getString("ad_seat_img").split(',').toList
        val class2_list = oldObject.getString("class2_name").split(',').toList
        val classToTimeList = oldObject.getString("classToTime").split(',').toList

        val string_class3_list = new JSONArray()
        val manList = new JSONArray()
        val objectList = new JSONArray()
        val actionList = new JSONArray()
        val senceList = new JSONArray()
        val class3Time = new JSONArray()

        val string_class_img_list = new JSONArray()
        val string_class2_list = new JSONArray()

        for (i <- class3_list) {
          i.substring(0, 1) match {
            case "4" =>
              manList.add(i.substring(1))
            //          man2List.add(file9)
            //          manImgList.add(file10)
            case "1" =>
              objectList.add(i.substring(1))
            //          object2List.add(file9)
            //          objectImgList.add(file10)
            case "3" =>
              actionList.add(i.substring(1))
            //          action2List.add(file9)
            //          actionImgList.add(file10)
            case "2" =>
              senceList.add(i.substring(1))
            //          sence2List.add(file9)
            //          senceImgList.add(file10)
          }
        }

        for (i <- class3_list) {
          string_class3_list.add(i.substring(1))

        }
        for (i <- class_img_list) {
          string_class_img_list.add(i)
        }
        for (i <- class2_list) {
          string_class2_list.add(i)
        }
        for (i <- classToTimeList) {
          class3Time.add(i)
        }


        newObject.put("string_vid", string_vid)
        newObject.put("media_name", media_name)
        newObject.put("project_id", project_id)
        newObject.put("department_id", department_id)
        newObject.put("string_drama_name", string_drama_name)
        newObject.put("string_drama_type_name", string_drama_type_name)
        newObject.put("string_media_area_name", string_media_area_name)
        newObject.put("string_time", string_time)
        newObject.put("b_t", string_time.split('_').head.toLong)
        newObject.put("string_time_long", string_time_long)
        newObject.put("string_class3_list", string_class3_list)
        newObject.put("string_class3", string_class3_list.toString)
        newObject.put("string_man_list", manList)
        newObject.put("string_object_list", objectList)
        newObject.put("string_action_list", actionList)
        newObject.put("string_sence_list", senceList)
        newObject.put("string_class2_list", string_class2_list)
        newObject.put("string_class_img_list", string_class_img_list)
        newObject.put("resolution", resolution)
        newObject.put("frame", frame)
        newObject.put("class3Time", class3Time.toString)


        newObject.put("resourceId", "2")

        newObject.toString

      })


    rst2.saveJsonToEs("video_wave/doc", Map(
      //            "es.index.auto.create" -> "true",
      "es.nodes" -> confUtil.adxStreamingEsHost,
      "es.user" -> confUtil.adxStreamingEsUser,
      "es.password" -> confUtil.adxStreamingEsPassword,
      "es.port" -> "9200"
    ))
    logWarning("分镜头存入ES成功")


    rst2.groupBy(str => {
      JSON.parseObject(str).getString("string_vid")
    }).foreach(x => {
      val vid = x._1
      val storyNum = x._2.toList.size

      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        sqlProxy.executeUpdate(client, "update `task` set story=story+?,total=total+?,status=1 where video_id = ?",
          Array(storyNum, storyNum, vid))
      }
      catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }

    })
    logWarning("更新task表分镜头数量成功")


    spark.sql(
      """
        |select *
        |from task
        |""".stripMargin)
      .toJSON
      .rdd
      .foreach(str => {
        val nObject = JSON.parseObject(str)
        val vid = nObject.getString("video_id")

        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          sqlProxy.executeUpdate(client, "update task set status=1 where video_id=?",
            Array(vid))
        }
        catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

    logWarning("更新task表剩余status成功")


    spark.close()


  }

}
