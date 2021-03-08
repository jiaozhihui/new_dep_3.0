package com.bjvca.storycut

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bjvca.commonutils.ConfUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


/**
 * 分镜头
 */
object SubCut extends Logging {

  def main(args: Array[String]): Unit = {


    logWarning("Demo开始运行")


    val confUtil = new ConfUtils("application58.conf")
    //    val confUtil = new ConfUtils("线上application.conf")

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .config("es.net.proxy.http.use.system.props", "false")
      .config("spark.debug.maxToStringFields", "200")
      .getOrCreate()


//     0.读取任务表
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


    // 从mysql拿到数据，转化为json
    import spark.implicits._

    /**
     * 从广告位拿到可用的广告位列表
     */
    // 拿到可播放视频
    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "kukai_videos"
      )).load()
      .select($"videoId" as "video_id", $"originalUrl" as "media_addr")
      .createOrReplaceTempView("bbb")


    // 1.未分组 a0
    spark.sql(
      """
        |select t.*
        |from (
        |  select video_id,
        |         media_name,
        |         ad_seat_b_time,
        |         ad_seat_e_time,
        |         drama_name,
        |         drama_type_name,
        |         media_area_name,
        |         class2_name,
        |         class_type_id,
        |         class3_name,
        |         ad_seat_img,
        |         story_start,
        |         story_end
        |  from (select recognition2_behavior.media_id   video_id,
        |               kukai_videos.videoName           media_name,
        |               recognition2_behavior.time_start ad_seat_b_time,
        |               recognition2_behavior.time_end   ad_seat_e_time,
        |               kukai_videos.category            drama_name,
        |               kukai_videos.classify            drama_type_name,
        |               kukai_videos.area                media_area_name,
        |               recognition2_class.class1_name   class2_name,
        |               recognition2_class.class_type    class_type_id,
        |               recognition2_class.class2_name   class3_name,
        |               recognition2_behavior.object_img ad_seat_img,
        |               story_start,
        |               story_end
        |        from recognition2_behavior
        |                 left join recognition2_class
        |                      on recognition2_behavior.class_id = recognition2_class.class_id
        |                 left join kukai_videos
        |                      on kukai_videos.videoId = media_id
        |                 left join recognition2_videostory
        |                      on recognition2_behavior.media_id = recognition2_videostory.media_id
        |        union all
        |        select recognition2_face.media_id     video_id,
        |               kukai_videos.videoName         media_name,
        |               recognition2_face.time_start   ad_seat_b_time,
        |               recognition2_face.time_end     ad_seat_e_time,
        |               kukai_videos.category          drama_name,
        |               kukai_videos.classify          drama_type_name,
        |               kukai_videos.area              media_area_name,
        |               recognition2_class.class1_name class2_name,
        |               recognition2_class.class_type  class_type_id,
        |               recognition2_class.class2_name class3_name,
        |               recognition2_face.object_img   ad_seat_img,
        |               story_start,
        |               story_end
        |        from recognition2_face
        |                 left join recognition2_class
        |                      on recognition2_face.class_id = recognition2_class.class_id
        |                 left join kukai_videos
        |                      on kukai_videos.videoId = media_id
        |                 left join recognition2_videostory
        |                      on recognition2_face.media_id = recognition2_videostory.media_id
        |        union all
        |        select recognition2_object.media_id   video_id,
        |               kukai_videos.videoName         media_name,
        |               recognition2_object.time_start ad_seat_b_time,
        |               recognition2_object.time_end   ad_seat_e_time,
        |               kukai_videos.category          drama_name,
        |               kukai_videos.classify          drama_type_name,
        |               kukai_videos.area              media_area_name,
        |               recognition2_class.class1_name class2_name,
        |               recognition2_class.class_type  class_type_id,
        |               recognition2_class.class2_name class3_name,
        |               recognition2_object.object_img ad_seat_img,
        |               story_start,
        |               story_end
        |        from recognition2_object
        |                 left join recognition2_class
        |                      on recognition2_object.class_id = recognition2_class.class_id
        |                 left join kukai_videos
        |                      on kukai_videos.videoId = media_id
        |                 left join recognition2_videostory
        |                      on recognition2_object.media_id = recognition2_videostory.media_id
        |        union all
        |        select recognition2_scene.media_id    video_id,
        |               kukai_videos.videoName         media_name,
        |               recognition2_scene.time_start  ad_seat_b_time,
        |               recognition2_scene.time_end    ad_seat_e_time,
        |               kukai_videos.category          drama_name,
        |               kukai_videos.classify          drama_type_name,
        |               kukai_videos.area              media_area_name,
        |               recognition2_class.class1_name class2_name,
        |               recognition2_class.class_type  class_type_id,
        |               recognition2_class.class2_name class3_name,
        |               recognition2_scene.object_img  ad_seat_img,
        |               story_start,
        |               story_end
        |        from recognition2_scene
        |                 left join recognition2_class
        |                      on recognition2_scene.class_id = recognition2_class.class_id
        |                 left join kukai_videos
        |                      on kukai_videos.videoId = media_id
        |                 left join recognition2_videostory
        |                      on recognition2_scene.media_id = recognition2_videostory.media_id
        |       ) b ) t
        |       join task
        |       on task.video_id = t.video_id
        |""".stripMargin)
      .createOrReplaceTempView("a0")



    // 分组求和
    spark.sql(
      """
        |select video_id, media_name, drama_name, drama_type_name, media_area_name, class2_name, class_type_id, first(ad_seat_img) ad_seat_img, first(ad_seat_b_time) ad_seat_b_time, first(ad_seat_e_time) ad_seat_e_time, story_start, story_end, sum(ad_seat_e_time - ad_seat_b_time) totaltime,
        |       concat_ws('',concat_ws(',',collect_set(class_type_id)),concat_ws(',',collect_set(class3_name))) as class3_name
        |from a0
        |where ad_seat_b_time > story_start
        |  and ad_seat_e_time < story_end
        |group by video_id, media_name, drama_name, drama_type_name, class2_name, media_area_name, class_type_id, class3_name, story_start, story_end
        |""".stripMargin)
      .createOrReplaceTempView("a1")
//
//    // 准备将要进行分组的字段
//    spark.sql(
//      """
//        |select *, totaltime/(story_end - story_start) appearRate
//        |from a1
//        |""".stripMargin)

    // 组内排序
    spark.sql(
      """
        |select *, row_number() OVER (PARTITION BY story_start, story_end ORDER BY totaltime DESC) rank
        |from a1
        |""".stripMargin)
      .createOrReplaceTempView("a2")

    // 取组内TOP10
    spark.sql(
      """
        |select * from a2 where rank <= 10
        |""".stripMargin)
      .createOrReplaceTempView("a3")


    // 合并标签
    spark.sql(
      """
        |SELECT drama_name string_drama_name,
        |       drama_type_name string_drama_type_name,
        |       concat_ws('_',story_start,story_end) string_time,
        |       video_id string_vid,
        |       media_area_name string_media_area_name,
        |       story_end - story_start string_long,
        |       media_name,
        |       concat_ws(',',collect_set(class3_name)) as class3_name,
        |       concat_ws(',',collect_set(ad_seat_img)) as ad_seat_img,
        |       concat_ws(',',collect_set(class2_name)) as class2_name
        |FROM a3
        |GROUP BY drama_name,drama_type_name,video_id,media_area_name,story_start,story_end,media_name
        |""".stripMargin)
    .toJSON
    .rdd
      .map(x => {
        val newObject = new JSONObject()

        val oldObject = JSON.parseObject(x)

        val string_drama_name = oldObject.getString("string_drama_name")
        val string_drama_type_name = oldObject.getString("string_drama_type_name")
        val string_time = oldObject.getString("string_time")
        val string_vid = oldObject.getString("string_vid")
        val string_media_area_name = oldObject.getString("string_media_area_name")
        val string_long = oldObject.getString("string_time_long")
        val media_name = oldObject.getString("media_name")

        val class3_list = oldObject.getString("class3_name").split(',').toList
        val class_img_list = oldObject.getString("ad_seat_img").split(',').toList
        val class2_list = oldObject.getString("class2_name").split(',').toList

        val string_class3_list = new JSONArray()
        val manList = new JSONArray()
        val objectList = new JSONArray()
        val actionList = new JSONArray()
        val senceList = new JSONArray()

        val string_class_img_list = new JSONArray()
        val string_class2_list = new JSONArray()

        for (i <- class3_list) {
          i.substring(0,1) match {
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

        newObject.put("string_vid", string_vid)
        newObject.put("media_name", media_name)
        newObject.put("string_drama_name", string_drama_name)
        newObject.put("string_drama_type_name", string_drama_type_name)
        newObject.put("string_media_area_name", string_media_area_name)
        newObject.put("string_time", string_time)
        newObject.put("string_long", string_long)
        newObject.put("string_class3_list", string_class3_list)
        newObject.put("string_man_list", manList)
        newObject.put("string_object_list", objectList)
        newObject.put("string_action_list", actionList)
        newObject.put("string_sence_list", senceList)
        newObject.put("string_class2_list", string_class2_list)
        newObject.put("string_class_img_list", string_class_img_list)

        newObject.put("resourceId", "3")

        newObject.toString

      })


      .saveJsonToEs("test/doc", Map(
        //            "es.index.auto.create" -> "true",
        "es.nodes" -> confUtil.adxStreamingEsHost,
        "es.user" -> confUtil.adxStreamingEsUser,
        "es.password" -> confUtil.adxStreamingEsPassword,
        "es.port" -> "9200"
      ))
    spark.close()

    //      Thread.sleep(5000)

  }

  //    }
}
