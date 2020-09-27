package com.bjvca.videocut

import com.alibaba.fastjson.JSONObject
import com.bjvca.commonutils.ConfUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

import scala.collection.mutable.ListBuffer

/**
 * 多标签
 * 正常合成
 */
object Cleaned2 extends Logging {

  def main(args: Array[String]): Unit = {


    logWarning("Demo开始运行")


    val confUtil = new ConfUtils("application.conf")
    //    val confUtil = new ConfUtils("线上application.conf")

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .config("es.net.proxy.http.use.system.props", "false")
      .config("spark.debug.maxToStringFields", "200")
      .getOrCreate()

    // 读取将要用到的表
    // 1.recognition2_behavior
    spark.read.format("jdbc")
      //      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
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
      //      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
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
      //      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
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
      //      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
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
      //      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
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
      //      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
      .options(Map("url" -> s"jdbc:mysql://${confUtil.videocutMysqlHost}:3306/video_wave?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "kukai_videos"
      ))
      .load()
      .createOrReplaceTempView("kukai_videos")


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

    // 拿到全部广告位数据
    // 视频id、视频名、
    // 开始时间、结束时间
    // drama_name（剧集分类）, drama_type_name（剧集类型）
    // media_area_name（地区名）, media_release_data（上映年份）
    // 二级标签name
    // 一级标签id（分类用）、三级标签name
    //      .select($"video_id", $"media_name",
    //      $"ad_seat_b_time", $"ad_seat_e_time",
    //      $"drama_name", $"drama_type_name",
    //      $"media_area_name", $"media_release_date",
    //      $"class2_name",
    //      $"class_type_id", $"class3_name",
    //      $"ad_seat_img")
    //      .createOrReplaceTempView("aaa")

    // 1.拿到所有的广告位 aaa
    spark.sql(
      """
        |select recognition2_behavior.media_id video_id,
        |       kukai_videos.videoName media_name,
        |       recognition2_behavior.time_start ad_seat_b_time,
        |       recognition2_behavior.time_end ad_seat_e_time,
        |       kukai_videos.category drama_name,
        |       kukai_videos.classify drama_type_name,
        |       kukai_videos.area media_area_name,
        |       recognition2_class.class1_name class2_name,
        |       recognition2_class.class_type class_type_id,
        |       recognition2_class.class2_name class3_name,
        |       recognition2_behavior.object_img ad_seat_img
        |from recognition2_behavior
        |join recognition2_class
        |    on recognition2_behavior.class_id = recognition2_class.class_id
        |join kukai_videos
        |on kukai_videos.videoId = media_id
        |union all
        |select recognition2_face.media_id video_id,
        |       kukai_videos.videoName media_name,
        |       recognition2_face.time_start ad_seat_b_time,
        |       recognition2_face.time_end ad_seat_e_time,
        |       kukai_videos.category drama_name,
        |       kukai_videos.classify drama_type_name,
        |       kukai_videos.area media_area_name,
        |       recognition2_class.class1_name class2_name,
        |       recognition2_class.class_type class_type_id,
        |       recognition2_class.class2_name class3_name,
        |       recognition2_face.object_img ad_seat_img
        |from recognition2_face
        |join recognition2_class
        |    on recognition2_face.class_id = recognition2_class.class_id
        |join kukai_videos
        |on kukai_videos.videoId = media_id
        |union all
        |select recognition2_object.media_id video_id,
        |       kukai_videos.videoName media_name,
        |       recognition2_object.time_start ad_seat_b_time,
        |       recognition2_object.time_end ad_seat_e_time,
        |       kukai_videos.category drama_name,
        |       kukai_videos.classify drama_type_name,
        |       kukai_videos.area media_area_name,
        |       recognition2_class.class1_name class2_name,
        |       recognition2_class.class_type class_type_id,
        |       recognition2_class.class2_name class3_name,
        |       recognition2_object.object_img ad_seat_img
        |from recognition2_object
        |join recognition2_class
        |    on recognition2_object.class_id = recognition2_class.class_id
        |join kukai_videos
        |on kukai_videos.videoId = media_id
        |union all
        |select recognition2_scene.media_id video_id,
        |       kukai_videos.videoName media_name,
        |       recognition2_scene.time_start ad_seat_b_time,
        |       recognition2_scene.time_end ad_seat_e_time,
        |       kukai_videos.category drama_name,
        |       kukai_videos.classify drama_type_name,
        |       kukai_videos.area media_area_name,
        |       recognition2_class.class1_name class2_name,
        |       recognition2_class.class_type class_type_id,
        |       recognition2_class.class2_name class3_name,
        |       recognition2_scene.object_img ad_seat_img
        |from recognition2_scene
        |join recognition2_class
        |    on recognition2_scene.class_id = recognition2_class.class_id
        |join kukai_videos
        |on kukai_videos.videoId = media_id
        |""".stripMargin)
      //      .filter(s"video_id = $video_id")
      .createOrReplaceTempView("aaa")

    // 为后面过滤掉ts格式的视频
    //    val filterList = spark.sql(
    //      """
    //        |SELECT * FROM bbb WHERE media_addr LIKE '%.ts'
    //        |""".stripMargin)
    //      .select("video_id")
    //      .collect()
    //    val array = filterList.map(_.get(0).toString)

    //     广播出去ts的数组
    //    val bFilterList = spark.sparkContext.broadcast(array)

    spark.sql(
      """
        |select
        |aaa.video_id,
        |aaa.media_name,
        |aaa.drama_name,
        |aaa.drama_type_name,
        |aaa.media_area_name,
        |aaa.class2_name,
        |aaa.class_type_id,
        |aaa.class3_name,
        |aaa.ad_seat_b_time,
        |aaa.ad_seat_e_time,
        |aaa.ad_seat_img
        |from aaa join bbb
        |on aaa.video_id=bbb.video_id
        |""".stripMargin)
      .createOrReplaceTempView("ccc")

    spark.sql("cache table ccc")

    spark.sql("select * from ccc")
      .rdd
      .map(x => AdSeat(x.get(0).toString, x.get(1).toString, x.get(2).toString, x.get(3).toString, x.get(4).toString,
        x.get(5).toString, x.get(6).toString, x.get(7).toString, x.get(8).toString, x.get(9).toString,
        x.get(10).toString))

      //    val reduced = mysqlRDD
      //      .filter(x => {
      //        val key = x.class_type_id
      //        key.equals("1") || key.equals("2") || key.equals("3") || key.equals("4")
      //      })
      //      .filter(x => {
      //        !bFilterList.value.contains(x.video_id)
      //      })
      // 处理数据为json格式，以video_id为key的元组
      .map(x => {
        val videoID = x.video_id
        val class3Name = x.class3_name
        ((videoID, class3Name), x)
      })
      .groupByKey()

      /**
       * 间隔十秒内的相同标签都合并
       *
       */
      .mapValues(x => {

        // 转换成数组然后排序
        val seatSorted = x.toList.sortBy(_.ad_seat_b_time.toLong)

        var temp: AdSeat = seatSorted(0)
        val result = scala.collection.mutable.ListBuffer[AdSeat]()

        for (i <- 1 until seatSorted.size) {

          val thisSeat = seatSorted(i)

          if (thisSeat.ad_seat_b_time.toLong - temp.ad_seat_e_time.toLong <= 10000) {
            // 合并广告位，然后继续等待下一个标签
            temp = temp.copy(ad_seat_e_time = thisSeat.ad_seat_e_time)
          } else {
            // 不合并，输出已有的广告位
            result.append(temp.copy())
            temp = thisSeat
          }

        }
        result.append(temp)
        result
      })
      .flatMap(_._2)
      .map(x => {
        (x.video_id, x)
      })

      /**
       * 核心逻辑
       *
       * 间隔10秒内的标签都组合
       */
      .groupByKey()
      .mapValues(x => {
        val seatSorted = x.toList.sortBy(_.ad_seat_b_time.toLong)

        // 最终返回的数据resultList
        var resultList = ListBuffer[ListBuffer[AdSeat]]()

        var temp = new ListBuffer[AdSeat]()
        temp.append(seatSorted(0))

        var maxETime: String = seatSorted(0).ad_seat_e_time

        // 遍历所有point点，进而增加或减少tempMap中的adseat，进而处理处新片段
        for (i <- 1 until seatSorted.size) {
          val thisSeat = seatSorted(i)

          if (thisSeat.ad_seat_b_time.toLong - maxETime.toLong <= 10000) {
            temp.append(thisSeat)
            maxETime = Math.max(thisSeat.ad_seat_b_time.toLong, maxETime.toLong).toString
          } else {
            resultList.append(temp.clone())

            temp.clear()
            temp.append(thisSeat)
            maxETime = thisSeat.ad_seat_e_time
          }
        }

        resultList.append(temp.clone())

        // 最终返回resultList
        resultList
      })
      .flatMap(x => x._2)
      .map(x => CuterUtils.seatToJSON(x))
      .filter(x => x.asInstanceOf[JSONObject].getString("string_time_long").toLong >= 1000)
      .map(_.toString)
      .saveJsonToEs("video_wave/doc", Map(
        "es.index.auto.create" -> "true",
        "es.nodes" -> confUtil.adxStreamingEsHost,
        "es.port" -> "9200"
      ))
  }

}