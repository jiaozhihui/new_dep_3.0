package com.bjvca.videocut

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bjvca.commonutils.{ConfUtils, DataSourceUtil, SqlProxy}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * （基于Cleaned_union2）
 * 分镜头 + 标签合成
 */
object AllCleand1 extends Logging {

  def main(args: Array[String]): Unit = {


    logWarning("开始合成")

        var i = 0

        while (true) {

    val confUtil = new ConfUtils("application.conf")
    //    val confUtil = new ConfUtils("线上application.conf")

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .config("es.net.proxy.http.use.system.props", "false")
      .config("spark.debug.maxToStringFields", "200")
      .getOrCreate()

          i = i + 1
          logWarning("第" + i + "次合并任务开始...")

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


    //    join task
    //      on task.video_id = media_id
    // 1.拿到所有的广告位 aaa
    spark.sql(
      """
        |select recognition2_behavior.media_id video_id,
        |       kukai_videos.albumId project_id,
        |       kukai_videos.department_id department_id,
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
        |join task
        |on task.video_id = media_id
        |union all
        |select recognition2_face.media_id video_id,
        |       kukai_videos.albumId project_id,
        |       kukai_videos.department_id department_id,
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
        |join task
        |on task.video_id = media_id
        |union all
        |select recognition2_object.media_id video_id,
        |       kukai_videos.albumId project_id,
        |       kukai_videos.department_id department_id,
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
        |join task
        |on task.video_id = media_id
        |union all
        |select recognition2_scene.media_id video_id,
        |       kukai_videos.albumId project_id,
        |       kukai_videos.department_id department_id,
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
        |join task
        |on task.video_id = media_id
        |""".stripMargin)
      //      .filter(s"video_id = $video_id")
      .createOrReplaceTempView("aaa")


    spark.sql(
      """
        |select
        |aaa.video_id,
        |aaa.project_id,
        |aaa.department_id,
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
        |from aaa join kukai_videos
        |on aaa.video_id=kukai_videos.videoId
        |""".stripMargin)
      .createOrReplaceTempView("ccc")

    spark.sql("cache table ccc")


    /**
     * 单标签合成
     */

    val rst1 = spark.sql("select * from ccc")
      .toJSON
      .rdd

      // 处理数据为json格式，以video_id为key的元组
      .map(x => {
        val jsonArray = new JSONArray()
        val key = JSON.parseObject(x).get("video_id").toString
        val class3Name = JSON.parseObject(x).get("class3_name").toString
        jsonArray.add(x)
        //      key、vid+class3
        //      value 标签信息
        ((key, class3Name), jsonArray)
      })
      // 将同一个video_id的相同标签reduce到一起，数据组成JSONArray
      .reduceByKey((x, y) => {
        for (i <- 0 until y.size()) {
          x.add(y.get(i))
        }
        x
      })

      /**
       * 预处理逻辑
       * 1、将所有的起止点，前后各扩展4秒
       * 2、根据vid和class3Name分组，将一样的分到一个组，
       * 3、拓展后相邻的相同标签合并在一起
       *
       */
      .map(x => {
        val vid = x._1
        val videocutJsonArray = x._2

        // 将时间点增大前后各四秒

        val videocutList = videocutJsonArray.toArray.map(videocutjson => {
          val nObject = JSON.parseObject(videocutjson.toString)

          val oldbtime = nObject.get("ad_seat_b_time").toString.toLong
          val oldetime = nObject.get("ad_seat_e_time").toString.toLong

          val newbtime = if (oldbtime.toLong - 4000 < 0) 0
          else oldbtime.toLong - 4000

          val newetime = oldetime.toLong + 4000

          nObject.put("ad_seat_b_time", newbtime.toString)
          nObject.put("ad_seat_e_time", newetime.toString)
          nObject
        }).sortBy(y => y.get("ad_seat_b_time").toString.toLong)

        val resultList = new JSONArray()

        var temp = videocutList(0)
        for (i <- 1 until videocutList.length) {
          val thisseat = videocutList(i)

          if (thisseat.getString("ad_seat_b_time").toLong - temp.getString("ad_seat_e_time").toLong <= 0) {
            val nowetime = thisseat.getString("ad_seat_e_time")

            temp.put("ad_seat_e_time", nowetime)
          } else {
            resultList.add(temp.clone().asInstanceOf[JSONObject])
            temp = thisseat

          }

        }
        resultList.add(temp.clone().asInstanceOf[JSONObject])

        (vid, resultList)
      })

      /**
       * 核心逻辑
       *
       * 对拿到的同一个video_id的一组视频进行处理
       * 将所有标签放到一个videocutMap中
       * 将所有起止点放到一个pointlist中
       * 1、创建一个空的tempMap
       * 2、遍历pointlist
       * 3、每拿到一个起始点，就从将对应的标签放到tempMap中
       * 4、输出resultMap中所有的视频片段到resultlist中
       * 5、每拿到一个终止点，就将对应的标签移除出tempMap
       * 6、重复3-4-5
       * 7、整理得到最终的resultMap
       */
      .map(x => {
        val videocutJsonArray = x._2

        // 广告位的Map
        var videocutMap = mutable.Map[String, JSONObject]()
        // 起止点的List
        var pointList = ListBuffer[(String, JSONObject)]()
        // 缓存当前videocut的tempMap
        var tempMap = mutable.Map[String, JSONObject]()
        // 最终返回的数据resultList
        var resultList = ListBuffer[(String, JSONObject)]()


        // 遍历广告位JSON数组，将数据添加到videocutMap中
        // 遍历广告位数据，将所有起止点放到pointList中
        for (i <- 0 until videocutJsonArray.size()) {
          val jsonObject = JSON.parseObject(videocutJsonArray.get(i).toString)
          //            val class3Name = jsonObject.get("class3_name").toString
          val bTime = jsonObject.get("ad_seat_b_time").toString
          val eTime = jsonObject.get("ad_seat_e_time").toString

          // key
          val key = bTime + "-" + Random.nextInt(1000)
          videocutMap += (key -> jsonObject)

          // 起始点
          val bObject = new JSONObject
          bObject.put("point_type", "begin")
          bObject.put("videocut_key", key)
          pointList += ((bTime, bObject))
          // 终止点
          val eObject = new JSONObject
          eObject.put("point_type", "end")
          eObject.put("videocut_key", key)
          pointList += ((eTime, eObject))
        }

        val pointList2 = pointList.sortBy(_._1.toInt)

        var beginTime = ""
        var endTime = ""

        // 遍历所有point点，进而增加或减少tempMap中的videocut，进而处理处新片段
        for (i <- pointList2.indices) {
          val (pointTime, thisPoint) = pointList2(i)
          val pointType = thisPoint.get("point_type").toString
          val videocutKey = thisPoint.get("videocut_key").toString

          //如果tempMap是空的，将pointTime赋值到开始时间
          if (tempMap.isEmpty) {
            // 初始化开始时间
            beginTime = pointTime
          } else {
            //设置本批次的结束时间为pointTime
            endTime = pointTime

            // 处理tempMap的数据，然后放到resultList中
            /**
             * 处理tempMap
             */
            // 先造出来一个片段的对象
            val tempJsonObj = new JSONObject()

            // 遍历tempMap，将这个片段内所包含的每个videocut数据处理进thisJsonObj
            val keys = tempMap.keys

            val class3List = new JSONArray()
            val manList = new JSONArray()
            val objectList = new JSONArray()
            val actionList = new JSONArray()
            val senceList = new JSONArray()

            val class2List = new JSONArray()
//            val man2List = new JSONArray()
//            val object2List = new JSONArray()
//            val action2List = new JSONArray()
//            val sence2List = new JSONArray()

            val classImgList = new JSONArray()
//            val manImgList = new JSONArray()
//            val objectImgList = new JSONArray()
//            val actionImgList = new JSONArray()
//            val senceImgList = new JSONArray()

            val class3ToImg = new JSONObject()

            for (key <- keys) {

              val thisObj = tempMap(key)
              val file1 = thisObj.get("video_id").asInstanceOf[String]
              val file2 = thisObj.get("media_name").asInstanceOf[String]
              val file3 = thisObj.get("drama_name").asInstanceOf[String]
              val file4 = thisObj.get("drama_type_name").asInstanceOf[String]
              val file5 = thisObj.get("media_area_name").asInstanceOf[String]
              //              val file6 = thisObj.get("media_release_date").toString
              val file7 = thisObj.get("class_type_id").toString
              val file8 = thisObj.get("class3_name").asInstanceOf[String]
              val file9 = thisObj.get("class2_name").asInstanceOf[String]
              val file10 = thisObj.get("ad_seat_img").asInstanceOf[String]
              val file11 = thisObj.get("project_id").asInstanceOf[String]
              val file12 = thisObj.get("department_id").asInstanceOf[Int]

              //            将组合后的标签，前后各拓展3s
              val newbegin =
                if ((beginTime.toLong - 3000L) < 0) 0.toString else (beginTime.toLong - 3000L).toString

              val newend = (endTime.toLong + 3000L).toString

              //            media_name索引字段
              tempJsonObj.put("string_vid", file1)
              tempJsonObj.put("media_name", file2)
              tempJsonObj.put("string_drama_name", file3)
              tempJsonObj.put("string_drama_type_name", file4)
              tempJsonObj.put("string_media_area_name", file5)
              //              tempJsonObj.put("string_media_release_date", file6)
              tempJsonObj.put("b_t", newbegin.toLong)
              tempJsonObj.put("string_time", newbegin + "_" + newend)
              tempJsonObj.put("string_time_long", newend.toLong - newbegin.toLong)
              tempJsonObj.put("resourceId", "1")
              tempJsonObj.put("project_id", file11)
              tempJsonObj.put("department_id", file12)

              class3List.add(file8)
              class2List.add(file9)
              classImgList.add(file10)

              file7 match {
                case "4" =>
                  manList.add(file8)
                  //                  man2List.add(file9)
                  //                  manImgList.add(file10)
                case "1" =>
                  objectList.add(file8)
                  //                  object2List.add(file9)
                  //                  objectImgList.add(file10)
                case "3" =>
                  actionList.add(file8)
                  //                  action2List.add(file9)
                  //                  actionImgList.add(file10)
                case "2" =>
                  senceList.add(file8)
                  //                  sence2List.add(file9)
                  //                  senceImgList.add(file10)
              }

              class3ToImg.put(file8, file10)

            }

            tempJsonObj.put("string_class3_list", class3List)
            tempJsonObj.put("string_man_list", manList)
            tempJsonObj.put("string_object_list", objectList)
            tempJsonObj.put("string_action_list", actionList)
            tempJsonObj.put("string_sence_list", senceList)

            tempJsonObj.put("string_class2_list", class2List)
//            tempJsonObj.put("string_man2_list", man2List)
//            tempJsonObj.put("string_object2_list", object2List)
//            tempJsonObj.put("string_action2_list", action2List)
//            tempJsonObj.put("string_sence2_list", sence2List)

            tempJsonObj.put("string_class_img_list", classImgList)
//            tempJsonObj.put("string_man_img_list", manImgList)
//            tempJsonObj.put("string_object_img_list", objectImgList)
//            tempJsonObj.put("string_action_img_list", actionImgList)
//            tempJsonObj.put("string_sence_img_list", senceImgList)

            tempJsonObj.put("string_class3_to_img", class3ToImg)

            tempJsonObj.put("string_frame_img_list", classImgList)


            resultList += ((videocutKey, tempJsonObj))


            // 设置本次结束的时间为下一批次的开始时间
            beginTime = endTime

          }

          // 处理完此point点前的片段后，然后针对此point点对tempMap操作
          if (pointType.equals("begin")) {
            // 如果是起始点，从videocutMap拿到对应数据，放到tempMap中
            tempMap += (videocutKey -> videocutMap(videocutKey))

          } else {
            // 如果是终止点，从tempMap中拿掉对应tempMap
            tempMap -= videocutKey

          }

        }

        // 最终返回resultList
        resultList
      })
      .flatMap(x => x.toArray[(String, JSONObject)])
      // 过滤掉时长小于1000毫秒的
      .filter(x => x._2.asInstanceOf[JSONObject].getInteger("string_time_long") >= 1000)
      .map(x => {
        x._2.toString
      })

    rst1.saveJsonToEs("video_wave/doc", Map(
      //            "es.index.auto.create" -> "true",
      "es.nodes" -> confUtil.adxStreamingEsHost,
      "es.user" -> confUtil.adxStreamingEsUser,
      "es.password" -> confUtil.adxStreamingEsPassword,
      "es.port" -> "9200"
      //                "es.mapping.id" -> ""
    ))
          logWarning("单标签存入ES成功")


          rst1.groupBy(str => {
            JSON.parseObject(str).getString("string_vid")
          }).foreach(x => {
            val vid = x._1
            val fragment = x._2.toList.size

            val sqlProxy = new SqlProxy()
            val client = DataSourceUtil.getConnection
            try {
              sqlProxy.executeUpdate(client, "update task set fragment=?,total=total+?,status=1 where video_id=?",
                Array(fragment,fragment,vid))
            }
            catch {
              case e: Exception => e.printStackTrace()
            } finally {
              sqlProxy.shutdown(client)
            }

          })
          logWarning("更新task表片段1数量成功")

    // 统计生成的视频片段数
//    var fragment = 0
//    rst1.mapPartitionsWithIndex {
//      (partIdx, iter) => {
//        val part_map = scala.collection.mutable.Map[String, Int]()
//        while (iter.hasNext) {
//          val part_name = "part_" + partIdx
//          if (part_map.contains(part_name)) {
//            val ele_cnt = part_map(part_name)
//            part_map(part_name) = ele_cnt + 1
//          } else {
//            part_map(part_name) = 1
//          }
//          iter.next()
//        }
//        part_map.iterator
//      }
//    }.collect.toList.foreach(
//      fragment += _._2
//    )


    /**
     * Cleaned2 多标签合成
     */
    val rst = spark.sql("select * from ccc")
      .rdd
      .map(x => AdSeat(
        x.get(0).toString,
        x.get(1).toString,
        x.get(2).toString,
        x.get(3).toString,
        x.get(4).toString,
        x.get(5).toString,
        x.get(6).toString,
        x.get(7).toString,
        x.get(8).toString,
        x.get(9).toString,
        x.get(10).toString,
        x.get(11).toString,
        x.get(12).toString))

      .map(x => {
        val videoID = x.video_id
        val class3Name = x.class3_name
        ((videoID, class3Name), x)
      })
      .groupByKey()

      /**
       * 间隔十秒内的相同标签都合并
       */
      .mapValues(x => {

        // 转换成数组然后排序
        val seatSorted = x.toList.sortBy(_.ad_seat_b_time.toLong)

        var temp: AdSeat = seatSorted.head
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
        val resultList = ListBuffer[ListBuffer[AdSeat]]()

        val temp = new ListBuffer[AdSeat]()
        temp.append(seatSorted.head)

        var maxETime: String = seatSorted.head.ad_seat_e_time

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
//      .map(x =>{
//        val list = x.toList
//        val resultList = ListBuffer[AdSeat]()
//        val head = x.toList.head.class3_name
//        val c3list = List(head)
//        for (i <- 1 until list.size) {
//          if (c3list.contains(list(i).class3_name)){
//            c3list.addString(list(i).class3_name)
//          }
//        }
//        resultList
//      })
      .map(x => CuterUtils3.seatToJSON(x))
      .filter(x => x.getInteger("string_time_long") >= 1000)
      .map(_.toString)

    //       多标签写入ES库
    rst.saveJsonToEs("video_wave/doc", Map(
      "es.index.auto.create" -> "true",
      "es.nodes" -> confUtil.adxStreamingEsHost,
      "es.port" -> "9200"
    ))

          logWarning("多标签存入ES成功")

          rst.groupBy(str => {
            JSON.parseObject(str).getString("string_vid")
          }).foreach(x => {
            val vid = x._1
            val fragment = x._2.toList.size

            val sqlProxy = new SqlProxy()
            val client = DataSourceUtil.getConnection
            try {
              sqlProxy.executeUpdate(client, "update task set fragment=fragment + ?,total=total+?,status=1 where video_id=?",
                Array(fragment,fragment,vid))
            }
            catch {
              case e: Exception => e.printStackTrace()
            } finally {
              sqlProxy.shutdown(client)
            }

          })

          logWarning("更新task表片段2数量成功")

    // 统计生成的视频片段数
//    rst.mapPartitionsWithIndex {
//      (partIdx, iter) => {
//        val part_map = scala.collection.mutable.Map[String, Int]()
//        while (iter.hasNext) {
//          val part_name = "part_" + partIdx
//          if (part_map.contains(part_name)) {
//            val ele_cnt = part_map(part_name)
//            part_map(part_name) = ele_cnt + 1
//          } else {
//            part_map(part_name) = 1
//          }
//          iter.next()
//        }
//        part_map.iterator
//      }
//    }.collect.toList.foreach(
//      fragment += _._2
//    )


    /**
     * 分镜头处理
     */

    // 1.未分组 a0
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
        |             story_end
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
        |                   kukai_videos.department_id department_id
        |            from recognition2_behavior
        |                     join recognition2_class
        |                          on recognition2_behavior.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_behavior.media_id
        |                     right join recognition2_videostory
        |                          on recognition2_behavior.media_id = recognition2_videostory.media_id
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
        |                   kukai_videos.department_id department_id
        |            from recognition2_face
        |                     join recognition2_class
        |                          on recognition2_face.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_face.media_id
        |                     right join recognition2_videostory
        |                          on recognition2_face.media_id = recognition2_videostory.media_id
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
        |                   kukai_videos.department_id department_id
        |            from recognition2_object
        |                     join recognition2_class
        |                          on recognition2_object.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_object.media_id
        |                     right join recognition2_videostory
        |                          on recognition2_object.media_id = recognition2_videostory.media_id
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
        |                   kukai_videos.department_id department_id
        |            from recognition2_scene
        |                     join recognition2_class
        |                          on recognition2_scene.class_id = recognition2_class.class_id
        |                     join kukai_videos
        |                          on kukai_videos.videoId = recognition2_scene.media_id
        |                     right join recognition2_videostory
        |                          on recognition2_scene.media_id = recognition2_videostory.media_id
        |           ) b
        |           where ad_seat_b_time >= story_start
        |              and ad_seat_e_time <= story_end
        |""".stripMargin)
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
        |       first(department_id) department_id
        |from a01
        |group by a01.video_id, media_name, drama_name, drama_type_name, class2_name, media_area_name, class_type_id, class3_name, story_start, story_end
        |""".stripMargin)
      .createOrReplaceTempView("a1")

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
                    |       concat_ws(',',collect_set(class2_name)) as class2_name
                    |FROM a3
                    |GROUP BY drama_name,drama_type_name,video_id,media_area_name,story_start,story_end,media_name
                    |""".stripMargin)
                  .createOrReplaceTempView("a4")

          spark.sql(
            """
              |select string_vid,SUBSTRING_INDEX(string_time,'_',1) bt
              |from a4
              |""".stripMargin)
              .createOrReplaceTempView("a")

          spark.sql(
            """
              |select media_id,story_start
              |from recognition2_videostory
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
            |       concat_ws('_',recognition2_videostory.story_start,recognition2_videostory.story_end)  string_time,
            |       recognition2_videostory.media_id  string_vid,
            |       kukai_videos.area string_media_area_name,
            |       recognition2_videostory.story_end-recognition2_videostory.story_start string_time_long,
            |       kukai_videos.videoName  media_name,
            |       kukai_videos.albumId  project_id,
            |       kukai_videos.department_id  department_id,
            |       recognition2_videostory.image image
            |from useless
            |left join recognition2_videostory
            |on useless.media_id=recognition2_videostory.media_id
            |and useless.story_start=recognition2_videostory.story_start
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
              newObject.put("string_man_list", new JSONArray())
              newObject.put("string_object_list", new JSONArray())
              newObject.put("string_action_list", new JSONArray())
              newObject.put("string_sence_list", new JSONArray())
              newObject.put("string_class2_list", new JSONArray())
              newObject.put("string_class_img_list", imglist)


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
                Array(storyNum,storyNum,vid))
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
                    newObject.put("string_man_list", manList)
                    newObject.put("string_object_list", objectList)
                    newObject.put("string_action_list", actionList)
                    newObject.put("string_sence_list", senceList)
                    newObject.put("string_class2_list", string_class2_list)
                    newObject.put("string_class_img_list", string_class_img_list)


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
                Array(storyNum,storyNum,vid))
            }
            catch {
              case e: Exception => e.printStackTrace()
            } finally {
              sqlProxy.shutdown(client)
            }

          })
          logWarning("更新task表分镜头数量成功")



//          val jSONArray = new JSONArray()
//
//          rst2.collect().foreach(x => {
//            val nObject = JSON.parseObject(x)
//
//            val vid = nObject.getString("string_vid")
//            jSONArray.add()
//          })


//          // 统计生成的分镜头数
//          var story = 0
//          rst1.mapPartitionsWithIndex {
//            (partIdx, iter) => {
//              val part_map = scala.collection.mutable.Map[String, Int]()
//              while (iter.hasNext) {
//                val part_name = "part_" + partIdx
//                if (part_map.contains(part_name)) {
//                  val ele_cnt = part_map(part_name)
//                  part_map(part_name) = ele_cnt + 1
//                } else {
//                  part_map(part_name) = 1
//                }
//                iter.next()
//              }
//              part_map.iterator
//            }
//          }.collect.toList.foreach(
//            story += _._2
//          )

//           修改task表status为1
//                rst.foreach(s => {
//                  //              println(s)
////                  val i = s.indexOf("string_vid")
////                  val vid = s.substring(i + 13, i + 13 + 36)
//
//
//                  val sqlProxy = new SqlProxy()
//                  val client = DataSourceUtil.getConnection
//                  try {
//                    sqlProxy.executeUpdate(client, "replace into `task` (video_id,fragment,story,total,status) values(?,?,?,?,1)",
//                      Array(storyArray.getJSONObject(),fragment,storyRdd.collect(),fragment+story))
//                  }
//                  catch {
//                    case e: Exception => e.printStackTrace()
//                  } finally {
//                    sqlProxy.shutdown(client)
//                  }
//                })
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

          Thread.sleep(5000)

    spark.close()
        }
  }
}
