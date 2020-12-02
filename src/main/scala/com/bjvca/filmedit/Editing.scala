package com.bjvca.filmedit

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bjvca.commonutils.{ConfUtils, DataSourceUtil, SqlProxy, TableRegister}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Editing extends Logging {
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
        .set("es.read.field.as.array.include", "string_class3_list,class3_arr") //数组

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
          |       resolution,
          |       frame,
          |       total_duration totalLong,
          |       seat_num
          |from clip_tpl_class
          |join clip_task
          |on clip_task.tpl_id=clip_tpl_class.tpl_id
          |""".stripMargin)
      temp.createOrReplaceTempView("target")
//            .show()
      /**
       * +------+--------+----------------+--------+----------+-----+---------+--------+
       * |tpl_id|label_id|             arr|timeLong|resolution|frame|totalLong|seat_num|
       * +------+--------+----------------+--------+----------+-----+---------+--------+
       * |     1|  label1|          [沙发]|      21|  1280*720|   20|      100|       3|
       * |     1|  label2|[王丽坤, 朱亚文]|      15|  1280*720|   20|      100|       3|
       * |     1|  label3|        [电脑椅]|      15|  1280*720|   20|      100|       3|
       * |     2|  label1|          [沙发]|      21|  1280*720|   20|      100|       3|
       * |     2|  label2|[王丽坤, 朱亚文]|      15|  1280*720|   20|      100|       3|
       * |     2|  label3|        [电脑椅]|      15|  1280*720|   20|      100|       3|
       * +------+--------+----------------+--------+----------+-----+---------+--------+
       **/

      // 搜索(根据限制条件关联mysql和ES)
      spark.sql(
        s"""
           |select *,
           |       row_number() OVER (PARTITION BY tpl_id,label_id ORDER BY label_id DESC) rank
           |from (
           |  select tpl_id,
           |         label_id,
           |         string_vid,
           |         media_name,
           |         arr string_class3_list,
           |         string_time_long,
           |         string_time,
           |         video_wave.resolution,
           |         video_wave.frame,
           |         timeLong*1000 timeLong,
           |         totalLong*1000 totalLong,
           |         seat_num
           |  from video_wave
           |  join target
           |  on array_intersect(string_class3_list,arr)=arr
           |  where video_wave.resolution=target.resolution
           |  and video_wave.frame=target.frame
           |  ) b
           |where string_time_long >= timeLong-1000
           |and string_time_long <= timeLong+1000
           |order by label_id
           |""".stripMargin)
        .createOrReplaceTempView("ranked")
//              .show(1000,false)
      /**
       * +------+--------+------------------------------------+----------------+------------------+----------------+---------------+----------+-----+--------+---------+--------+----+
       * |tpl_id|label_id|string_vid                          |media_name      |string_class3_list|string_time_long|string_time    |resolution|frame|timeLong|totalLong|seat_num|rank|
       * +------+--------+------------------------------------+----------------+------------------+----------------+---------------+----------+-----+--------+---------+--------+----+
       * |2     |label1  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[沙发]            |21660           |733120_754780  |1280*720  |20   |21000   |100000   |3       |1   |
       * |1     |label1  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[沙发]            |21660           |733120_754780  |1280*720  |20   |21000   |100000   |3       |1   |
       * |1     |label2  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[王丽坤, 朱亚文]  |14900           |144880_159780  |1280*720  |20   |15000   |100000   |3       |1   |
       * |2     |label2  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[王丽坤, 朱亚文]  |14900           |144880_159780  |1280*720  |20   |15000   |100000   |3       |1   |
       * |2     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |14940           |2413440_2428380|1280*720  |20   |15000   |100000   |3       |1   |
       * |1     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |14940           |2413440_2428380|1280*720  |20   |15000   |100000   |3       |1   |
       * |2     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |15660           |2535920_2551580|1280*720  |20   |15000   |100000   |3       |2   |
       * |1     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |15660           |2535920_2551580|1280*720  |20   |15000   |100000   |3       |2   |
       * +------+--------+------------------------------------+----------------+------------------+----------------+---------------+----------+-----+--------+---------+--------+----+
       */

      // 每个木板高度标注(count(*)),每个标签搜出来的数量 -> count,即每个木板的高度
      spark.sql(
        """
          |select tpl_id,
          |       label_id,
          |       collect_list(string_vid) as string_vid,
          |       collect_list(media_name) as media_name,
          |       collect_list(string_time) as string_time,
          |       collect_list(string_time_long) as string_time_long,
          |       first(concat_ws('/',string_class3_list)) as string_class3_list,
          |       first(resolution) as resolution,
          |       first(frame) as frame,
          |       first(timeLong) as timeLong,
          |       first(totalLong) totalLong,
          |       count(*) count,
          |       first(seat_num) seat_num
          |from ranked
          |group by tpl_id,label_id
          |""".stripMargin)
                .createOrReplaceTempView("boardLength")
//        .show(false)
      /**
       * +------+--------+----------------------------------------------------------------------------+------------------------------------------------+----------------------------------------+-----------------------------+------------------+----------+-----+--------+---------+-----+--------+
       * |tpl_id|label_id|string_vid                                                                  |media_name                                     |string_time                             |string_time_long             |string_class3_list|resolution|frame|timeLong|totalLong|count|seat_num|
       * +------+--------+----------------------------------------------------------------------------+-----------------------------------------------+----------------------------------------+-----------------------------+------------------+----------+-----+--------+---------+-----+--------+
       * |3     |label1  |[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12, 漂洋过海来看你12] |[31880_47140, 245120_260460]            |[15260, 15340, 15300, 14900]  |王丽坤            |1280*720  |20   |15000   |100000   |10   |1       |
       * |1     |label1  |[0199a921-7893-4cab-ba7f-c1ef91146633]                                      |[漂洋过海来看你12]                                |[733120_754780]                         |[21660]                      |沙发              |1280*720  |20   |21000   |100000   |1    |3       |
       * |2     |label1  |[0199a921-7893-4cab-ba7f-c1ef91146633]                                      |[漂洋过海来看你12]                                |[733120_754780]                         |[21660]                      |沙发              |1280*720  |20   |21000   |100000   |1    |3       |
       * |1     |label2  |[0199a921-7893-4cab-ba7f-c1ef91146633]                                      |[漂洋过海来看你12]                                |[144880_159780]                         |[14900]                      |王丽坤/朱亚文     |1280*720  |20   |15000   |100000   |1    |3       |
       * |2     |label2  |[0199a921-7893-4cab-ba7f-c1ef91146633]                                      |[漂洋过海来看你12]                                |[144880_159780]                         |[14900]                      |王丽坤/朱亚文     |1280*720  |20   |15000   |100000   |1    |3       |
       * |1     |label3  |[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12]                 |[2535920_2551580, 2413440_2428380]      |[15660, 14940]               |电脑椅            |1280*720  |20   |15000   |100000   |2    |3       |
       * |2     |label3  |[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12]                 |[2535920_2551580, 2413440_2428380]      |[15660, 14940]               |电脑椅            |1280*720  |20   |15000   |100000   |2    |3       |
       * +------+--------+----------------------------------------------------------------------------+-----------------------------------------------+-----------------------------------------+-----------------------------+----------------+----------+-----+--------+---------+-----+--------+
       */

      // 得到木桶的短板高度(shortSlab),并将每个模板压缩
      spark.sql(
        """
          |select tpl_id,
          |       collect_list(label_id) as label_id,
          |       collect_list(concat_ws(';',string_vid)) as string_vid,
          |       collect_list(concat_ws(';',media_name)) as media_name,
          |       collect_list(concat_ws(';',string_class3_list)) as string_class3_list,
          |       collect_list(concat_ws(';',string_time_long)) as string_time_long,
          |       collect_list(concat_ws(';',string_time)) as string_time,
          |       collect_list(concat_ws(';',resolution)) as resolution,
          |       collect_list(concat_ws(';',frame)) as frame,
          |       collect_list(concat_ws(';',timeLong)) as timeLong,
          |       first(totalLong) totalLong,
          |       min(count) shortSlab,
          |       first(seat_num) seat_num
          |from boardLength
          |group by tpl_id
          |""".stripMargin)
//          .show(false)
        /**
         * +------+----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+--------+--------------+---------+---------+--------+
         * |tpl_id|label_id        |string_vid                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |media_name                                                                                                                                                                                                                    |string_class3_list|string_time_long                                                               |string_time                                                                                                                                                                                    |resolution          |frame   |timeLong      |totalLong|shortSlab|seat_num|
         * +------+----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+--------+--------------+---------+---------+--------+
         * |1     |[label1, label3]|[dea74128-bcfe-4671-b49e-80672e834d6f, 764e2f3b-735d-4107-8b5b-fe6d672c5bf6;d236507e-1285-430e-9f4a-732506e89231;b36a4cca-6fb1-44f5-9334-ae3fb55da6e3;d236507e-1285-430e-9f4a-732506e89231]                                                                                                                                                                                                                                                                                                       |[乡村爱情11 15, 欢乐颂2;特别任务10;飞行少年8;特别任务10]                                                                                                                                                                      |[沙发, 电脑椅]    |[21140, 15860;15500;15980;14900]                                               |[1478280_1499420, 2308680_2324540;891040_906540;2291199_2307179;2246040_2260940]                                                                                                               |[1280*720, 1280*720]|[20, 20]|[21000, 15000]|100000   |1        |3       |
         * |3     |[label1]        |[0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633;0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12;漂洋过海来看你12]|[王丽坤]          |[14980;15340;15260;14900;15420;14940;14900;15060;15820;15260;15380;14900;15300]|[206640_221620;245120_260460;298320_313580;437080_451980;2524720_2540140;399880_414820;416440_431340;1416280_1431340;1845120_1860940;31880_47140;261440_276820;1917520_1932420;1927480_1942780]|[1280*720]          |[20]    |[15000]       |100000   |13       |1       |
         * |2     |[label1, label3]|[dea74128-bcfe-4671-b49e-80672e834d6f, 764e2f3b-735d-4107-8b5b-fe6d672c5bf6;d236507e-1285-430e-9f4a-732506e89231;b36a4cca-6fb1-44f5-9334-ae3fb55da6e3;d236507e-1285-430e-9f4a-732506e89231]                                                                                                                                                                                                                                                                                                       |[乡村爱情11 15, 欢乐颂2;特别任务10;飞行少年8;特别任务10]                                                                                                                                                                      |[沙发, 电脑椅]    |[21140, 15860;15500;15980;14900]                                               |[1478280_1499420, 2308680_2324540;891040_906540;2291199_2307179;2246040_2260940]                                                                                                               |[1280*720, 1280*720]|[20, 20]|[21000, 15000]|100000   |1        |3       |
         * +------+----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+--------+--------------+---------+---------+--------+
         */

        .rdd
        // 包装成json形式
        .map(x => {

          val resultObj = new JSONObject()

          val tpl_id = x.get(0).toString
          val label_id = x.get(1).toString.split(',').toList
          val string_vid = x.get(2).toString.split(',').toList
          val media_name = x.get(3).toString.split(',').toList
          val string_class3_list = x.get(4).toString.split(',').toList
          val string_time_long = x.get(5).toString.split(',').toList
          val string_time = x.get(6).toString.split(',').toList
          val resolution = x.get(7).toString.split(',').toList
          val frame = x.get(8).toString.split(',').toList
          val timeLong = x.get(9).toString.split(',').toList
          val totalLong = x.get(10).toString.toInt
          val shortSlab = x.get(11).toString.toInt
          val seat_num = x.get(12).toString.toInt

          /**
           * 核心逻辑
           * 如果seat_num = 1，并且shortSlab > totalLong/timeLong ，则生成 totalLong/timeLong 段组合视频，否则根据shortSlab生成组合数
           */

          // 只有单个标签位的特殊处理
          if (seat_num == 1) {
            val clipNum = totalLong / timeLong.head.split(';').head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",").toInt

            for (x <- 0 until shortSlab/clipNum) {

                val tplArray = new JSONArray()

                // 根据标签位n，将n段视频拼接成一个Json
                for (_ <- 0 until clipNum) {

                  val tplObject = new JSONObject()

                  tplObject.put("tpl_id", tpl_id)
                  tplObject.put("label_id", label_id.head.split(';').head)
                  tplObject.put("string_vid", string_vid.head.split(';').toList(x))
                  tplObject.put("media_name", media_name.head.split(';').toList(x))
                  tplObject.put("string_class3_list", string_class3_list.head)
                  tplObject.put("string_time_long", string_time_long.head.split(';').toList(x))
                  tplObject.put("string_time", string_time.head.split(';').toList(x))
                  tplObject.put("resolution", resolution.head.split(';').toList.head)
                  tplObject.put("frame", frame.head.split(';').toList.head)
                  tplObject.put("timeLong", timeLong.head.split(';').toList.head)
                  tplObject.put("totalLong", totalLong)
                  tplObject.put("shortSlab", shortSlab)

                  tplArray.add(tplObject)

                }
                val resultJson = tplArray.toString.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",")

                val nObject = JSON.parseArray(resultJson)
                resultObj.put((x + 1).toString, nObject)

              }
              resultObj.toString
              resultObj

          } else {

            for (x <- 0 until shortSlab) {

              val tplArray = new JSONArray()

              // 根据标签位n，将n段视频拼接成一个Json
              for (i <- 0 until seat_num) {

                val tplObject = new JSONObject()

                tplObject.put("tpl_id", tpl_id)
                tplObject.put("label_id", label_id(i).split(';').head)
                tplObject.put("string_vid", string_vid(i).split(';').toList(x))
                tplObject.put("media_name", media_name(i).split(';').toList(x))
                tplObject.put("string_class3_list", string_class3_list(i))
                tplObject.put("string_time_long", string_time_long(i).split(';').toList(x))
                tplObject.put("string_time", string_time(i).split(';').toList(x))
                tplObject.put("resolution", resolution(i).split(';').toList.head)
                tplObject.put("frame", frame(i).split(';').toList.head)
                tplObject.put("timeLong", timeLong(i).split(';').toList.head)
                tplObject.put("totalLong", totalLong)
                tplObject.put("shortSlab", shortSlab)

                tplArray.add(tplObject)

              }
              val resultJson = tplArray.toString.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",")

              val nObject = JSON.parseArray(resultJson)
              resultObj.put((x+1).toString,nObject)

            }
          resultObj.toString
          resultObj
          }

        })
//              .collect().foreach(println)
        /**
         * [{"string_time":"733120_754780","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"21000","totalLong":100000,"tpl_id":"1","count":1,"string_time_long":"21660","media_name":"漂洋过海来看你12","string_class3_list":"沙发","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":" 144880_159780","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"1","count":1,"string_time_long":" 14900","media_name":" 漂洋过海来看你12","string_class3_list":" 王丽坤","resolution":" 1280*720","label_id":" label2","frame":" 20"},{"string_time":" 2413440_2428380","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"1","count":1,"string_time_long":" 14940","media_name":" 漂洋过海来看你12","string_class3_list":" 电脑椅","resolution":" 1280*720","label_id":" label3","frame":" 20"}]
         * [{"string_time":"298320_313580","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"15000","totalLong":100000,"tpl_id":"3","count":10,"string_time_long":"15260","media_name":"漂洋过海来看你12","string_class3_list":"王丽坤","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":"1938040_1953580","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"15000","totalLong":100000,"tpl_id":"3","count":10,"string_time_long":"15540","media_name":"漂洋过海来看你12","string_class3_list":"王丽坤","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":"1591040_1606740","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"15000","totalLong":100000,"tpl_id":"3","count":10,"string_time_long":"15700","media_name":"漂洋过海来看你12","string_class3_list":"王丽坤","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":"1845120_1860940","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"15000","totalLong":100000,"tpl_id":"3","count":10,"string_time_long":"15820","media_name":"漂洋过海来看你12","string_class3_list":"王丽坤","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":"2524720_2540140","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"15000","totalLong":100000,"tpl_id":"3","count":10,"string_time_long":"15420","media_name":"漂洋过海来看你12","string_class3_list":"王丽坤","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":"31880_47140","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"15000","totalLong":100000,"tpl_id":"3","count":10,"string_time_long":"15260","media_name":"漂洋过海来看你12","string_class3_list":"王丽坤","resolution":"1280*720","label_id":"label1","frame":"20"}]
         * [{"string_time":"733120_754780","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"21000","totalLong":100000,"tpl_id":"2","count":1,"string_time_long":"21660","media_name":"漂洋过海来看你12","string_class3_list":"沙发","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":" 144880_159780","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"2","count":1,"string_time_long":" 14900","media_name":" 漂洋过海来看你12","string_class3_list":" 王丽坤","resolution":" 1280*720","label_id":" label2","frame":" 20"},{"string_time":" 2413440_2428380","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"2","count":1,"string_time_long":" 14940","media_name":" 漂洋过海来看你12","string_class3_list":" 电脑椅","resolution":" 1280*720","label_id":" label3","frame":" 20"}]
         */

        // 将结果保存,更新mysql表数据
        .groupBy(str => {
          val i = str.toString.indexOf("tpl_id")
          val tpl_id = str.toString.substring(i + 9, i + 10)
          tpl_id
        })
        .foreach(x => {
          val tpl_id = x._1
          val num = x._2.toList.size
          val rst = x._2.toList.toString.replaceAll("List", "").replace("(", "").replace(")", "")

          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update `clip_tpl` set num=?,status=1,result=? where tpl_id = ?",
              Array(num, rst, tpl_id))
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
