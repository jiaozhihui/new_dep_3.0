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

    // 0.读取任务表
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

    //注册mysql的广告位表，从mysql中拿到广告位数据
    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "clip_tpl_class", "clip_tpl_class")

    // 加载索引表 addx，从es中拿到正在投放的计划数据
    TableRegister.registEsTable(spark, confUtil.adxStreamingEsHost, "9200",
      confUtil.adxStreamingEsUser, confUtil.adxStreamingEsPassword, confUtil.adxStreamingEsIndex, "video_wave")


    // 拿到将要查询的标签
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
    //      .show()

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
         |       row_number() OVER (PARTITION BY label_id ORDER BY label_id DESC) rank
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
    //        .show(1000,false)
    /**
     * +------+--------+------------------------------------+----------------+------------------+----------------+---------------+----------+-----+--------+---------+--------+----+
     * |tpl_id|label_id|string_vid                          |media_name      |string_class3_list|string_time_long|string_time    |resolution|frame|timeLong|totalLong|seat_num|rank|
     * +------+--------+------------------------------------+----------------+------------------+----------------+---------------+----------+-----+--------+---------+--------+----+
     * |1     |label1  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[沙发]            |21660           |733120_754780  |1280*720  |20   |21000   |100000   |3       |1   |
     * |2     |label1  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[沙发]            |21660           |733120_754780  |1280*720  |20   |21000   |100000   |3       |2   |
     * |1     |label2  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[王丽坤, 朱亚文]  |14900           |144880_159780  |1280*720  |20   |15000   |100000   |3       |1   |
     * |2     |label2  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[王丽坤, 朱亚文]  |14900           |144880_159780  |1280*720  |20   |15000   |100000   |3       |2   |
     * |1     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |15660           |2535920_2551580|1280*720  |20   |15000   |100000   |3       |1   |
     * |2     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |14940           |2413440_2428380|1280*720  |20   |15000   |100000   |3       |4   |
     * |2     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |15660           |2535920_2551580|1280*720  |20   |15000   |100000   |3       |2   |
     * |1     |label3  |0199a921-7893-4cab-ba7f-c1ef91146633|漂洋过海来看你12|[电脑椅]          |14940           |2413440_2428380|1280*720  |20   |15000   |100000   |3       |3   |
     * +------+--------+------------------------------------+----------------+------------------+----------------+---------------+----------+-----+--------+---------+--------+----+
     */

    // 木桶高度标注(count(rank))
    spark.sql(
      """
        |select first(tpl_id) tpl_id,
        |       collect_list(label_id) as label_id,
        |       collect_list(string_vid) as string_vid,
        |       collect_list(media_name) as media_name,
        |       collect_list(concat_ws(';',string_class3_list)) as string_class3_list,
        |       collect_list(string_time_long) as string_time_long,
        |       collect_list(string_time) as string_time,
        |       collect_list(resolution) as resolution,
        |       collect_list(frame) as frame,
        |       collect_list(timeLong) as timeLong,
        |       first(totalLong) totalLong,
        |       count(rank) count,
        |       first(seat_num) seat_num
        |from ranked
        |group by rank
        |""".stripMargin)
      .createOrReplaceTempView("countRank")
    //        .show(false)
    /**
     * +------+------------------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------+-----------------------------+---------------------+-----------------------------------------------+------------------------------+------------+---------------------+---------+-----+--------+
     * |tpl_id|label_id                |string_vid                                                                                                        |media_name                                            |string_class3_list           |string_time_long     |string_time                                    |resolution                    |frame       |timeLong             |totalLong|count|seat_num|
     * +------+------------------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------+-----------------------------+---------------------+-----------------------------------------------+------------------------------+------------+---------------------+---------+-----+--------+
     * |1     |[label1, label2, label3]|[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12, 漂洋过海来看你12]|[沙发, 王丽坤;朱亚文, 电脑椅]|[21660, 14900, 15660]|[733120_754780, 144880_159780, 2535920_2551580]|[1280*720, 1280*720, 1280*720]|[20, 20, 20]|[21000, 15000, 15000]|100000   |3    |3       |
     * |1     |[label3]                |[0199a921-7893-4cab-ba7f-c1ef91146633]                                                                            |[漂洋过海来看你12]                                    |[电脑椅]                     |[14940]              |[2413440_2428380]                              |[1280*720]                    |[20]        |[15000]              |100000   |1    |3       |
     * |2     |[label3]                |[0199a921-7893-4cab-ba7f-c1ef91146633]                                                                            |[漂洋过海来看你12]                                    |[电脑椅]                     |[14940]              |[2413440_2428380]                              |[1280*720]                    |[20]        |[15000]              |100000   |1    |3       |
     * |2     |[label1, label2, label3]|[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12, 漂洋过海来看你12]|[沙发, 王丽坤;朱亚文, 电脑椅]|[21660, 14900, 15660]|[733120_754780, 144880_159780, 2535920_2551580]|[1280*720, 1280*720, 1280*720]|[20, 20, 20]|[21000, 15000, 15000]|100000   |3    |3       |
     * +------+------------------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------+-----------------------------+---------------------+-----------------------------------------------+------------------------------+------------+---------------------+---------+-----+--------+
     */

    // 木桶取最低高度,并压缩
    spark.sql(
      """
        |select *
        |from countRank
        |where count=seat_num
        |""".stripMargin)
      //        .show(false)

      /**
       * +------+------------------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------+-----------------------------+---------------------+-----------------------------------------------+------------------------------+------------+---------------------+---------+-----+--------+
       * |tpl_id|label_id                |string_vid                                                                                                        |media_name                                            |string_class3_list           |string_time_long     |string_time                                    |resolution                    |frame       |timeLong             |totalLong|count|seat_num|
       * +------+------------------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------+-----------------------------+---------------------+-----------------------------------------------+------------------------------+------------+---------------------+---------+-----+--------+
       * |1     |[label1, label2, label3]|[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12, 漂洋过海来看你12]|[沙发, 王丽坤;朱亚文, 电脑椅]|[21660, 14900, 14940]|[733120_754780, 144880_159780, 2413440_2428380]|[1280*720, 1280*720, 1280*720]|[20, 20, 20]|[21000, 15000, 15000]|100000   |3    |3       |
       * |2     |[label1, label2, label3]|[0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633, 0199a921-7893-4cab-ba7f-c1ef91146633]|[漂洋过海来看你12, 漂洋过海来看你12, 漂洋过海来看你12]|[沙发, 王丽坤;朱亚文, 电脑椅]|[21660, 14900, 14940]|[733120_754780, 144880_159780, 2413440_2428380]|[1280*720, 1280*720, 1280*720]|[20, 20, 20]|[21000, 15000, 15000]|100000   |3    |3       |
       * +------+------------------------+------------------------------------------------------------------------------------------------------------------+------------------------------------------------------+-----------------------------+---------------------+-----------------------------------------------+------------------------------+------------+---------------------+---------+-----+--------+
       */


      .rdd
      // 包装成json形式
      .map(x => {

        val tplArray = new JSONArray()

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
        val count = x.get(11).toString.toInt
        val seat_num = x.get(12).toString.toInt


        for (i <- 0 until seat_num) {

          val tplObject = new JSONObject()

          tplObject.put("tpl_id", tpl_id)
          tplObject.put("label_id", label_id(i))
          tplObject.put("string_vid", string_vid(i))
          tplObject.put("media_name", media_name(i))
          tplObject.put("string_class3_list", string_class3_list(i))
          tplObject.put("string_time_long", string_time_long(i))
          tplObject.put("string_time", string_time(i))
          tplObject.put("resolution", resolution(i))
          tplObject.put("frame", frame(i))
          tplObject.put("timeLong", timeLong(i))
          tplObject.put("totalLong", totalLong)
          tplObject.put("count", count)

          tplArray.add(tplObject)

        }
        val resultJson = tplArray.toString.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",")
        resultJson
      })
      //      .collect().foreach(println)
      /**
       * [{"string_time":"733120_754780","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"21000","totalLong":100000,"tpl_id":"1","count":3,"string_time_long":"21660","media_name":"漂洋过海来看你12","string_class3_list":"沙发","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":" 144880_159780","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"1","count":3,"string_time_long":" 14900","media_name":" 漂洋过海来看你12","string_class3_list":" 王丽坤,朱亚文","resolution":" 1280*720","label_id":" label2","frame":" 20"},{"string_time":" 2535920_2551580","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"1","count":3,"string_time_long":" 15660","media_name":" 漂洋过海来看你12","string_class3_list":" 电脑椅","resolution":" 1280*720","label_id":" label3","frame":" 20"}]
       * [{"string_time":"733120_754780","string_vid":"0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":"21000","totalLong":100000,"tpl_id":"2","count":3,"string_time_long":"21660","media_name":"漂洋过海来看你12","string_class3_list":"沙发","resolution":"1280*720","label_id":"label1","frame":"20"},{"string_time":" 144880_159780","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"2","count":3,"string_time_long":" 14900","media_name":" 漂洋过海来看你12","string_class3_list":" 王丽坤,朱亚文","resolution":" 1280*720","label_id":" label2","frame":" 20"},{"string_time":" 2535920_2551580","string_vid":" 0199a921-7893-4cab-ba7f-c1ef91146633","timeLong":" 15000","totalLong":100000,"tpl_id":"2","count":3,"string_time_long":" 15660","media_name":" 漂洋过海来看你12","string_class3_list":" 电脑椅","resolution":" 1280*720","label_id":" label3","frame":" 20"}]
       */

      // 将结果保存,更新mysql表数据
      .groupBy(str => {
        val i = str.indexOf("tpl_id")
        val tpl_id = str.substring(i + 9, i + 10)
        tpl_id
        //        JSON.parseObject(str).getString("tpl_id")
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

  }

  //  }
}
