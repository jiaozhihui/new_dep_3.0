package com.bjvca.filmedit

import com.bjvca.commonutils.{ConfUtils, DataSourceUtil, SqlProxy, TableRegister}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

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
          |       total_duration totalLong,
          |       seat_num
          |from clip_tpl_class
          |join clip_task
          |on clip_task.tpl_id=clip_tpl_class.tpl_id
          |""".stripMargin)
      temp.createOrReplaceTempView("target")
//            .show()

    /**
     * +------+--------+----------------+--------+---------+--------+
     * |tpl_id|label_id|             arr|timeLong|totalLong|seat_num|
     * +------+--------+----------------+--------+---------+--------+
     * |     1|  label1|          [沙发]|      21|      100|       3|
     * |     1|  label2|[王丽坤, 朱亚文]|      36|      100|       3|
     * |     1|  label3|        [电脑椅]|      15|      100|       3|
     * |     2|  label1|          [沙发]|      21|      100|       3|
     * |     2|  label2|[王丽坤, 朱亚文]|      36|      100|       3|
     * |     2|  label3|        [电脑椅]|      15|      100|       3|
     * |     3|  label1|        [王丽坤]|      15|      100|       1|
     * +------+--------+----------------+--------+---------+--------+
     */

      // 搜索(根据限制条件关联mysql和ES),挑选时长符合的片段
      spark.sql(
        s"""
           |select *
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
           |  ) b
           |where string_time_long >= timeLong-1000
           |and string_time_long <= timeLong+1000
           |order by tpl_id,label_id
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
        |select ranked.*
        |from ranked
        |join resolution_target
        |on resolution_target.tpl_id=ranked.tpl_id
        |and resolution_target.resolution=ranked.resolution
        |order by tpl_id,label_id
        |""".stripMargin)
        .createOrReplaceTempView("ranked1")
//      .show(1000,false)


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
          |       collect_set(resolution) as resolution,
          |       first(timeLong) as timeLong,
          |       first(totalLong) totalLong,
          |       count(*) count,
          |       first(seat_num) seat_num
          |from ranked1
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


    val frame: DataFrame = spark.sql(
      """
        |select array_distinct(split(concat_ws(',',collect_set(concat_ws(',',resolution))),',')) as resolution
        |from boardLength
        |""".stripMargin)

    val listBuffer = new ArrayBuffer[String]
    frame.rdd.foreach(row => {
      val list = row.getList[String](0)
      for (i <- 0 until list.size) {
        listBuffer += list.get(i)
      }
    })



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
        //分条插入mysql
        .foreach(x => {

          // 分割每个label
          val tpl_id = x.get(0).toString
          val label_id = x.get(1).toString.split(',').toList
          val string_vid = x.get(2).toString.split(',').toList
          val media_name = x.get(3).toString.split(',').toList
          val string_class3_list = x.get(4).toString.split(',').toList
          val string_time_long = x.get(5).toString.split(',').toList
          val string_time = x.get(6).toString.split(',').toList
          val resolution = x.get(7).toString.split(',').toList
          val timeLong = x.get(8).toString.split(',').toList
          val totalLong = x.get(9).toString.toInt
          val shortSlab = x.get(10).toString.toInt
          val seat_num = x.get(11).toString.toInt

          /**
           * 核心逻辑
           * 如果seat_num = 1，并且shortSlab > totalLong/timeLong ，则生成 totalLong/timeLong 段组合视频，否则根据shortSlab生成组合数
           */

          // 只有单个标签位的特殊处理
          if (seat_num == 1) {
            // 得到需要插入的条数
            val clipNum = totalLong / timeLong.head.split(';').head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",").toInt
            // 得到最多可以生成的成品数
            val pid = shortSlab / clipNum

            for (x <- 0 until pid) {
              // 对于每个pid,插入对应的条数
              for (i <- 0 until clipNum) {

                val sqlProxy = new SqlProxy()
                val client = DataSourceUtil.getConnection
                try {
                  sqlProxy.executeUpdate(client, s"insert into clip_tpl_result values(null,?,?,?,?,?,?,null,?,?,?,null)",
                    Array(tpl_id,
                      label_id(0).split(';').head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      x,
                      media_name.head.split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      resolution.head.split(';').toList.head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_class3_list.head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_time.head.split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_time_long.head.split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_vid.head.split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",")))
                }
                catch {
                  case e: Exception => e.printStackTrace()
                } finally {
                  sqlProxy.shutdown(client)
                }
              }
            }
          } else {

            // 得到需要插入的条数
            val clipNum = seat_num
            // 得到最多可以生成的成品数
            val pid = shortSlab

            for (x <- 0 until pid) {
              // 对于每个pid,插入对应的条数
              for (i <- 0 until clipNum) {

                val sqlProxy = new SqlProxy()
                val client = DataSourceUtil.getConnection
                try {
                  sqlProxy.executeUpdate(client, s"insert into clip_tpl_result values(null,?,?,?,?,?,?,null,?,?,?,null)",
                    Array(tpl_id,
                      label_id(i).split(';').head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      x,
                      media_name(i).split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      resolution(i).split(';').toList.head.replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_class3_list(i).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_time(i).split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_time_long(i).split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ","),
                      string_vid(i).split(';').toList(x).replaceAll("WrappedArray", "").replace("(", "").replace(")", "").replace(";", ",")))
                }
                catch {
                  case e: Exception => e.printStackTrace()
                } finally {
                  sqlProxy.shutdown(client)
                }
              }
            }


          }
        })


      spark.close()
      logWarning("End")

//    }

  }
}
