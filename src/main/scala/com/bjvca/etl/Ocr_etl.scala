package com.bjvca.etl

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import com.bjvca.commonutils.{ConfUtils, ConfigurationManager, DataSourceUtil, SqlProxy, TableRegister}
import org.apache.spark.sql.{Row, SparkSession}

object Ocr_etl {
  def main(args: Array[String]): Unit = {

    val confUtil = new ConfUtils("application58.conf")

    // 创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Demo")
      .master("local[*]")
      .config("es.net.proxy.http.use.system.props", "false")
      .config("spark.debug.maxToStringFields", "2000")
      .getOrCreate()

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "ocr_offset", "ocr_offset")

    TableRegister.registMysqlTable(spark, confUtil.videocutMysqlHost, confUtil.videocutMysqlDb,
      confUtil.videocutMysqlUser, confUtil.videocutMysqlPassword, "recognition2_ocr", "recognition2_ocr")

    // 从offset处读取数据,去中文和空格
    spark.sql(
      """
        |select id,platform_id,project_id,media_id,lines_start,lines_end,condfid,trim(regexp_replace(OCR_content, '[a-zA-Z]+', '')) OCR_content,offset
        |from recognition2_ocr
        |join ocr_offset
        |on id > offset
        |""".stripMargin)
//      .show(100,false)
      .createOrReplaceTempView("ocr")

    // 获取最大偏移量 max_offset
    var max_offset = 0
    spark.sql(
      """
        |select max(id) max_offset
        |from ocr
        |""".stripMargin)
      //        .createOrReplaceTempView("cur_offset")
      .collect.foreach(row => {
      max_offset = row.getInt(0)
    })

    spark.udf.register("get_similar", (str1: String, str2: String) => {

      val array1 = if (null != str1) {
        str1.toCharArray.filter(ch => ch != ' ')
      } else {
        Array.empty
      }
      val array2 = if (null != str2) {
        str2.toCharArray.filter(ch => ch != ' ')
      } else {
        Array.empty
      }
      val inter_chars = array1.intersect(array2)
      val i = (inter_chars.length.toDouble / (array1.length + array2.length)).toFloat
      i * 2

    })

    val noSame = spark.sql(
      """
        |select *,ROW_NUMBER() OVER (ORDER BY t1.id ASC) row_num
        |from
        | (select t1.id,t1.platform_id,t1.project_id,t1.media_id,t1.lines_start,t1.lines_end,t1.OCR_content,t1.condfid,get_similar(t1.OCR_content,t2.OCR_content) similar
        | from ocr t1
        | left join ocr t2
        | on t1.id = t2.id - 1
        | order by t1.id) T1
        |where T1.similar < 0.7
        |""".stripMargin)

    noSame.createOrReplaceTempView("noSame")
//      .show(100,false)

    // id_span:0->上一条没有被删除，!0->前续数据被删除的条数
    spark.sql(
      """
        |select t1.*,nvl(t1.id - t2.id - 1,0) id_span
        |from noSame t1
        |left join noSame t2
        |on t1.row_num = t2.row_num + 1
        |""".stripMargin)
      .createOrReplaceTempView("id_span")
//      .show(100,false)

    spark.sql(
      """
        |select t1.id,
        |       t1.platform_id,
        |       t1.project_id,
        |       t1.media_id,
        |       ocr.lines_start,
        |       t1.lines_end,
        |       if(t1.condfid > ocr.condfid,t1.OCR_content,ocr.OCR_content) OCR_content,
        |       if(t1.condfid > ocr.condfid,t1.condfid,ocr.condfid) condfid
        |from
        | (select *,id - id_span tar_id
        | from id_span
        | where id_span != 0) t1
        |join ocr
        |on tar_id = ocr.id
        |order by t1.id
        |""".stripMargin)
      .createOrReplaceTempView("rep_etl")
//      .show(100,false)

    spark.sql(
      """
        |select id,
        |       platform_id,
        |       project_id,
        |       media_id,
        |       lines_start,
        |       lines_end,
        |       OCR_content,
        |       condfid
        |from id_span
        |where id_span = 0
        |""".stripMargin)
      .createOrReplaceTempView("normal")
//      .show(100,false)

    spark.sql(
      """
        |select *
        |from normal
        |union all
        |select *
        |from rep_etl
        |""".stripMargin)
      .where("length(OCR_content) > 0")
      .orderBy("id")
//      .show(100,false)


      .foreachPartition(iterator => {

        var conn: Connection = null
        var ps: PreparedStatement = null

        try {
          conn = DriverManager.getConnection(ConfigurationManager.getProperty("jdbc.url"), "video_cut_user", "Slt_2020")
          conn.setAutoCommit(false)

          ps = conn.prepareStatement(
            """INSERT INTO recognition2_ocr_etl(id,platform_id,project_id,media_id,lines_start,lines_end,OCR_content,condfid)
              |VALUES(?,?,?,?,?,?,?,?)
              |""".stripMargin)
          var row = 0
          iterator.foreach(it => {
            ps.setInt(1, it.getAs[Int]("id"))
            ps.setString(2, it.getAs[String]("platform_id"))
            ps.setString(3, it.getAs[String]("project_id"))
            ps.setString(4, it.getAs[String]("media_id"))
            ps.setInt(5, it.getAs[Int]("lines_start"))
            ps.setInt(6, it.getAs[Int]("lines_end"))
            ps.setString(7, it.getAs[String]("OCR_content"))
            ps.setString(8, it.getAs[String]("condfid"))

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


          // 更改偏移量
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            sqlProxy.executeUpdate(client, "update ocr_offset set offset=?",
              Array(max_offset))
          }
          catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }


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

  }

}