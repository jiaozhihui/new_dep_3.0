package com.bjvca.commonutils

import org.apache.spark.sql.SparkSession

// 表注册类
object TableRegister {

  // 注册es的表
  def registEsTable(spark: SparkSession, nodes: String, port: String, username:String,password:String,index: String, tableName: String) = {
    // 参数配置
    val options = Map(
      "es.nodes" -> nodes,
      "es.port" -> port,
      "es.net.http.auth.user" -> username,
      "es.net.http.auth.pass" -> password)
    // 加载
    val df = spark.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load(index)
    // 注册
    df.createOrReplaceTempView(tableName)
    df
  }

  // 注册mysql表
  def registMysqlTable(spark: SparkSession, host: String, database: String, user: String, password: String, dbTable: String, tableName: String) = {
    spark.read.format("jdbc").options(
      Map("url" -> s"jdbc:mysql://${host}:3306/${database}?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> dbTable,
        "user" -> user,
        "password" -> password)
    ).load().createOrReplaceTempView(tableName)
  }

}
