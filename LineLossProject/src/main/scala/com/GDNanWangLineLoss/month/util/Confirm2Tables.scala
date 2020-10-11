package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.SparkSession
import com.GDNanWangLineLoss.month.bean.Variables._
/**
  * 确保两个表有数据
  */
object Confirm2Tables {
    def confirm2Tables(sparkSession:SparkSession)={
        val xlSize = sparkSession.sql(s"select * from xlxstjxx where ny=${nowMonth} limit 1").collect.size
        val tqSize = sparkSession.sql(s"select * from tqxstjxx where ny=${nowMonth} limit 1").collect.size
    }



}
