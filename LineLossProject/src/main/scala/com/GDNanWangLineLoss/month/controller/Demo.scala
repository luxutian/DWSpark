package com.GDNanWangLineLoss.month.controller

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Date:2020/11/3/10:16
 * @Description:
 */
object Demo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Demo").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
        val ssc = sparkSession.sparkContext


        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1
        start = System.currentTimeMillis() //todo 用于记录程序运行时间

        val result = 8


    }

}
