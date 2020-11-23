package com.GDNanWangLineLoss.month.controller

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Date:2020/11/2/17:33
 * @Description:
 */
object MysqlToKudu {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName(this.getClass.getName)

        val sparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext

        // 2020/11/2 1读取mysql数据  忘了不能本地跑





    }

}
