package com.GDNanWangLineLoss.month.controller

import com.GDNanWangLineLoss.month.bean.Variables
import com.GDNanWangLineLoss.month.bean.Variables.{addOneMonth, currentMonth, nowMonth}
import com.GDNanWangLineLoss.month.service.DataSourceTest
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object  LineController {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName(this.getClass().getName).setMaster("local[4]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext
//        val kuduContext = new KuduContext("192.168.22.112:7051", ssc)

        //todo 表的命名统一为中文名拼音首字母

        Variables.setVariables1(sparkSession)  //如果数据时间不是当月，去相应月份表取数据
        Variables.setVariables2(sparkSession)  //用于“生成计量表码合并视图”段落







        val url = "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"
        DataSourceTest(sparkSession,url)

    }

}
