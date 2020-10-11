package com.GDNanWangLineLoss.month.util

import java.sql.Date

import org.apache.spark.sql.SparkSession

import com.GDNanWangLineLoss.month.bean.Variables._
/**
  * 添加监控视图
  */
object AddMonitoring {
    def addMonitoring(sparkSession:SparkSession)={

        sparkSession.udf.register("getFormatDate", (time:Long) => {
            df_cjsj.format(new Date(time))
        })
        var start = 0l
        var end = 0l
        var reason = ""
        var isSuccess = 1
        start = System.currentTimeMillis()
        end = System.currentTimeMillis()
        sparkSession.sql(s"select getFormatDate(${end}) recordtime,'开始',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
    }

}
