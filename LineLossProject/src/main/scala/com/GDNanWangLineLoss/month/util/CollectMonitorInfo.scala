package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._

/**
  * 收集监控信息
  */
object CollectMonitorInfo {

    def collectMonitorInfo(sparkSession:SparkSession,url:String)={

        sparkSession.sql("select * from ruleState")
          .repartition(1).createOrReplaceTempView("ruleState")

        //todo rulestate这个表没有
        //todo 这个应该是将整个程序下来的state，保存到表里面
        sparkSession.sql(
            s"""
               |select
               |    getUUID(),${create_time},${create_time},${ybs} tjzq,${nowMonth} tjsj,*
               |from ruleState
             """.stripMargin).write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.rulestate"))
          .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
    }

}
