package com.GDNanWangLineLoss.month.controller

import java.io.File

import com.GDNanWangLineLoss.month.bean.Variables
import com.GDNanWangLineLoss.month.bean.Variables.{addOneMonth, currentMonth, nowMonth}
import com.GDNanWangLineLoss.month.dao.DataSourceDao
import com.GDNanWangLineLoss.month.service.{Line1Service, Line2Service}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object  LineController {
    def main(args: Array[String]): Unit = {
        val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath

        val conf = new SparkConf().setAppName(this.getClass.getName)
          .set("spark.sql.warehouse.dir", warehouseLocation)

        val sparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext
//        ssc.setLogLevel("warn")

        //todo 表的命名统一为中文名拼音首字母

        Variables.setVariables1(sparkSession)  //如果数据时间不是当月，去相应月份表取数据
        Variables.setVariables2(sparkSession)  //用于“生成计量表码合并视图”段落

        val url = "cdh2:7051,cdh3:7051,cdh5:7051"
//        DataSourceTest.receiverData(sparkSession,url)

        // 2020/10/21 测试数据源联通性
//        DataSourceDao.mergeTables(sparkSession,url)  //2020-10-15 计量相关的表还没有入库
//        DataSourceDao.odsGetBaseDatas(sparkSession,url)
//        DataSourceDao.powerDetail(sparkSession,url)
//        DataSourceDao.extraSection(sparkSession,url)

        // 2020/10/21 测试生成第一批清单
//        Line1Service.line1Service(sparkSession,url)
        Line2Service.line2Service(sparkSession,url)


    }

}

/*
spark2-submit \
--master yarn \
--deploy-mode client \
--driver-memory 6g \
--driver-cores 4 \
--num-executors 10 \
--executor-cores 8 \
--executor-memory 10g \
--conf spark.core.connection.ack.wait.timeout=300 \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/xiansun/kudu/log4j.properties" \
--class com.GDNanWangLineLoss.month.controller.LineController /home/xiansun/jars/LineLossProject-1.0-SNAPSHOT-jar-with-dependencies.jar >> \
/home/xiansun/logs/test.log 2>&1


--queue root.default \
 */
