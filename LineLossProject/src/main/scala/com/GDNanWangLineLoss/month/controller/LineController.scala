package com.GDNanWangLineLoss.month.controller

import java.io.File

import com.GDNanWangLineLoss.month.bean.Variables
import com.GDNanWangLineLoss.month.bean.Variables.{addOneMonth, currentMonth, nowMonth}
import com.GDNanWangLineLoss.month.dao.DataSourceDao
import com.GDNanWangLineLoss.month.service.{Line1Service, Line2Service, Line3Service, Line4Service, Line5Service, Line6Service, TaiQu1Service, TaiQu2Service, TaiQu3Service, TaiQu4Service, TaiQu5Service, TaiQu6Service}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object  LineController {
    def main(args: Array[String]): Unit = {
//        val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath

        val conf = new SparkConf().setAppName(this.getClass.getName)
//          .set("spark.sql.warehouse.dir", warehouseLocation)

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
        Line1Service.line1Service(sparkSession,url)
        Line2Service.line2Service(sparkSession,url)
        Line3Service.line3Service(sparkSession,url)
        Line4Service.line4Service(sparkSession,url)
//        Line5Service.line5Service(sparkSession,url)
//        Line6Service.line6Service(sparkSession,url)

//        TaiQu1Service.taiqu1Service(sparkSession,url)
//        TaiQu2Service.taiqu2Service(sparkSession,url)
//        TaiQu3Service.taiqu3Service(sparkSession,url)
//        TaiQu4Service.taiqu4Service(sparkSession,url)
//        TaiQu5Service.taiqu5Service(sparkSession,url)
//        TaiQu6Service.taiqu6Service(sparkSession,url)


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
--conf spark.yarn.driver.memoryOverhead=2048 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/xiansun/kudu/log4j.properties" \
--class com.GDNanWangLineLoss.month.controller.LineController /home/xiansun/jars/LineLossProject-1.0-SNAPSHOT-jar-with-dependencies.jar >> \
/home/xiansun/logs/test.log 2>&1


--conf "spark.yarn.executor.memoryOverhead=4G" \
--queue root.default \
 */

/* 2020-10-16
/opt/cloudera/parcels/SPARK2/bin/spark2-submit \
--name SparkKuduDemo \
--master yarn \
--deploy-mode client \
--conf spark.dynamicAllocation.enabled=false \
--driver-memory 2G \
--executor-memory 4G \
--num-executors 2 \
--executor-cores 1 \
--class com.csg.demo.SparkKuduDemo ./SparkDemo-1.0-SNAPSHOT_2.jar
 */