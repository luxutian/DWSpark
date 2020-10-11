package com.GDNanWangLineLoss.month.bean

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import com.GDNanWangLineLoss.month.controller.LineController
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import org.apache.spark.storage.StorageLevel


/**
  * 变量
  */
object Variables {


//    val session: SparkSession = SparkSession.builder().getOrCreate()

    val calendar = Calendar.getInstance()
    val df = new SimpleDateFormat("yyyyMM")
    val _df = new SimpleDateFormat("yyyy-MM")
    val df_cjsj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dfYear = new SimpleDateFormat("yyyy")
    //yyyy当前年份
    val year = dfYear.format(calendar.getTime)

    //创建时间
    val create_time = "'" + df_cjsj.format(calendar.getTime) + "'"

    //系统月份
    val currentMonth = df.format(calendar.getTime)
    //T-1
    calendar.add(Calendar.MONDAY, -1)
    //同期年月
    calendar.add(Calendar.YEAR, -1)

    val tqny = df.format(calendar.getTime)
    calendar.add(Calendar.YEAR, 1)
    calendar.add(Calendar.MONDAY, 1)
    val addOneMonth = df.format(calendar.getTime)
    val _addOneMonth = _df.format(calendar.getTime)
    calendar.add(Calendar.MONDAY, -1)
    //yyyyMM当前月份
    val nowMonth = df.format(calendar.getTime)
    // val nowMonth = 201912
    //月份 MM
    val month = calendar.get(Calendar.MONTH)
    //yyyy-MM当前月份
    val _nowMonth = _df.format(calendar.getTime)
    // val _nowMonth = "2019-12"

    calendar.add(Calendar.MONDAY, -1)
    //年月编码
    val nybm = nowMonth
    //yyyyMM上个月份
    val lastMonth = df.format(calendar.getTime)
    // val lastMonth = 201911
    //yyyy-MM上个月份
    val _lastMonth = _df.format(calendar.getTime)
    // val _lastMonth = "2019-11"
    //前第2个月
    calendar.add(Calendar.MONDAY, -1)
    val lastMonth2 = df.format(calendar.getTime)
    //前第3个月
    calendar.add(Calendar.MONDAY, -1)
    val lastMonth3 = df.format(calendar.getTime)
    //前第4个月
    calendar.add(Calendar.MONDAY, -1)
    val lastMonth4 = df.format(calendar.getTime)
    //前第5个月
    calendar.add(Calendar.MONDAY, -1)
    val lastMonth5 = df.format(calendar.getTime)
    //统计周期月标识
    val ybs = "'1'"
    //统计周期日标识
    val rbs = "'2'"
    //线路标志
    val xlbz = "'1'"
    //台区标志
    val tqbz = "'2'"
    //结果表的schema
    val writeSchema = "gpsx_sxxm"
    //创建人id
    val creator_id = "null"
    //修改时间
    val update_time = create_time
    //修改人id
    val updator_id = "null"
    //工单翻月日期
    val gdfyrq = "'" + _nowMonth + "-28'"

    val resultPartition = 2

    //定义地市局
    val citySG:City = City("SG", "030200")
    val cityST:City = City("ST", "030500")
    val cityFS:City = City("FS", "030600")
    val cityZQ:City = City("ZQ", "031200")
    val cityZS:City = City("ZS", "032000")

    //val cityNameList = "'"+citySG.name+"','"+cityST.name+"','"+cityFS.name+"','"+cityZQ.name+"','"+cityZS.name+"'"
    //val cityCodeList = "'"+citySG.code+"','"+cityST.code+"','"+cityFS.code+"','"+cityZQ.code+"','"+cityZS.code+"'"
    val cityNameList = "'"+cityZS.name+"'"
    val cityCodeList = "'"+cityZS.code+"'"

    //定义统计结束时间
    var v_sjsj = _addOneMonth + "-01 00:00:00"
    //定义统计开始时间
    var v_sjsj_before = _nowMonth + "-01 00:00:00"




    /*--------------------------------------------------------------------*/
    var yearMonth = nowMonth
    var tableName = "tmr_ods.to_new_dycldrdjbm"
    var tableNameG = "tmr_ods.to_new_gycldssbm"
    var tableNameC = "tmr_ods.to_new_clddldy"

    def setVariables1(sparkSession:SparkSession)={
        //如果数据时间不是当月，去相应月份表取数据
        if(!yearMonth.equals(currentMonth)){
            tableName = tableName + "_" + yearMonth
            tableNameG = tableNameG + "_" + yearMonth
            tableNameC = tableNameC + "_" + yearMonth
            try{
                sparkSession.sql(s"select 1 from ${tableName} limit 1").collect.size
                sparkSession.sql(s"select 1 from ${tableNameG} limit 1").collect.size
                sparkSession.sql(s"select 1 from ${tableNameC} limit 1").collect.size
            }catch{
                case e:Exception => {
                    tableName = "tmr_ods.to_new_dycldrdjbm"
                    tableNameG = "tmr_ods.to_new_gycldssbm"
                    tableNameC = "tmr_ods.to_new_clddldy"
                }
            }
            println(tableName)
            println(tableNameG)
            println(tableNameC)
        }
    }


    //用于“生成计量表码合并视图”段落
    var addOneTableName = "tmr_ods.to_new_dycldrdjbm"  //没发现有这个表
    var addOneTableNameG = "tmr_ods.to_new_gycldssbm"

    def setVariables2(sparkSession:SparkSession)={
        //如果数据时间下月不是当月，去相应月份表取数据
        if(!addOneMonth.equals(currentMonth)){
            addOneTableName = addOneTableName + "_" + addOneMonth
            addOneTableNameG = addOneTableNameG + "_" + addOneMonth
            try{
                sparkSession.sql(s"select 1 from ${addOneTableName} limit 1").collect.size
                sparkSession.sql(s"select 1 from ${addOneTableNameG} limit 1").collect.size
            }catch{
                case e:Exception => {
                    addOneTableName = "tmr_ods.to_new_dycldrdjbm"
                    addOneTableNameG = "tmr_ods.to_new_gycldssbm"
                }
            }
            println(addOneTableName)
            println(addOneTableNameG)
        }

    }



    /*--------------------------------------------------------------------*/

}
