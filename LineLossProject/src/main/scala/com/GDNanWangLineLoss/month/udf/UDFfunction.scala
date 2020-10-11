package com.GDNanWangLineLoss.month.udf

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.GDNanWangLineLoss.month.util.PearsonCorrelation
import org.apache.spark.sql.SparkSession
import com.GDNanWangLineLoss.month.bean.Variables._


/**
  * 公共方法
  */
object UDFfunction {


    def udfFunction(sparkSession: SparkSession)={
        //自定义函数
        //自定义获取uuid函数
        sparkSession.udf.register("getUUID", () => {
            val uuid = UUID.randomUUID().toString
            uuid.replaceAll("-", "")
        })
        //获取一个月有多少天
        sparkSession.udf.register("getdayOfMonth", () => {
            val a = Calendar.getInstance()
            a.set(Calendar.MONTH,month)//month在变量段落定义
            a.set(Calendar.DATE,1)
            a.roll(Calendar.DATE,-1)
            a.get(Calendar.DATE)
        })
        //自定义获取年月函数
        sparkSession.udf.register("getYearMonth", (date: Date) => {
            date match {
                case null => 197001
                case _ =>{
                    val ca = Calendar.getInstance()
                    ca.setTime(date)
                    val df = new SimpleDateFormat("yyyyMM")
                    df.format(ca.getTime).toInt
                }
            }
        })
        //自定义获取年月日函数
        sparkSession.udf.register("getDay", (date: Date) => {
            date match {
                case null => 19700101
                case _ =>{
                    val ca = Calendar.getInstance()
                    ca.setTime(date)
                    val df = new SimpleDateFormat("yyyyMMdd")
                    df.format(ca.getTime).toInt
                }
            }
        })

        //获取相关系数
        sparkSession.udf.register("getCorr", (col1:String,col2:String) => {
            PearsonCorrelation.getPearsonCorrelationScore(col1,col2)
        })
        //处理数值类型null值
        sparkSession.udf.register("handleNumber", (value:Any) => {
            value match {
                case null => "-999"
                case a:java.lang.Double => a.toString
                case b:java.math.BigDecimal => b.toString
                case c:java.lang.Long => c.toString
                case d:java.lang.String => d
                case _ => "-999"
            }
        })
        //处理时间类型null值
        sparkSession.udf.register("handleTime", (value:Any) => {
            value match {
                case null => "1970-01-01 00:00:00"
                case d:Date => d.toString
            }
        })
        //处理汇总数值类型null值
        sparkSession.udf.register("handleHzNumber", (value:Any) => {
            value match {
                case a:java.lang.Double => a.toString
                case b:java.math.BigDecimal => b.toString
                case c:java.lang.Long => c.toString
                case _ => "0"
            }
        })

        //取三相电流最大值
        sparkSession.udf.register("zddl", (a: Double,b: Double,c: Double)  => {
            var max: Double = 0
            if(a > b){
                max = a
            }else{
                max = b
            }
            if(max < c){
                max = c
            }
            max
        })
        //取三相电流最小值
        sparkSession.udf.register("zxdl", (a: Double,b: Double,c: Double) => {
            var min: Double = 0
            if(a < b){
                min = a
            }else{
                min = b
            }
            if(min > c){
                min = c
            }
            min
        })

    }


    /*-----------下面是拆分开来，一个个实现udf功能--------------------------------------------------------*/
    def getIsFiveDsj(sparkSession:SparkSession)={
        // %spark_yxxsxm
        //获取供电单位编码前4个编码
        sparkSession.udf.register("isFiveDsj", (gddwbm: String) => {
            gddwbm match {
                case s:String =>{
                    if(gddwbm.length<=4){
                        0
                    }else if("0305".equals(gddwbm.substring(0,4))||"0302".equals(gddwbm.substring(0,4))||"0306".equals(gddwbm.substring(0,4))||"0312".equals(gddwbm.substring(0,4))||"0320".equals(gddwbm.substring(0,4))){
                        1
                    }else{
                        0
                    }
                }
                case _ => 0
            }
        })
    }

    /*--------------------------------------------------------------------*/
    //自定义函数
    //自定义获取uuid函数
    def getUUID(sparkSession:SparkSession)={

        sparkSession.udf.register("getUUID", () => {
            val uuid = UUID.randomUUID().toString
            uuid.replaceAll("-", "")
        })
    }

    //获取一个月有多少天

    def getdayOfMonth(sparkSession:SparkSession)={
        //获取一个月有多少天
        sparkSession.udf.register("getdayOfMonth", () => {
            val a = Calendar.getInstance()
            a.set(Calendar.MONTH,month)//month在变量段落定义
            a.set(Calendar.DATE,1)
            a.roll(Calendar.DATE,-1)
            a.get(Calendar.DATE)
        })
    }

    //自定义获取年月函数
    def getYearMonth(sparkSession:SparkSession)={

        sparkSession.udf.register("getYearMonth", (date: Date) => {
            date match {
                case null => 197001
                case _ =>{
                    val ca = Calendar.getInstance()
                    ca.setTime(date)
                    val df = new SimpleDateFormat("yyyyMM")
                    df.format(ca.getTime).toInt
                }
            }
        })
    }


    def getDay(sparkSession:SparkSession)={
        //自定义获取年月日函数
        sparkSession.udf.register("getDay", (date: Date) => {
            date match {
                case null => 19700101
                case _ =>{
                    val ca = Calendar.getInstance()
                    ca.setTime(date)
                    val df = new SimpleDateFormat("yyyyMMdd")
                    df.format(ca.getTime).toInt
                }
            }
        })
    }


    def getCorr(sparkSession:SparkSession)={
        //获取相关系数
        sparkSession.udf.register("getCorr", (col1:String,col2:String) => {
            PearsonCorrelation.getPearsonCorrelationScore(col1,col2)
        })
        //处理数值类型null值
        sparkSession.udf.register("handleNumber", (value:Any) => {
            value match {
                case null => "-999"
                case a:java.lang.Double => a.toString
                case b:java.math.BigDecimal => b.toString
                case c:java.lang.Long => c.toString
                case d:java.lang.String => d
                case _ => "-999"
            }
        })
    }

    def handleTime(sparkSession:SparkSession)={
        //处理时间类型null值
        sparkSession.udf.register("handleTime", (value:Any) => {
            value match {
                case null => "1970-01-01 00:00:00"
                case d:Date => d.toString
            }
        })
    }


    def handleHzNumber(sparkSession:SparkSession)={
        //处理汇总数值类型null值
        sparkSession.udf.register("handleHzNumber", (value:Any) => {
            value match {
                case a:java.lang.Double => a.toString
                case b:java.math.BigDecimal => b.toString
                case c:java.lang.Long => c.toString
                case _ => "0"
            }
        })
    }


    def zddl(sparkSession:SparkSession)={
        //取三相电流最大值
        sparkSession.udf.register("zddl", (a: Double,b: Double,c: Double)  => {
            var max: Double = 0
            if(a > b){
                max = a
            }else{
                max = b
            }
            if(max < c){
                max = c
            }
            max
        })
    }

    def zxdl(sparkSession:SparkSession)={
        //取三相电流最小值
        sparkSession.udf.register("zxdl", (a: Double,b: Double,c: Double) => {
            var min: Double = 0
            if(a < b){
                min = a
            }else{
                min = b
            }
            if(min > c){
                min = c
            }
            min
        })
    }

}
