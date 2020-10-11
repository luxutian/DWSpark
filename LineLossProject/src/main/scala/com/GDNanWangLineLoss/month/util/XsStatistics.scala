package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import com.GDNanWangLineLoss.month.bean.Variables._

import scala.collection.mutable.Map

/**
  * GK_XSXLHTQTJ(线损线路和台区统计)
  */
object XsStatistics {
    def xsStatistics(sparkSession:SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1
        //线损线路和台区统计
        start = System.currentTimeMillis()
        try{
            //线损线路统计
            val xltjType= StructType(List[StructField](
                StructField("gddwbm", StringType, true),
                StructField("fsszs", LongType, true),
                StructField("gsszs", LongType, true),
                StructField("dbszs", LongType, true),
                StructField("xsl03zs", LongType, true),
                StructField("xsl35zs", LongType, true),
                StructField("xsl5zs", LongType, true),
                StructField("bdzkhbzs", LongType, true)
            ))
            sparkSession.sql(
                s"""
                   |select
                   |    l.gddwbm,
                   |    (case when l.byxsl<0 then l.xlxdbs else null end) fssXlxdbs,
                   |    (case when l.byxsl>5 then l.xlxdbs else null end) gssXlxdbs,
                   |    (case when l.byxsl between 0 and 5 then l.xlxdbs else null end) dbsXlxdbs,
                   |    (case when l.byxsl >=0 and l.byxsl<=3 then l.xlxdbs else null end) xsl03Xlxdbs,
                   |    (case when l.byxsl >3 and l.byxsl<=5 then l.xlxdbs else null end) xsl35Xlxdbs,
                   |    (case when l.byxsl >5 then l.xlxdbs else null end) xsl5Xlxdbs,
                   |    getbdzkhb(l.xlxdbs) bdzkhbXlxdbs
                   |from xlxstjxx l  --线路线损统计信息
                   |where l.ny = ${nowMonth}
                   |
                 """.stripMargin)
              .createOrReplaceTempView("xsxltj")  //线损线路统计

            val xsxltj =
                s"""
                   |select
                   |    y.gddwbm gddwbm,
                   |    count(distinct y.fssXlxdbs) fsszs,
                   |    count(distinct y.gssXlxdbs) gsszs,
                   |    count(distinct y.dbsXlxdbs) dbszs,
                   |    count(distinct y.xsl03Xlxdbs) xsl03zs,
                   |    count(distinct y.xsl35Xlxdbs) xsl35zs,
                   |    count(distinct y.xsl5Xlxdbs) xsl5zs,
                   |    count(y.bdzkhbXlxdbs) bdzkhbzs
                   |from xsxltj y  --线损线路统计
                   |group by y.gddwbm
                   |
                 """.stripMargin

            val xsxltjArray= sparkSession.sql(xsxltj).collect()
            val xsxltjMap = Map[String,Map[String,Long]]()
            DependentStatistics.updateSpeMap(xsxltjArray,xsxltjMap ,
                "gddwbm","fsszs","gsszs","dbszs","xsl03zs","xsl35zs","xsl5zs","bdzkhbzs")

            val xsxltjIterable= xsxltjMap.map(data=>{
                val map = data._2
                (data._1,map.get("fsszs").getOrElse(0l),map.get("gsszs").getOrElse(0l),
                  map.get("dbszs").getOrElse(0l),map.get("xsl03zs").getOrElse(0l)
                  ,map.get("xsl35zs").getOrElse(0l),map.get("xsl5zs").getOrElse(0l),map.get("bdzkhbzs").getOrElse(0l))
            })
            val xsxltjRDD = sparkSession.sparkContext.parallelize(xsxltjIterable.toSeq)
              .map(data => Row(data._1,data._2,data._3,data._4,data._5,data._6,data._7,data._8))
            sparkSession.createDataFrame(xsxltjRDD,xltjType).createOrReplaceTempView("xsxltj")
            // spark.sql("select * from xsxltj").show(100)
            sparkSession.sql(
                s"""
                   |select
                   |    getUUID(),${creator_id} ,${create_time},${update_time},${updator_id},
                   |    xs.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,zs.sl zs,xs.gsszs,
                   |    0 ycgss,xs.fsszs,xs.dbszs,if(zs.sl=0,1,xs.dbszs*100/zs.sl),xs.fsszs xsl1,
                   |    xs.xsl03zs xsl2,xs.xsl35zs xsl3,xs.xsl5zs xsl4,0 xsl5,xs.bdzkhbzs bdzkhh,0 tqkhh,
                   |    0 zbkhh,0 gbkh
                   |from xsxltj xs
                   |join xlzs zs on xs.gddwbm = zs.gddwbm
                   |
                 """.stripMargin)
              .createOrReplaceTempView("xsxltj")

            //线损台区统计
            val tqtjType= StructType(List[StructField](
                StructField("gddwbm", StringType, true),
                StructField("fsszs", LongType, true),
                StructField("gsszs", LongType, true),
                StructField("dbszs", LongType, true),
                StructField("xsl05zs", LongType, true),
                StructField("xsl510zs", LongType, true),
                StructField("xsl10zs", LongType, true),
                StructField("tqkhbzs", LongType, true)
            ))
            sparkSession.sql(
                s"""
                   |select
                   |    gddwbm,
                   |    (case when l.byxsl<0 then l.tqbs else null end) fssTqbs,
                   |    (case when l.byxsl>5 then l.tqbs else null end) gssTqbs,
                   |    (case when l.byxsl between 0 and 5 then l.tqbs else null end) dbsTqbs,
                   |    (case when l.byxsl >=0 and l.byxsl<=5 then l.tqbs else null end) xsl05Tqbs,
                   |    (case when l.byxsl >5 and l.byxsl<=10 then l.tqbs else null end) xsl510Tqbs,
                   |    (case when l.byxsl >10 then l.tqbs else null end) xsl10Tqbs,gettqkhb(l.tqbs) tqkhbTqbs
                   |from tqxstjxx l
                   |where l.ny = ${nowMonth}
                   |
                 """.stripMargin)
              .createOrReplaceTempView("xstqtj")
            val xstqtj =
                s"""
                   |select
                   |    y.gddwbm gddwbm,
                   |    count(distinct y.fssTqbs) fsszs,
                   |    count(distinct y.gssTqbs) gsszs,
                   |    count(distinct y.dbsTqbs) dbszs,
                   |    count(distinct y.xsl05Tqbs) xsl05zs,
                   |    count(distinct y.xsl510Tqbs) xsl510zs,
                   |    count(distinct y.xsl10Tqbs) xsl10zs,
                   |    count(y.tqkhbTqbs) tqkhbzs
                   |from xstqtj y
                   |group by y.gddwbm
                   |
                 """.stripMargin


            val xstqtjArray:Array[Row] = sparkSession.sql(xstqtj).collect()
            val xstqtjMap = Map[String,Map[String,Long]]()
            DependentStatistics.updateSpeMap(xstqtjArray,xstqtjMap ,
                "gddwbm","fsszs","gsszs","dbszs","xsl05zs","xsl510zs","xsl10zs","tqkhbzs")

            val xstqtjIterable= xstqtjMap.map(data=>{
                val map = data._2
                (data._1,map.get("fsszs").getOrElse(0l),map.get("gsszs").getOrElse(0l),
                  map.get("dbszs").getOrElse(0l),map.get("xsl05zs").getOrElse(0l)
                  ,map.get("xsl510zs").getOrElse(0l),map.get("xsl10zs").getOrElse(0l),map.get("tqkhbzs").getOrElse(0l))
            })
            val xstqtjRDD= sparkSession.sparkContext.parallelize(xstqtjIterable.toSeq)
              .map(data => Row(data._1,data._2,data._3,data._4,data._5,data._6,data._7,data._8))
            sparkSession.createDataFrame(xstqtjRDD,tqtjType).createOrReplaceTempView("xstqtj")
            // spark.sql("select * from xstqtj").show(100)

            sparkSession.sql(
                s"""
                   |select
                   |    getUUID(),${creator_id} ,${create_time},${update_time},${updator_id},xs.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,zs.sl zs,xs.gsszs,
                   |    0 ycgss,xs.fsszs,xs.dbszs,if(zs.sl=0,1,xs.dbszs*100/zs.sl),xs.fsszs xsl1,
                   |    xs.xsl05zs xsl2,xs.xsl510zs xsl3,xs.xsl10zs xsl4,0 xsl5,0 bdzkhh,xs.tqkhbzs tqkhh,
                   |    0 zbkhh,0 gbkh
                   |from xstqtj xs
                   |join tqzs zs on xs.gddwbm = zs.gddwbm
                   |
                 """.stripMargin)
              .createOrReplaceTempView("xstqtj")  //线损台区统计

            //GK_XSXLHTQTJ 线损线路和台区统计
            sparkSession.sql("select * from xsxltj union all select * from xstqtj")
              .createOrReplaceTempView("gk_xsxlhtqtj") //todo 2个维度线路、台区统计

            sparkSession.sql("select * from gk_xsxlhtqtj where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("gk_xsxlhtqtj")

            // TODO: 输出目标表
            sparkSession.sql(
                s"""
                   |select *,tjsj fqrq from gk_xsxlhtqtj
                 """.stripMargin).write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_xsxlhtqtj"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(
//                s"""
//                   |insert into ${writeSchema}.gk_xsxlhtqtj_HIS partition(fqrq)
//                   |select *,tjsj fqrq from gk_xsxlhtqtj
//                 """.stripMargin)
            reason = ""
            isSuccess = 1
        }catch{
            case e:Exception => {
                isSuccess = 0
                val message = e.getMessage
                if(message.length>800) reason = message.substring(0,800) else reason = message.substring(0,message.length)
            }
        }

        end = System.currentTimeMillis()
        sparkSession.sql(
            s"""
               |select *
               |from ruleState
               |union all
               |select
               |    getFormatDate(${end}) recordtime,'线损线路和台区统计',${isSuccess} state,
               |    '${reason}' reason,${(end-start)/1000} runtime
             """.stripMargin).createOrReplaceTempView("ruleState")
        println(s"线损线路和台区统计运行${(end-start)/1000}秒")

    }

}
