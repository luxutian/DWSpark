package com.GDNanWangLineLoss.month.dao

import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map

/**
  * 异常线路和台区带来的额外段落
  */
object ExtraSection {
    private val  url ="10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"

    def extraSection(sparkSession:SparkSession)={
        //1 线路线损统计信息  xlbh-线路编号,xlmc-线路名称,xlxdbs-线路线段标识,ny-年月,byxsl-本月线损率,
        //                  bygdl-本月供电量,bysdl-本月售电量,gddwbm-供电单位编码,bdzbh-变电站编号,
        //                  bdzmc-变电站名称,bdzbs-变电站标识,khzb-考核指标,khzbxx-考核指标下限

        val npmis_gk_xlxstjxx = sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "gk_xlxstjxx"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        npmis_gk_xlxstjxx.createOrReplaceTempView("xlxstjxx")
        val xlxstjxx =sparkSession.sql(
            s"""
               |select
               |    xlbh,xlmc,xlxdbs,ny,byxsl,bygdl,bysdl,gddwbm,bdzbh,bdzmc,
               |    bdzbs,khzb,khzbxx,xszrrbs,dqbm,sqxsl,ssqxsl,byxsl,ny
               |from xlxstjxx
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        xlxstjxx.createOrReplaceTempView("xlxstjxx") //线路线损统计信息
        /*-------------------------------------------------------------------*/

        //2 台区线损统计信息 tqbs-台区标识,tqbh-台区编号,tqmc-台区名称,bygdl-本月供电量,bysdl-本月售电量,gddwbm-供电单位编码,bdzbs-变电站标识
        val npmis_gk_tqxstjxx =sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "gk_tqxstjxx"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        npmis_gk_tqxstjxx.createOrReplaceTempView("tqxstjxx")
        val tqxstjxx =sparkSession.sql(
            s"""
               |select
               |    tqbs,tqbh,tqmc,bygdl,bysdl,ny,gddwbm,bdzbs,xlxdbs,xlbh,xlmc,tqbs,tqbh,tqmc,
               |    xszrrbs,dqbm,sqxsl,ssqxsl,byxsl,khzb,khzbxx,ny
               |from tqxstjxx --台区线损统计信息
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        tqxstjxx.createOrReplaceTempView("tqxstjxx") //台区线损统计信息
        /*-------------------------------------------------------------------*/

        //线损异常(线路和台区)明细  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,xltqbz-线路台区标识(1-线路,2-台区),
        //                      gddwbm-供电单位编码,ny-年月,byxsl-本月线损率,bygdl-本月供电量,bysdl-本月售电量
        //                      tqbs-台区标识,tqbh-台区编号,tqmc-台区名称
        // 自己汇总  缺乏sfxf,bz 字段
        sparkSession.sql(
            s"""
               |select
               |    distinct xl.ny,xl.gddwbm,substr(xl.gddwbm,1,length(xl.gddwbm)-2) sjdwbm,
               |    substr(xl.gddwbm,1,length(xl.gddwbm)-4) ssjdwbm,${xlbz} xltqbz,xl.xlxdbs,
               |    l.xlbh xlbh,l.xlmc xlmc,null tqbs,null tqbh,null tqmc,xl.bygdl,xl.bysdl,xl.byxsl,
               |    xl.xszrrbs,r.rymc xszrr,bz.xlbzbh zhxx,null sfxf,xl.dqbm,null bz,xl.khzbxx,xl.khzb,
               |    xl.sqxsl,xl.ssqxsl,xl.bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc
               |from xlxstjxx xl
               |left join all_xlxd l on xl.xlxdbs = l.xlxdbs
               |left join xlbzxx bz on xl.xlxdbs = bz.xlxdbs and bz.bzny = xl.ny
               |left join ry r on r.rybs = xl.xszrrbs
               |where (xl.byxsl > xl.khzb or xl.byxsl < xl.khzbxx) and (xl.bygdl > 30000 or xl.bysdl > 30000)
               |union all
               |select
               |    distinct tq.ny,tq.gddwbm,substr(tq.gddwbm,1,length(tq.gddwbm)-2) sjdwbm,
               |    substr(tq.gddwbm,1,length(tq.gddwbm)-4) ssjdwbm,${tqbz} xltqbz,tq.xlxdbs,l.xlbh xlbh,
               |    l.xlmc xlmc,tq.tqbs tqbs,tq.tqbh tqbh,tq.tqmc tqmc,tq.bygdl,tq.bysdl,tq.byxsl,tq.xszrrbs,
               |    r.rymc xszrr,bz.tqbzbh zhxx,null sfxf,tq.dqbm,null bz,tq.khzbxx,tq.khzb,tq.sqxsl,
               |    tq.ssqxsl,tq.bdzbs,getbdzbh(tq.xlxdbs) bdzbh,getbdzmc(tq.xlxdbs) bdzmc
               |from tqxstjxx tq
               |left join all_xlxd l on tq.xlxdbs = l.xlxdbs
               |left join tqbzxx bz on tq.tqbs = bz.tqbs and bz.bzny = tq.ny
               |left join ry r on r.rybs = tq.xszrrbs
               |where (tq.byxsl > tq.khzb or tq.byxsl < tq.khzbxx) and (tq.bygdl > 10000 or tq.bysdl > 10000)
            """.stripMargin).createOrReplaceTempView("xsycxlhtqmx")//线损异常(线路和台区)明细

        /*-------------------------------------------------------------------*/

        //3 异常线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
        val xlxd1 =sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "dw_xlxd"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        xlxd1.createOrReplaceTempView("xlxd")
        val xlxd =sparkSession.sql(
            s"""
               |select
               |    l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm,y.gddwbm
               |from xlxd l  --与all_xlxd join 也一样
               |join xsycxlhtqmx y on l.xlxdbs = y.xlxdbs
               |where y.ny = ${nowMonth} and y.xltqbz = ${xlbz}
               |
             """.stripMargin)
        xlxd.createOrReplaceTempView("xlxd") //异常线路线段

        /*-------------------------------------------------------------------*/

        //4 异常台区  tqbs-台区标识,tqbh-台区编号,tqmc-台区名称
        val tq1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "dw_tq"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        tq1.createOrReplaceTempView("tq")

        //与异常的信息join，拿到异常的拿到异常的台区
        val tq =sparkSession.sql(
            s"""
               |select
               |    t.tqbs,t.tqbh,t.tqmc,t.yxztdm,t.gddwbm
               |from tq t
               |join xsycxlhtqmx y on t.tqbs = y.tqbs
               |where t.gddwbm is not null and y.ny = ${nowMonth} and y.xltqbz = ${tqbz}
               |
             """.stripMargin)
        tq.createOrReplaceTempView("tq") //异常台区

        /*-------------------------------------------------------------------*/
        //2020.04.07添加
        //根据线路线段表示获取上期线损率   xlxdbs 线路线段标识    bqxsl 线损率
        val xlxdSqxsl = sparkSession.sql(
            s"""
               |select
               |    xlxdbs,cast(byxsl as string) bqxsl
               |from xlxstjxx
               |where ny=${lastMonth}
               |
             """.stripMargin)

        val xlxdSqxslList= xlxdSqxsl.collect()
        val xlxdSqxslMap = Map[String, String]()
        for (xsl <- xlxdSqxslList) {
            xlxdSqxslMap.put(xsl.getAs[String]("xlxdbs"), xsl.getAs[String]("bqxsl"))
        }

        //广播变量
        val xlxdSqxslBroadcast= sparkSession.sparkContext.broadcast(xlxdSqxslMap)

        //udf-获取线段的线损率
        sparkSession.udf.register("getXlxdSqXsl", (xlxdbs: String)  => {
            xlxdSqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })

        //根据线路线段表示获取上上期线损率
        val xlxdSsqxsl = sparkSession.sql(s"select xlxdbs,cast(byxsl as string) bqxsl from xlxstjxx where ny=${lastMonth2}")

        val xlxdSsqxslList= xlxdSsqxsl.collect()
        val xlxdSsqxslMap = Map[String, String]()
        for (xsl <- xlxdSsqxslList) {
            xlxdSsqxslMap.put(xsl.getAs[String]("xlxdbs"), xsl.getAs[String]("bqxsl"))
        }
        val xlxdSsqxslBroadcast= sparkSession.sparkContext.broadcast(xlxdSsqxslMap)
        sparkSession.udf.register("getXlxdSsqXsl", (xlxdbs: String)=> {
            xlxdSsqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })

        /*-------------------------------------------------------------------*/

        //根据台区标识获取上期线损率
        val tqSqxsl = sparkSession.sql(
            s"""
               |select
               |    tqbs,cast(byxsl as string) bqxsl
               |from tqxstjxx where ny=${lastMonth}
             """.stripMargin)

        val tqSqxslList= tqSqxsl.collect()
        val tqSqxslMap = scala.collection.mutable.Map[String, String]()
        for (xsl <- tqSqxslList) {
            tqSqxslMap.put(xsl.getAs[String]("tqbs"), xsl.getAs[String]("bqxsl"))
        }

        //广播变量
        val tqSqxslBroadcast= sparkSession.sparkContext.broadcast(tqSqxslMap)
        //udf-获取标识
        sparkSession.udf.register("getTqSqXsl", (xlxdbs: String) => {
            tqSqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })

        //根据台区标识获取上上期线损率
        val tqSsqxsl = sparkSession.sql(
            s"""
               |select
               |    tqbs,cast(byxsl as string) bqxsl
               |from tqxstjxx where ny=${lastMonth2}
               |
             """.stripMargin)

        val tqSsqxslList= tqSsqxsl.collect()
        val tqSsqxslMap = scala.collection.mutable.Map[String, String]()
        for (xsl <- tqSsqxslList) {
            tqSsqxslMap.put(xsl.getAs[String]("tqbs"), xsl.getAs[String]("bqxsl"))
        }
        val tqSsqxslBroadcast= sparkSession.sparkContext.broadcast(tqSsqxslMap)

        //输入key ，获取value
        sparkSession.udf.register("getTqSsqXsl", (xlxdbs: String) => {
            tqSsqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })

    }


}
