package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import com.GDNanWangLineLoss.month.bean.Variables._

import scala.collection.mutable.Map


/**
  * GK_XSYCYYFBQKTJ  线损异常原因分布情况统计 需要关联
  */
object AbnormityReasonStatistics {

    def abnormityReason(sparkSession:SparkSession,url:String)={

        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1
        // gk_xsycyyfbqktj  线损异常原因分布情况统计
        start = System.currentTimeMillis()
        val wtflzsType = StructType(List[StructField](
            StructField("gddwbm", StringType, true),
            StructField("chsgl", LongType, true),
            StructField("scyx", LongType, true),
            StructField("ydjc", LongType, true),
            StructField("qtwt", LongType, true),
            StructField("jczlgl", LongType, true),
            StructField("jlgl", LongType, true),
            StructField("xxxtwts", LongType, true),
            StructField("yybmwts", LongType, true)
        ))
        try{
            // 线路
            val xlwtflzs =
                s"""
                   |select
                   |    y.gddwbm,
                   |    count(case when getycgzfl(y.ycgzbh)='抄核收管理' then 1 else null end) chsgl,
                   |    count(case when getycgzfl(y.ycgzbh)='生产运行' then  1 else null end) scyx,
                   |    count(case when getycgzfl(y.ycgzbh)='用电检查' then  1 else null end) ydjc,
                   |    count(case when getycgzfl(y.ycgzbh)='其它原因' then  1 else null end) qtwt,
                   |    count(case when getycgzfl(y.ycgzbh)='基础资料管理' then  1 else null end) jczlgl,
                   |    count(case when getycgzfl(y.ycgzbh)='计量管理' then  1 else null end) jlgl,
                   |    count(case when getycgzfl(y.ycgzbh)='信息系统' then  1 else null end) xxxtwts,
                   |    count(case when y.ycgzbh is null then 1 else null end) yybmwts
                   |from (
                   |    select
                   |        distinct x.gddwbm,x.sjdwbm,x.ssjdwbm,x.xlxdbs,x.xlbh,x.xlmc,x.tqbs,x.tqbh,
                   |        x.tqmc,x.bygdl,x.bysdl,x.byxsl,x.xszrrbs,x.zhxx,x.sfxf,x.bz,x.khzbxx zbxx,
                   |        x.khzb zbsx,x.sqxsl,x.ssqxsl,x.xltqbz,x.bdzbs,x.bdzbh,x.bdzmc,y.ycgzbh
                   |    from xsycxlhtqmx x left  --线损异常(线路和台区)明细
                   |    join ycgddwxlgx y  --异常供电单位线路对应关系
                   |    on y.gddwbm = x.gddwbm and y.xlxdbs = x.xlxdbs and y.xltqbz = ${xlbz}
                   |        and y.tjsj = ${nowMonth} where x.xltqbz = ${xlbz} and x.ny = ${nowMonth}
                   |        ) y
                   |group by y.gddwbm
                   |
                 """.stripMargin
            val xlwtflzsArray= sparkSession.sql(xlwtflzs).collect()
            val xlwtflzsMap = Map[String,Map[String,Long]]()
            DependentStatistics.updateSpeMap(xlwtflzsArray,xlwtflzsMap ,"gddwbm","chsgl","scyx","ydjc","qtwt","jczlgl","jlgl","xxxtwts","yybmwts")

            val xlwtflzsIterable= xlwtflzsMap.map(data=>{
                val map = data._2
                (data._1,map.get("chsgl").getOrElse(0l),map.get("scyx").getOrElse(0l),
                  map.get("ydjc").getOrElse(0l),map.get("qtwt").getOrElse(0l),
                  map.get("jczlgl").getOrElse(0l),map.get("jlgl").getOrElse(0l),
                  map.get("xxxtwts").getOrElse(0l),map.get("yybmwts").getOrElse(0l))
            })
            val xlwtflzsRDD= sparkSession.sparkContext.parallelize(xlwtflzsIterable.toSeq)
              .map(data=>(Row(data._1,data._2,data._3,data._4,data._5,data._6,data._7,data._8,data._9)))
            sparkSession.createDataFrame(xlwtflzsRDD,wtflzsType).createOrReplaceTempView("xlwtflzs")
            // spark.sql("select * from xlwtflzs").show(100)

            //台区
            val tqwtflzs =
                s"""
                   |select
                   |    y.gddwbm,
                   |    count(case when getycgzfl(y.ycgzbh)='抄核收管理' then 1 else null end) chsgl,
                   |    count(case when getycgzfl(y.ycgzbh)='生产运行' then  1 else null end) scyx,
                   |    count(case when getycgzfl(y.ycgzbh)='用电检查' then  1 else null end) ydjc,
                   |    count(case when getycgzfl(y.ycgzbh)='其它原因' then  1 else null end) qtwt,
                   |    count(case when getycgzfl(y.ycgzbh)='基础资料管理' then  1 else null end) jczlgl,
                   |    count(case when getycgzfl(y.ycgzbh)='计量管理' then  1 else null end) jlgl,
                   |    count(case when getycgzfl(y.ycgzbh)='信息系统' then  1 else null end) xxxtwts,
                   |    count(case when y.ycgzbh is null then 1 else null end) yybmwts
                   |from (
                   |    select distinct x.gddwbm,x.sjdwbm,x.ssjdwbm,x.xlxdbs,x.xlbh,x.xlmc,x.tqbs,x.tqbh,
                   |        x.tqmc,x.bygdl,x.bysdl,x.byxsl,x.xszrrbs,x.zhxx,x.sfxf,x.bz,x.khzbxx zbxx,
                   |        x.khzb zbsx,x.sqxsl,x.ssqxsl,x.xltqbz,x.bdzbs,x.bdzbh,x.bdzmc,y.ycgzbh
                   |    from xsycxlhtqmx x
                   |    left join ycgddwxlgx y
                   |    on y.gddwbm = x.gddwbm and y.tqbs = x.tqbs and y.xltqbz = ${tqbz}
                   |        and y.tjsj = ${nowMonth} where x.xltqbz = ${tqbz} and x.ny = ${nowMonth}
                   |       ) y
                   |group by y.gddwbm
                   |
                 """.stripMargin
            val tqwtflzsArray:Array[Row] = sparkSession.sql(tqwtflzs).collect()
            val tqwtflzsMap = Map[String,Map[String,Long]]()
            DependentStatistics.updateSpeMap(tqwtflzsArray,tqwtflzsMap ,"gddwbm","chsgl","scyx","ydjc","qtwt","jczlgl","jlgl","xxxtwts","yybmwts")

            val tqwtflzsIterable= tqwtflzsMap.map(data=>{
                val map = data._2
                (data._1,map.get("chsgl").getOrElse(0l),map.get("scyx").getOrElse(0l),map.get("ydjc").getOrElse(0l),map.get("qtwt").getOrElse(0l),
                  map.get("jczlgl").getOrElse(0l),map.get("jlgl").getOrElse(0l),map.get("xxxtwts").getOrElse(0l),map.get("yybmwts").getOrElse(0l))
            })
            val tqwtflzsRDD= sparkSession.sparkContext.parallelize(tqwtflzsIterable.toSeq)
              .map(data=>(Row(data._1,data._2,data._3,data._4,data._5,data._6,data._7,data._8,data._9)))
            sparkSession.createDataFrame(tqwtflzsRDD,wtflzsType).createOrReplaceTempView("tqwtflzs")
            // spark.sql("select * from tqwtflzs").show(100)
            //要把分区表gpsxzq.GK_FQXSTJXX给换了   yybmwts未确定

            //todo zbgl_zs_yczs_ycl这个表没找到，不知道是不是这个 zbgl_zs_yczs_ycl
            sparkSession.sql(
                s"""
                   |select
                   |    getUUID(),${creator_id},${create_time},${update_time},${updator_id},
                   |    z.gddwbm,substr(z.gddwbm,1,length(z.gddwbm)-2) sjdwbm,
                   |    substr(z.gddwbm,1,length(z.gddwbm)-4) ssjdwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
                   |    z.zs,z.yczs,z.ycl,l.chsgl chswts,l.jczlgl jcdawts,l.jlgl jlglwts,l.scyx scyxwts,
                   |    l.ydjc ydjcwts,l.qtwt qtwts,l.yybmwts yybmwts,handleNumber(f.bygdl),
                   |    handleNumber(f.bysdl),handleNumber(f.byxsl),y.sl ycclwcs,y2.ycclwcl ycclwcl,
                   |    getDsjbm(z.gddwbm) dsjbm,getQxjbm(z.gddwbm) qxjbm,getGdsbm(z.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(z.gddwbm)) dsj,getzzmc(getQxjbm(z.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(z.gddwbm)) gds,l.xxxtwts,f.khzb,f.khzbxx
                   |from zs_yczs_ycl z
                   |join xlwtflzs l on z.gddwbm = l.gddwbm
                   |left join ycclwcs y on y.gddwbm = l.gddwbm  --异常处理完成数
                   |left join ycclwcl y2 on y2.gddwbm = l.gddwbm --异常处理完成率
                   |left join fqxstjxx f on l.gddwbm = f.gddwbm  --线损报表分区线损统计信息
                   |where z.xltqbz = ${xlbz} and f.ny = ${nowMonth}
                   |
                   |union
                   |select
                   |    getUUID(),${creator_id},${create_time},${update_time},${updator_id},
                   |    z.gddwbm,substr(z.gddwbm,1,length(z.gddwbm)-2) sjdwbm,
                   |    substr(z.gddwbm,1,length(z.gddwbm)-4) ssjdwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,
                   |    z.zs,z.yczs,z.ycl,l.chsgl chswts,l.jczlgl jcdawts,l.jlgl jlglwts,l.scyx scyxwts,
                   |    l.ydjc ydjcwts,l.qtwt qtwts,l.yybmwts yybmwts,handleNumber(f.bygdl),
                   |	handleNumber(f.bysdl),handleNumber(f.byxsl),y.sl ycclwcs,y2.ycclwcl ycclwcl,
                   |    getDsjbm(z.gddwbm) dsjbm,getQxjbm(z.gddwbm) qxjbm,getGdsbm(z.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(z.gddwbm)) dsj,getzzmc(getQxjbm(z.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(z.gddwbm)) gds,l.xxxtwts,f.khzb,f.khzbxx
                   |from zs_yczs_ycl z
                   |join tqwtflzs l on z.gddwbm = l.gddwbm
                   |left join ycclwcs y on y.gddwbm = l.gddwbm
                   |left join ycclwcl y2 on y2.gddwbm = l.gddwbm
                   |left join fqxstjxx f on l.gddwbm = f.gddwbm
                   |where z.xltqbz = ${tqbz} and f.ny = ${nowMonth}
                   |
                 """.stripMargin)
              .repartition(resultPartition).createOrReplaceTempView("gk_xsycyyfbqktj")

            //线损异常原因分布情况统计
            sparkSession.sql("select * from gk_xsycyyfbqktj where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("gk_xsycyyfbqktj")

            sparkSession.sql(s"select *,tjsj fqrq from gk_xsycyyfbqktj")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_xsycyyfbqktj"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(
//                s"""
//                   |insert into ${writeSchema}.gk_xsycyyfbqktj_his partition(fqrq)
//                   |select *,tjsj fqrq from gk_xsycyyfbqktj
//                   |
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
        sparkSession.sql(s"").createOrReplaceTempView("ruleState")
        println(s"线损异常原因分布情况统计运行${(end-start)/1000}秒")

    }

}
