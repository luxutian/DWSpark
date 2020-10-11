package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.SparkSession
import com.GDNanWangLineLoss.month.bean.Variables._

/**
  * gk_xsycxlhtqycmx(线损异常线路和台区异常明细)
  */
object CollectXsycxlhtqycmxService {

    //汇总：线损异常线路和台区异常明细    sfxf-是否下发,bz-备注
    /**
      *
      * @param sparkSession
      */
    def collectXsycxlhtqycmxService(sparkSession: SparkSession)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        start = System.currentTimeMillis()
        try{
            val gk_xsycxlhtqycmx =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xl.gddwbm,
                   |    xl.sjdwbm,xl.ssjdwbm,${nowMonth} tjsj,${ybs} tjzq,xl.xlxdbs,xl.xlbh,xl.xlmc,
                   |    xl.tqbs,xl.tqbh,xl.tqmc,xl.bygdl,xl.bysdl,xl.byxsl,xl.xszrrbs,x.rymc xszrr,xl.zhxx,xl.sfxf,
                   |    xl.bz,xl.khzbxx zbxx,xl.khzb zbsx,xl.sqxsl,xl.ssqxsl,xl.xltqbz,xl.bdzbs,xl.bdzbh,
                   |    xl.bdzmc,k1.bdzkhbhh,null tqkhbhh,y.ycgzbh,y.ycgzmc,getycgzcs(y.ycgzbh,"ycyyyjfl"),
                   |    getycgzcs(y.ycgzbh,"ycyyyjflbm"),getycgzcs(y.ycgzbh,"ycyyejfl"),
                   |    getycgzcs(y.ycgzbh,"ycyyejflbm"),getycgzcs(y.ycgzbh,"ycyysjfl"),
                   |    getycgzcs(y.ycgzbh,"ycyysjflbm"),getycgzcs(y.ycgzbh,"ycyyyjfl_tree"),
                   |    getycgzcs(y.ycgzbh,"ycyyyjflbm_tree"),xl.dqbm,${nowMonth} nybm,
                   |    getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds
                   |from xsycxlhtqmx xl --线损异常(线路和台区)明细
                   |left join ry x on x.rybs = xl.xszrrbs  --人员表
                   |left join ycgddwxlgx y on xl.gddwbm = y.gddwbm and xl.xlxdbs = y.xlxdbs
                   |    and y.xltqbz = ${xlbz}  and y.tjzq=${ybs} and y.tjsj=${nowMonth}  --异常供电单位线路对应关系
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |where xl.ny = ${nowMonth}  and xl.xltqbz = ${xlbz}
                   |
                   |union all
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                   |    xl.gddwbm,xl.sjdwbm,xl.ssjdwbm,${nowMonth} tjsj,${ybs} tjzq,xl.xlxdbs,
                   |    xl.xlbh,xl.xlmc,xl.tqbs,xl.tqbh,xl.tqmc,xl.bygdl,xl.bysdl,xl.byxsl,xl.xszrrbs,
                   |    x.rymc xszrr,xl.zhxx,xl.sfxf,xl.bz,xl.khzbxx zbxx,xl.khzb zbsx,xl.sqxsl,
                   |    xl.ssqxsl,xl.xltqbz,xl.bdzbs,xl.bdzbh,xl.bdzmc,k1.bdzkhbhh,k2.tqkhbhh,
                   |    y.ycgzbh,y.ycgzmc,getycgzcs(y.ycgzbh,"ycyyyjfl"),getycgzcs(y.ycgzbh,"ycyyyjflbm"),
                   |    getycgzcs(y.ycgzbh,"ycyyejfl"),getycgzcs(y.ycgzbh,"ycyyejflbm"),
                   |    getycgzcs(y.ycgzbh,"ycyysjfl"),getycgzcs(y.ycgzbh,"ycyysjflbm"),
                   |    getycgzcs(y.ycgzbh,"ycyyyjfl_tree"),getycgzcs(y.ycgzbh,"ycyyyjflbm_tree"),
                   |    xl.dqbm,${nowMonth} nybm,getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,
                   |    getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds
                   |from xsycxlhtqmx xl
                   |left join ry x on x.rybs = xl.xszrrbs
                   |left join ycgddwxlgx y on xl.gddwbm = y.gddwbm and xl.tqbs = y.tqbs
                   |    and y.xltqbz = ${tqbz}  and y.tjzq=${ybs} and y.tjsj=${nowMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.ny = ${nowMonth} and xl.xltqbz = ${tqbz}
                   |
                 """.stripMargin
            sparkSession.sql(gk_xsycxlhtqycmx).createOrReplaceTempView("gk_xsycxlhtqycmx") //线损异常线路和台区异常明(异常原因级别分类)

            sparkSession.sql("select * from gk_xsycxlhtqycmx where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("gk_xsycxlhtqycmx")

            sparkSession.sql(
                s"""
                   |insert into ${writeSchema}.gk_xsycxlhtqycmx
                   |select
                   |    getUUID(),*,tjsj fqrq
                   |from gk_xsycxlhtqycmx  --管控线损异常线路和台区异常明细
                   |
                 """.stripMargin)

            sparkSession.sql(
                s"""
                   |insert into ${writeSchema}.gk_xsycxlhtqycmx_his
                   |select
                   |    getUUID(),*,tjsj fqrq
                   |from gk_xsycxlhtqycmx
                   |
                 """.stripMargin) //todo 为什么做一个临时表
            isSuccess = 1
            reason = ""
        }catch{
            case e:Exception => {
                isSuccess = 0
                val message = e.getMessage
                if(message.length>800) reason = message.substring(0,800) else reason = message.substring(0,message.length)
            }
        }

        end = System.currentTimeMillis()
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'线损异常线路和台区异常明细',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"线损异常线路和台区异常明细运行${(end-start)/1000}秒")
    }
}
