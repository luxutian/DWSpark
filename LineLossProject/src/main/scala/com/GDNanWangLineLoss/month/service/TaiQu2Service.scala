package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._

/**
  * 台区2
  */
object TaiQu2Service {

    def taiqu2Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        //台区2.1供电单位有变更
        start = System.currentTimeMillis()
        try{
            val tq_21 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},j.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,
                   |    xl.tqbh,xl.tqmc,getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,jl.gddwbm sygddwbm,
                   |    getycgzbh(${Constant.TQ_21}) ycgzbh,getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,
                   |    getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(xl.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx xl  --线损异常(线路和台区)明细
                   |inner join jldxx j on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |inner join jldxx jl on jl.yhbh = j.yhbh and jl.jldytdm <> '410' and  jl.dfny = ${lastMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '2' and xl.ny = ${nowMonth}  and jl.gddwbm <> j.gddwbm
                   |    and (j.yhlbdm in ('60','20') or (j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj in ('02','03')))) and j.yhztdm <> '2'
                   |    and (jl.yhlbdm in ('60','20') or (jl.yhlbdm = '40'
                   |    and exists (select 1 from dcfzxx b where b.dcbh = jl.yhbh and b.dydj in ('02','03')))) and jl.yhztdm <> '2'
                   |
                   |
                 """.stripMargin
            sparkSession.sql(tq_21).createOrReplaceTempView("res_gk_cshgddwbgqd")
            sparkSession.sql("select * from res_gk_cshgddwbgqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_cshgddwbgqd")

            //初始化供电单位变更清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cshgddwbgqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_cshgddwbgqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_CSHGDDWBGQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_CSHGDDWBGQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_cshgddwbgqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_cshgddwbgqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cshgddwbgqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq21',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.1运行${(end-start)/1000}秒")


        //台区2.2供电单位为空检查     sygddwbm还未确定
        start = System.currentTimeMillis()
        try{
            val tq_22 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                   |    t.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,
                   |    l.xlmc,t.tqbs,t.tqbh,t.tqmc,getbdzbs(l.xlxdbs) bdzbs,
                   |    getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,
                   |    y.yhmc,y.yhlbdm,null sygddwbm,getycgzbh(${Constant.TQ_22}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2' and y.gddwbm is null
                   |
                 """.stripMargin
            sparkSession.sql(tq_22).createOrReplaceTempView("res_gk_gddwwkhbgqd")
            sparkSession.sql("select * from res_gk_gddwwkhbgqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gddwwkhbgqd")

            //供电单位为空户变更清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gddwwkhbgqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gddwwkhbgqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GDDWWKHBGQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GDDWWKHBGQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gddwwkhbgqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gddwwkhbgqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gddwwkhbgqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq22',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.2运行${(end-start)/1000}秒")

        //台区2.3电压等级有变更
        start = System.currentTimeMillis()
        try{
            val tq_23 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,
                   |    xl.tqbh,xl.tqmc,getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,jl.jldydjdm sqdydjdm,
                   |    j.jldydjdm jldydjdm,getycgzbh(${Constant.TQ_23}) ycgzbh,getDsjbm(xl.gddwbm) dsjbm,
                   |    getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdmbmmc('DYDJDM',jl.jldydjdm) SQDYDJ,
                   |    getdmbmmc('DYDJDM',j.jldydjdm) DYDJ,getdqbm(xl.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx xl  --线损异常(线路和台区)明细
                   |inner join jldxx j on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |inner join jldxx jl on jl.yhbh = j.yhbh and jl.jldytdm <> '410' and  jl.dfny = ${lastMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '2' and xl.ny = ${nowMonth} and j.jldydjdm <> jl.jldydjdm
                   |    and j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj in ('02','03')) and j.yhztdm <> '2'
                   |    and jl.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = jl.yhbh and b.dydj in ('02','03')) and jl.yhztdm <> '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_23).createOrReplaceTempView("RES_GK_DFDCHDYDJBGQD")
            sparkSession.sql("select * from RES_GK_DFDCHDYDJBGQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_DFDCHDYDJBGQD")

            //地方电厂户电压等级变更清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dfdchdydjbgqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dfdchdydjbgqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_DFDCHDYDJBGQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_DFDCHDYDJBGQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dfdchdydjbgqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dfdchdydjbgqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dfdchdydjbgqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq23',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.3运行${(end-start)/1000}秒")

        //台区2.4光伏用户类别有误
        start = System.currentTimeMillis()
        try{
            val tq_24 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,handleNumber(y.ysdf),
                   |    getycgzbh(${Constant.TQ_24}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ysdfjl y  --应收电费记录
                   |inner join jld j on j.jldbh = y.jldbh and j.jldytdm <> '410'
                   |inner join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where y.yhlbdm = '45' and y.ysdf <> 0 and y.dfny = ${nowMonth}
                   |
                 """.stripMargin
            sparkSession.sql(tq_24).createOrReplaceTempView("res_gk_gfyhlbywqd")
            sparkSession.sql("select * from res_gk_gfyhlbywqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gfyhlbywqd")

            //光伏用户类别有误清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gfyhlbywqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gfyhlbywqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

//            sparkSession.sql(s"insert into ${writeSchema}.GK_GFYHLBYWQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GFYHLBYWQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gfyhlbywqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gfyhlbywqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gfyhlbywqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq24',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.4运行${(end-start)/1000}秒")

        //台区2.5抄表周期问题       bdzbs特殊，通过_tqxstjxx拿到
        start = System.currentTimeMillis()
        try{
            val tq_25 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},tj.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,tj.xlxdbs,tj.xlbh,tj.xlmc,tj.tqbs,tj.tqbh,tj.tqmc,
                   |    bd.bdzbs,bd.bdzbh,bd.bdzmc,k1.bdzkhbhh,k2.tqkhbhh,y.cbzq,handleNumber(tj.bygdl) gdl,
                   |    handleNumber(tj.bysdl) sdl,getycgzbh(${Constant.TQ_25}) ycgzbh,getDsjbm(tj.gddwbm) dsjbm,
                   |    getQxjbm(tj.gddwbm) qxjbm,getGdsbm(tj.gddwbm) gdsbm,getzzmc(getDsjbm(tj.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(tj.gddwbm)) qxj,getzzmc(getGdsbm(tj.gddwbm)) gds,
                   |    getdmbmmc('CBZQDM',y.cbzq) cbzq,getdqbm(tj.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join tqxstjxx tj on tj.tqbs = j.tqbs  --台区线损统计信息
                   |join bdz bd on bd.bdzbs = tj.bdzbs and tj.ny = ${nowMonth} and (tj.bygdl <> 0 or tj.bysdl <> 0)  --变电站
                   |lateral view outer explode(split(getbdzkhb(tj.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(tj.tqbs),',')) k2 as tqkhbhh
                   |where y.yhlbdm = '60'
                   |    and y.yhztdm <> '2'
                   |    and (y.cbzq is null or y.cbzq = 0)
                   |
                 """.stripMargin
            sparkSession.sql(tq_25).createOrReplaceTempView("res_gk_cbzqydlbppyhqd")
            sparkSession.sql("select * from res_gk_cbzqydlbppyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_cbzqydlbppyhqd")

            //抄表周期与电量不匹配用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cbzqydlbppyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_cbzqydlbppyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_CBZQYDLBPPYHQD_HIS select getUUID(),*,tjsj fqrq from res_gk_cbzqydlbppyhqd")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_cbzqydlbppyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_cbzqydlbppyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cbzqydlbppyhqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq25',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.5运行${(end-start)/1000}秒")

        //台区2.6计量点档案所属台区为空检查
        start = System.currentTimeMillis()
        try{
            val tq_26 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},j.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,j.xlxdbs,l.xlbh,l.xlmc,null tqbs,
                   |    null tqbh,null tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,getycgzbh(${Constant.TQ_26}) ycgzbh,
                   |    getDsjbm(j.gddwbm) dsjbm,getQxjbm(j.gddwbm) qxjbm,getGdsbm(j.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(j.gddwbm)) dsj,getzzmc(getQxjbm(j.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(j.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(j.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410' and  j.tqbs is null
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_26).createOrReplaceTempView("res_gk_jldxxdassxlwkqd")
            sparkSession.sql("select * from res_gk_jldxxdassxlwkqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jldxxdassxlwkqd")

            //计量点信息档案所属线路为空清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jldxxdassxlwkqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jldxxdassxlwkqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLDXXDASSXLWKQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLDXXDASSXLWKQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jldxxdassxlwkqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jldxxdassxlwkqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jldxxdassxlwkqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq26',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.6运行${(end-start)/1000}秒")

        //台区2.7线损计量点档案比较      两个考核表户号待定
        start = System.currentTimeMillis()
        try{
            val tq_27 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xstq.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xsxl.xlxdbs,xsxl.xlbh,xsxl.xlmc,
                   |    xstq.tqbs,xstq.tqbh,xstq.tqmc,getbdzbs(xsxl.xlxdbs) bdzbs,
                   |    getbdzbh(xsxl.xlxdbs) bdzbh,getbdzmc(xsxl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,
                   |    g.xlxdbs xsjlddaxlxdbs,xsxl.xlbh xsjlddaxlbh,xsxl.xlmc xsjlddaxlmc,
                   |    g.tqbs xsjlddatqbs,xstq.tqbh xsjlddatqbh,xstq.tqmc xsjlddatqmc,
                   |    d.xlxdbs zxbhxlxdbs,zxxl.xlbh zxbhtqbh,zxxl.xlmc zxbhxlmc,d.tqbs zxbhtqbs,
                   |    zxtq.tqbh zxbhtqbh,zxtq.tqmc zxbhtqmc,j.xlxdbs khzhdaxlxdbs,khxl.xlbh khzhdaxlbh,
                   |    khxl.xlmc khzhdaxlmc,j.tqbs khzhdatqbs,khtq.tqbh khzhdatqbh,khtq.tqmc khzhdatqmc,
                   |    getycgzbh(${Constant.TQ_27}) ycgzbh,getDsjbm(xstq.gddwbm) dsjbm,getQxjbm(xstq.gddwbm) qxjbm,
                   |    getGdsbm(xstq.gddwbm) gdsbm,getzzmc(getDsjbm(xstq.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xstq.gddwbm)) qxj,getzzmc(getGdsbm(xstq.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(xstq.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jldxx g on g.yhbh = y.yhbh and g.dfny = ${nowMonth}
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410' and g.tqbs <> j.tqbs
                   |inner join yhdyxx d on d.yhbh = y.yhbh  --用户电源信息
                   |inner join all_xlxd xsxl on xsxl.xlxdbs = g.xlxdbs
                   |inner join tq xstq on xstq.tqbs = g.tqbs
                   |inner join all_xlxd zxxl on zxxl.xlxdbs = d.xlxdbs
                   |inner join all_tq zxtq on zxtq.tqbs = d.tqbs
                   |inner join all_xlxd khxl on khxl.xlxdbs = j.xlxdbs
                   |inner join all_tq khtq on khtq.tqbs = j.tqbs
                   |lateral view outer explode(split(getbdzkhb(xsxl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xstq.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm in ('60','20') or (y.yhlbdm = '40' and exists (select 1 from _dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2'
                   |
                   |union
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xstq.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xsxl.xlxdbs,xsxl.xlbh,xsxl.xlmc,
                   |    xstq.tqbs,xstq.tqbh,xstq.tqmc,getbdzbs(xsxl.xlxdbs) bdzbs,getbdzbh(xsxl.xlxdbs) bdzbh,
                   |    getbdzmc(xsxl.xlxdbs) bdzmc,k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,
                   |    g.xlxdbs xsjlddaxlxdbs,xsxl.xlbh xsjlddaxlbh,xsxl.xlmc xsjlddaxlmc,
                   |    g.tqbs xsjlddatqbs,xstq.tqbh xsjlddatqbh,xstq.tqmc xsjlddatqmc,d.xlxdbs zxbhxlxdbs,
                   |    zxxl.xlbh zxbhtqbh,zxxl.xlmc zxbhxlmc,d.tqbs zxbhtqbs,zxtq.tqbh zxbhtqbh,zxtq.tqmc zxbhtqmc,
                   |    j.xlxdbs khzhdaxlxdbs,khxl.xlbh khzhdaxlbh,khxl.xlmc khzhdaxlmc,j.tqbs khzhdatqbs,
                   |    khtq.tqbh khzhdatqbh,khtq.tqmc khzhdatqmc,getycgzbh(${Constant.TQ_27}) ycgzbh,
                   |    getDsjbm(xstq.gddwbm) dsjbm,getQxjbm(xstq.gddwbm) qxjbm,getGdsbm(xstq.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xstq.gddwbm)) dsj,getzzmc(getQxjbm(xstq.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xstq.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
                   |    getdqbm(xstq.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jldxx g on g.yhbh = y.yhbh and g.dfny = ${nowMonth}
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join yhdyxx d on d.yhbh = y.yhbh and g.tqbs <> d.tqbs  --用户电源信息
                   |inner join all_xlxd xsxl on xsxl.xlxdbs = g.xlxdbs
                   |inner join tq xstq on xstq.tqbs = g.tqbs
                   |inner join all_xlxd zxxl on zxxl.xlxdbs = d.xlxdbs
                   |inner join all_tq zxtq on zxtq.tqbs = d.tqbs
                   |inner join all_xlxd khxl on khxl.xlxdbs = j.xlxdbs
                   |inner join all_tq khtq on khtq.tqbs = j.tqbs
                   |lateral view outer explode(split(getbdzkhb(xsxl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xstq.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm in ('60','20') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_27).createOrReplaceTempView("res_gk_yxljlddayyhdabyzqd")
            sparkSession.sql("select * from res_gk_yxljlddayyhdabyzqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxljlddayyhdabyzqd")

            //计量点档案与用户档案不一致清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxljlddayyhdabyzqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yxljlddayyhdabyzqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_YXLJLDDAYYHDABYZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_YXLJLDDAYYHDABYZQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yxljlddayyhdabyzqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxljlddayyhdabyzqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxljlddayyhdabyzqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq27',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.7运行${(end-start)/1000}秒")

        //台区2.8月台区GIS系统和营销系统电源点不一致清单      未确定，GIS系统的台区标识、编号、名称怎么找
        //11.04 修改byqbh为byqbs
        start = System.currentTimeMillis()
        try{
            val tq_28 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,b.sbbs byqbs,b.mc byqmc,
                   |    b.sbbs byqbh,b.gisid byqgisid,null,null,null,null,null,null,null,null,
                   |    t.tqbs yxxtdydtqbs,t.tqbh yxxtdydtqbh,t.tqmc yxxtdydtqmc,b.gisid yxxtdydtqgisid,
                   |    lt.tqbs gisxsdydtqbs,lt.tqbh gisxsdydtqbh,lt.tqmc gisxsdydtqmc,
                   |    g.trans_id gisxsdydtqgisid,getycgzbh(${Constant.TQ_28}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join yhdyxx d on d.yhbh = y.yhbh  --用户电源信息
                   |inner join tq t on t.tqbs = d.tqbs
                   |inner join yxbyq b on b.tqbs = t.tqbs  --运行变压器
                   |inner join all_xlxd l on l.xlxdbs = d.xlxdbs
                   |inner join gis_lvcustomersupply g on g.customer_mrid = y.yhbh and g.trans_id <> b.gisid
                   |inner join gis_g_dm_function_location f on f.id = g.trans_id and f.classify_id = '8390' and f.xfmr_type <> 2
                   |left join yxbyq lg on lg.gisid = g.trans_id --运行变压器
                   |left join tq lt on lt.tqbs = lg.tqbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm in ('60','20')
                   |    or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_28).createOrReplaceTempView("res_gk_gisxthyxxtdydbyzqd")
            sparkSession.sql("select * from res_gk_gisxthyxxtdydbyzqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gisxthyxxtdydbyzqd")

            //GIS系统和营销系统电源点不一致清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gisxthyxxtdydbyzqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gisxthyxxtdydbyzqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GISXTHYXXTDYDBYZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GISXTHYXXTDYDBYZQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gisxthyxxtdydbyzqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gisxthyxxtdydbyzqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gisxthyxxtdydbyzqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq28',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.8运行${(end-start)/1000}秒")

        //台区2.9抄表区段
        start = System.currentTimeMillis()
        try{
            val tq_29 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                   |    t.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,t.xlbh,t.xlmc,
                   |    t.tqbs,t.tqbh,t.tqmc,getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,y.cbqdbh,cb.cbqdmc,
                   |    getycgzbh(${Constant.TQ_29}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join xsycxlhtqmx t on t.tqbs = j.tqbs and t.ny = ${nowMonth} and t.xltqbz = '2'  --线损异常(线路和台区)明细
                   |left join cbqdxx cb on cb.cbqdbh = y.cbqdbh  --抄表区段信息
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm = '20' or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2'
                   |    and exists (
                   |                select 1
                   |                from (
                   |                    select
                   |                        a.tqbs,a.cbqdbh,a.cbqdsl,
                   |                        row_number() over(partition by a.tqbs order by a.cbqdsl desc) rowno
                   |                    from (
                   |                        select
                   |                            j.tqbs,y.cbqdbh,count(*) cbqdsl
                   |                        from ydkh y
                   |                        inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |                        inner join xsycxlhtqmx t on t.tqbs = j.tqbs and t.ny = ${nowMonth} and t.xltqbz = '2'
                   |                        where (y.yhlbdm = '20' or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |                            and y.yhztdm <> '2'
                   |                        group by j.tqbs,y.cbqdbh
                   |                        ) a
                   |                      ) aa
                   |                where aa.tqbs = t.tqbs and aa.cbqdbh = y.cbqdbh and aa.rowno <> 1)
                   |
                 """.stripMargin
            sparkSession.sql(tq_29).createOrReplaceTempView("res_gk_cbqdbyzyhqd")
            sparkSession.sql("select * from res_gk_cbqdbyzyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_cbqdbyzyhqd")

            //抄表区段不一致用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cbqdbyzyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_cbqdbyzyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_CBQDBYZYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_CBQDBYZYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_cbqdbyzyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_cbqdbyzyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cbqdbyzyhqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq29',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.9运行${(end-start)/1000}秒")

        //台区2.10与集中器对应关系不同
        start = System.currentTimeMillis()
        try{
            val tq_210 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,t.xlbh,t.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,a.tqbs dydxxtqbs,a.tqbh dydxxtqbh,
                   |    a.tqmc dydxxtqmc,a.yhbh dydxxtqkhhhh,b.tqbs jzqzdgxtqbs,b.tqbh jzqzdgxtqbh,
                   |    b.tqmc jzqzdgxtqmc,b.yhbh jzqzdgxtqkhhhh,getycgzbh(${Constant.TQ_210}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx t  --线损异常(线路和台区)明细
                   |inner join jld j on j.tqbs = t.tqbs and j.jldytdm <> '410'
                   |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '20' and y.yhztdm <> '2'
                   |inner join (
                   |            select
                   |                y.yhbh,t.tqbs,t.tqbh,t.tqmc
                   |            from xsycxlhtqmx t
                   |            inner join jld j on j.tqbs = t.tqbs and j.jldytdm <> '410'
                   |            inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '60' and y.yhztdm <> '2'
                   |            where t.ny = ${nowMonth} and t.xltqbz = '2'
                   |            ) a
                   |on a.tqbs = t.tqbs
                   |inner join (
                   |            select
                   |                gx.cjdxyhbh,zd.yhbh,t.tqbs,t.tqbh,t.tqmc
                   |            from yxjlzdhzd zd  --运行计量自动化终端
                   |            inner join jlzdhzdcjgxjl gx on gx.yxjlzdhzdbs = zd.yxjlzdhzdbs  --计量自动化终端采集关系记录
                   |            inner join jld j on j.yhbh = zd.yhbh and j.jldytdm <> '410'
                   |            inner join tq t on t.tqbs = j.tqbs
                   |            where zd.sblbdm = '05'
                   |            ) b
                   |on b.cjdxyhbh = y.yhbh and b.tqbs <> t.tqbs
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where t.ny = ${nowMonth} and t.xltqbz = '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_210).createOrReplaceTempView("res_gk_sstqyjzqzdgxbyzyhqd")
            sparkSession.sql("select * from res_gk_sstqyjzqzdgxbyzyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_sstqyjzqzdgxbyzyhqd")

            //所属台区与集中器终端关系不一致用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_sstqyjzqzdgxbyzyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_sstqyjzqzdgxbyzyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_SSTQYJZQZDGXBYZYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_SSTQYJZQZDGXBYZYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_sstqyjzqzdgxbyzyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_sstqyjzqzdgxbyzyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_sstqyjzqzdgxbyzyhqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq210',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.10运行${(end-start)/1000}秒")

        //台区2.11 考核表反向有功电量计量点缺失 swdl-上网电量，fxygdl-反向有功电量 未确定
        //2019年11月19日16:31:06 lixc 修改存在地方电厂上网用户的电压等级判断条件用到的表;左连接查询上网电量
        start = System.currentTimeMillis()
        try{
            val tq_211 =
                s"""
                   |select
                   |    ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,handleNumber(sw.swdl) swdl,null fxygdl,
                   |    getycgzbh(${Constant.TQ_211}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from tq t
                   |join jldxx j on j.tqbs = t.tqbs and j.yhlbdm = '60' and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |left join tqdfdcswdl sw on sw.tqbs = t.tqbs  --台区地方电厂上网电量
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where exists (
                   |            select 1
                   |            from gdljfhzxx a,dcfzxx b,yhdyxx dy
                   |            where b.dcbh = a.dcbh and a.dcbh = dy.yhbh and a.gdyf = ${nowMonth} and b.dydj in ('02','03') and dy.tqbs = t.tqbs)
                   |    and not exists (
                   |                select 1
                   |                from cbxx c
                   |                where c.sslxdm = '221' and c.dfny = ${nowMonth} and c.yhbh = j.yhbh and c.jldbh = j.jldbh)
                   |
                   |
                 """.stripMargin
            sparkSession.sql(tq_211).createOrReplaceTempView("res_gk_khbfxygdkjkdqsqd")
            sparkSession.sql("select * from res_gk_khbfxygdkjkdqsqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_khbfxygdkjkdqsqd")

            //考核表反向有功电量计量点缺失清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_khbfxygdkjkdqsqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_khbfxygdkjkdqsqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_KHBFXYGDKJKDQSQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_KHBFXYGDKJKDQSQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_khbfxygdkjkdqsqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_khbfxygdkjkdqsqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_khbfxygdkjkdqsqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq211',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.11运行${(end-start)/1000}秒")

        //台区2.12运行状态检查
        start = System.currentTimeMillis()
        try{
            val tq_212 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,y.yhztdm yxztdm,handleNumber(x.jfdl),
                   |    getycgzbh(${Constant.TQ_212}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
                   |    getdmbmmc('YXZT',y.yhztdm) yhzt,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jldxx x on x.yhbh = y.yhbh and x.jldytdm <> '410' and x.dfny = ${nowMonth} and x.jfdl <> 0
                   |inner join tq t on t.tqbs = x.tqbs
                   |inner join all_xlxd l on l.xlxdbs = x.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm in ('20','60')
                   |    or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm not in ('1','3')
                   |
                 """.stripMargin
            sparkSession.sql(tq_212).createOrReplaceTempView("res_gk_yxztydlbppqd")
            sparkSession.sql("select * from res_gk_yxztydlbppqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxztydlbppqd")

            //运行状态与电量不匹配清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxztydlbppqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yxztydlbppqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_YXZTYDLBPPQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_YXZTYDLBPPQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yxztydlbppqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxztydlbppqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxztydlbppqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq212',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.12运行${(end-start)/1000}秒")

        //台区2.13台区档案变更电子化移交不及时清单
        //2019年11月22日10:46:31 lixc 
        start = System.currentTimeMillis()
        try{
            val tq_213 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,
                   |    getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,j.jfdl,
                   |    d.ykgdbh,handleTime(g.sqsj),handleTime(g.wcsj),getycgzbh(${Constant.TQ_213}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |join dzhyjgzdxx d on d.yhbh = y.yhbh  --电子化移交工作单信息表
                   |join ykgzdjbxx yg on yg.gzdbh = d.ykgdbh  --业扩工作单基本信息
                   |join gzdxxls g on g.slbs = yg.slbs   --工作单信息历史
                   |join jldxx j on j.yhbh = y.yhbh and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                   |inner join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where (y.yhlbdm in ('60','20') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03')))) and y.yhztdm <> '2'
                   |    and datediff(g.sqsj,${gdfyrq}) < 0 and ((g.gzdzt = '1' and g.wcsj is null) or (g.gzdzt = '2' and datediff(g.wcsj,${gdfyrq}) >= 0))
                   |
                   |
                 """.stripMargin
            sparkSession.sql(tq_213).createOrReplaceTempView("res_gk_dabgdzhyjbjsqd")
            sparkSession.sql("select * from res_gk_dabgdzhyjbjsqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dabgdzhyjbjsqd")

            //档案变更电子化移交不及时清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dabgdzhyjbjsqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dabgdzhyjbjsqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_DABGDZHYJBJSQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_DABGDZHYJBJSQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dabgdzhyjbjsqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dabgdzhyjbjsqd_ycgddwxlgx")

            sparkSession.sql(s"insert into ${writeSchema}.ycgddwxlgx select getUUID(),*,tjsj fqrq from res_gk_dabgdzhyjbjsqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq213',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.13运行${(end-start)/1000}秒")

        //台区2.14检查电能表基础档案
        start = System.currentTimeMillis()
        try{
            val tq_214 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,
                   |    t.tqbh,t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,c.eddydm dnbeddydm,j.jlfsdm jlfsdm,
                   |    getycgzbh(${Constant.TQ_214}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
                   |    getdmbmmc('DNBEDDY',c.eddydm) DNBEDDY,
                   |    getdmbmmc('JLFSDM',j.jlfsdm) JLFS,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join jlddnbgx g on g.jldbh = j.jldbh  --计量点运行电能表关系
                   |inner join yxdnb d on d.yxdnbbs = g.yxdnbbs
                   |inner join dnbcs c on c.csbs = d.csbs  --电能表参数
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |join tq t on t.tqbs = j.tqbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where ((c.eddydm = '5' and j.jlfsdm in ('2','3')) or (c.eddydm = '2' and j.jlfsdm = '1'))
                   |    and (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_214).createOrReplaceTempView("res_gk_jlfzpdyhlbycqd")
            sparkSession.sql("select * from res_gk_jlfzpdyhlbycqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlfzpdyhlbycqd")

            //计量辅助判断用户类别异常清单
            sparkSession.sql(s"insert into ${writeSchema}.gk_jlfzpdyhlbycqd select getUUID(),*,tjsj fqrq from res_gk_jlfzpdyhlbycqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jlfzpdyhlbycqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLFZPDYHLBYCQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLFZPDYHLBYCQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jlfzpdyhlbycqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlfzpdyhlbycqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlfzpdyhlbycqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq214',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.14运行${(end-start)/1000}秒")

        //台区2.15月台区计量、业扩工单翻月清单
        //2019年11月18日18:52:01 lixc 修改环节提交时间，装拆信息录入环节小于28号，业扩归档（合并）环节大于等于28号start = System.currentTimeMillis()
        try{
            val tq_215 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,
                   |    xl.tqbh,xl.tqmc,getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,yw.ywbm ywlx,g.gzdbh ywgzdbh,
                   |    handleNumber(j.jfdl),getycgzbh(${Constant.TQ_215}) ycgzbh,getDsjbm(xl.gddwbm) dsjbm,
                   |    getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,yw.ywbmmc ywlx,getdqbm(xl.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx xl
                   |inner join jldxx j on j.tqbs = xl.tqbs
                   |inner join ydkh y on y.yhbh = j.yhbh
                   |inner join ykgzdjbxx g on g.yhbh = y.yhbh  --业扩工作单基本信息
                   |inner join wfrt_task_exec_info wf on wf.instance_id = g.slbs and wf.tache_name = '装拆信息录入' and datediff(wf.task_commit_time,${gdfyrq}) < 0
                   |inner join wfrt_task_exec_info wf2 on wf2.instance_id = g.slbs and wf2.tache_name = '业扩归档（合并）' and datediff(wf2.task_commit_time,${gdfyrq}) >= 0
                   |left join ywjl yw on yw.ywbm = g.ywlbbh  --业务级联
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '2' and xl.ny = ${nowMonth}
                   |    and (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03')))) and y.yhztdm <> '2'
                   |    and (g.ywlbbh in ('F-CSG-MK0512-01','F-CSG-MK0513-01','F-CSG-MK0513-02','F-CSG-MK0513-03','F-CSG-MK0513-04','F-CSG-MK0513-06','F-CSG-MK0513-08','F-CSG-MK0513-09',
                   |        'F-CSG-MK0514-16','F-CSG-MK0514-11','F-CSG-MK0514-28','F-CSG-MK0514-03','F-CSG-MK0514-04') or g.ywlbbh in (select ywbm from ywjl where sjywbm = 'MK082'))
                   |
                   |
                 """.stripMargin
            sparkSession.sql(tq_215).createOrReplaceTempView("res_gk_jlykgdfyqd")
            sparkSession.sql("select * from res_gk_jlykgdfyqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlykgdfyqd")

            //计量、业扩工单翻月清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlykgdfyqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jlykgdfyqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLYKGDFYQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLYKGDFYQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jlykgdfyqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlykgdfyqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlykgdfyqd_ycgddwxlgx")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq215',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.15运行${(end-start)/1000}秒")
        
    }
}
