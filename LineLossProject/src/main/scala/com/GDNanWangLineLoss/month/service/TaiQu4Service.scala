package com.GDNanWangLineLoss.month.service

import java.sql.Date

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._
import com.GDNanWangLineLoss.month.dao.DataSourceDao
import com.GDNanWangLineLoss.month.util.{Functions, UDFfunction}

object TaiQu4Service {

    def taiqu4Service(sparkSession: SparkSession,url:String)={
        println("进入taiqu4Service")
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1



        println("读取数据源")
        // 2020/10/21 获取数据源
        //        DataSourceDao.mergeTables(sparkSession,url)
        DataSourceDao.odsGetBaseDatas(sparkSession, url)

        // 2020/10/22 注册函数
        UDFfunction.udfFunction(sparkSession)
        Functions.getZzmc(sparkSession) // 2020/10/27  自定义获取组织名称函数
        Functions.getDMbm(sparkSession) // 2020/10/27 自定义获取代码编码名称
        Functions.getBDZxx(sparkSession) // 2020/10/27 自定义获取变电站信息
        Functions.getBDZkhbhh(sparkSession) // 2020/10/27 自定义获取变电站考核表户号
        Functions.getTQkhbhh(sparkSession) // 2020/10/27 自定义获取台区考核表户号
        Functions.getYwlb(sparkSession) // 2020/10/27 获取业务类别代码

        sparkSession.udf.register("getFormatDate", (time: Long) => {
            df_cjsj.format(new Date(time)) // 2020/10/23 ("yyyy-MM-dd HH:mm:ss")
        })

        // 2020/10/27 获取数据源(按照原来程序的执行顺序)
        DataSourceDao.powerDetail(sparkSession, url)
        DataSourceDao.extraSection(sparkSession, url)

        println("开始跑逻辑")


        //lixc20200602台区4.1公变三相负荷不平衡
        start = System.currentTimeMillis()
        try{
            val tq_41 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    handleNumber(s.sxfhbphl) sxfhbphl,
                   |    getycgzbh(${Constant.TQ_41}) ycgzbh,
                   |    getdsjbm(t.gddwbm) dsjbm,getqxjbm(t.gddwbm) qxjbm,getgdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getdsjbm(t.gddwbm)) dsj,getzzmc(getqxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getgdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,
                   |    ${nybm} nybm
                   |from ydkh y
                   |join sxfhbphl s on s.yhbh = y.yhbh and s.sxfhbphl > 30  --三相负荷不平衡率
                   |join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where y.yhlbdm = '60' and y.yhztdm<>'2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_41).repartition(resultPartition).createOrReplaceTempView("res_gk_sxfhbphqd")
            sparkSession.sql("select * from res_gk_sxfhbphqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_sxfhbphqd")

            //三相负荷不平衡清单
            sparkSession.sql(s"select getUUID() id,* from res_gk_sxfhbphqd").show(5)
            sparkSession.sql(s"select getUUID() id,* from res_gk_sxfhbphqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_sxfhbphqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_sxfhbphqd_his select getUUID(),*,tjsj fqrq from res_gk_sxfhbphqd")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_sxfhbphqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_sxfhbphqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_sxfhbphqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
//              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq41',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.1运行${(end-start)/1000}秒")

        //台区4.3 gk_zgzqzqd
        start = System.currentTimeMillis()
        try{
            val tq_43 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    t.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,
                   |    t.xlbh,t.xlmc,
                   |    t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,
                   |    getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    '重过载' fzqk,
                   |    cast(round(t.bygdl/(b.edrl*24*getdayOfMonth()),2) as string) fzl,
                   |    getycgzbh(${Constant.TQ_43}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,
                   |    ${nybm} nybm
                   |from tqxstjxx t  --台区线损统计信息
                   |join yxbyq b on b.tqbs = t.tqbs
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where t.ny = ${nowMonth} and t.bygdl/(b.edrl*24*getdayOfMonth()) > 0.8
                   |
                 """.stripMargin
            sparkSession.sql(tq_43).createOrReplaceTempView("res_gk_zgzqzqd")
            sparkSession.sql("select * from res_gk_zgzqzqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_zgzqzqd")

            //重过载轻载清单
            sparkSession.sql(s"select getUUID() id,* from res_gk_zgzqzqd").show(5)
            sparkSession.sql(s"select getUUID() id,* from res_gk_zgzqzqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_zgzqzqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_zgzqzqd_his select getUUID(),*,tjsj fqrq from res_gk_zgzqzqd")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_zgzqzqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_zgzqzqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_zgzqzqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
//              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq43',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.3运行${(end-start)/1000}秒")

        /*-------------- 上面和下面 都是重过载轻载清单 -----------------------------------------------------*/
        //台区4.4
        start = System.currentTimeMillis()
        try{
            val tq_44 =
                s"""
                   |select
                   |    ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    ,t.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,t.xlbh,t.xlmc,
                   |    t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,
                   |    getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    '轻载'  fzqk,
                   |    cast(round(t.bygdl/(b.edrl*24*getdayOfMonth()),2) as string) fzl,
                   |    getycgzbh(${Constant.TQ_44}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,
                   |    ${nybm} nybm
                   |from tqxstjxx t
                   |join yxbyq b on b.tqbs = t.tqbs
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where t.ny = ${nowMonth} and t.bygdl/(b.edrl*24*getdayOfMonth()) < 0.2
                   |
                 """.stripMargin
            sparkSession.sql(tq_44).createOrReplaceTempView("res_gk_zgzqzqd")
            sparkSession.sql("select * from res_gk_zgzqzqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_zgzqzqd")

            //重过载轻载清单
            sparkSession.sql(s"select getUUID() id,* from res_gk_zgzqzqd").show(5)
            sparkSession.sql(s"select getUUID() id,* from res_gk_zgzqzqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_zgzqzqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_zgzqzqd_his select getUUID(),*,tjsj fqrq from res_gk_zgzqzqd")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_zgzqzqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_zgzqzqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_zgzqzqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
//              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq44',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.4运行${(end-start)/1000}秒")

        //台区4.6电能表损耗信息
        start = System.currentTimeMillis()
        try{
            val tq_46 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    t.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,t.xlbh,
                   |    t.xlmc,
                   |    t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,
                   |    getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    handleNumber(a.dxyhsl) dxdnbsl,
                   |    handleNumber(a.dxshdl) dxdnbshdl,
                   |    handleNumber(a.sxyhsl) sxdnbsl,
                   |    handleNumber(a.sxshdl) sxdnbshdl,
                   |    handleNumber(a.dxshdl+a.sxshdl) zshdl,
                   |    handleNumber(if(sign(t.bygdl-t.bysdl)=1,(a.dxshdl+a.sxshdl)/(t.bygdl-t.bysdl)*100,0)) ztqzshdlbl,
                   |    getycgzbh(${Constant.TQ_46}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,
                   |    ${nybm} nybm
                   |from tqxstjxx t
                   |inner join (
                   |        select
                   |            b.tqbs,b.dxyhsl,b.sxyhsl,0.7*b.dxyhsl dxshdl,2.8*b.sxyhsl sxshdl
                   |        from (
                   |            select
                   |                t.tqbs,sum(case when j.jxfsdm = '1' then 1 else 0 end) dxyhsl,
                   |                sum(case when j.jxfsdm <> '1' then 1 else 0 end) sxyhsl
                   |            from tqxstjxx t
                   |            inner join jld j on t.tqbs = j.tqbs and j.jldytdm <> '410'
                   |            inner join ydkh y on y.yhbh = j.yhbh
                   |                    and y.yhlbdm = '20' and y.yhztdm <> '2'
                   |            inner join jlddnbgx g on g.jldbh = j.jldbh
                   |            inner join yxdnb d on d.yxdnbbs = g.yxdnbbs
                   |            where t.ny = ${nowMonth}
                   |            group by t.tqbs
                   |            ) b
                   |          ) a
                   |on a.tqbs = t.tqbs and if(sign(t.bygdl-t.bysdl)=1,(a.dxshdl+a.sxshdl)/(t.bygdl-t.bysdl),0) > 0.5 and t.ny = ${nowMonth}
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |
                 """.stripMargin
            sparkSession.sql(tq_46).createOrReplaceTempView("res_gk_dnbshqd")
            sparkSession.sql("select * from res_gk_dnbshqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_dnbshqd")

            //电能表损耗清单
            sparkSession.sql(s"select getUUID() id,* from res_gk_dnbshqd").show(5)
            sparkSession.sql(s"select getUUID() id,* from res_gk_dnbshqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dnbshqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_DNBSHQD_HIS select getUUID(),*,tjsj fqrq from res_gk_dnbshqd")

//            sparkSession.sql(s"select distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dnbshqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_dnbshqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dnbshqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
//              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq46',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.6运行${(end-start)/1000}秒")
    }

}
