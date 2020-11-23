package com.GDNanWangLineLoss.month.service

import java.sql.Date

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._
import com.GDNanWangLineLoss.month.dao.DataSourceDao
import com.GDNanWangLineLoss.month.util.{Functions, UDFfunction}


/**
  * 台区5
  */
object TaiQu5Service {

    def taiqu5Service(sparkSession: SparkSession,url:String)={
        println("进入taiqu5Service")
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




        // 台区5.1当月违窃工作单清单    gdrq未确定
        //2019年11月18日16:54:09 lixc 去掉工作单信息历史申请时间条件
        start = System.currentTimeMillis()
        try{
            val tq_51 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    l.tqbs,l.tqbh,
                   |    l.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,g.gzdbh,
                   |    handleTime(w.chsj) chwqydsj,
                   |    handleNumber(w.yzbdl) wqygdl,
                   |    handleTime(g.wcsj) gdrq,
                   |    getycgzbh(${Constant.TQ_51}) ycgzbh,
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,
                   |    ${nybm} nybm
                   |from xsycxlhtqmx l  --线损异常(线路和台区)明细
                   |inner join jld j on j.tqbs = l.tqbs and j.jldytdm <> '410'
                   |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '20' and y.yhztdm <> '2'
                   |inner join wqxx w on w.yhbh = y.yhbh and w.dfny = ${nowMonth}  --违窃信息
                   |inner join gzdxxls g on g.gzdbh = w.gzdbh  --工作单信息历史
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(l.tqbs),',')) k2 as tqkhbhh
                   |where l.xltqbz = '2' and l.ny = ${nowMonth}
                   |
                 """.stripMargin
            sparkSession.sql(tq_51).createOrReplaceTempView("res_gk_wqgzdqd")
            sparkSession.sql("select * from res_gk_wqgzdqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_wqgzdqd")

            //违窃工作单清单
            sparkSession.sql(s"select getUUID() id,* from res_gk_wqgzdqd").show(5)
            sparkSession.sql(s"select getUUID() id,* from res_gk_wqgzdqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_wqgzdqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_WQGZDQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_WQGZDQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_wqgzdqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_wqgzdqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wqgzdqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq51',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则5.1运行${(end-start)/1000}秒")

        //台区5.2 考核表户功率因数偏低
        start = System.currentTimeMillis()
        try{
            val tq_52 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    t2.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t2.xlxdbs,getxlbh(t2.xlxdbs),
                   |    getxlmc(t2.xlxdbs),
                   |    t2.tqbs,t2.tqbh,t2.tqmc,
                   |    getbdzbs(t2.xlxdbs) bdzbs,getbdzbh(t2.xlxdbs) bdzbh,
                   |    getbdzmc(t2.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    t2.yhbh,t2.yhmc,t2.yhlbdm,
                   |    handleNumber(t2.glys) glys,
                   |    handleNumber(t2.jfdl) jfdl,--// 2020/11/6 计费电量（发现有些写dl，后期注意排查）
                   |    getycgzbh(${Constant.TQ_52}) ycgzbh,
                   |    getDsjbm(t2.gddwbm) dsjbm,getQxjbm(t2.gddwbm) qxjbm,getGdsbm(t2.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t2.gddwbm)) dsj,getzzmc(getQxjbm(t2.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t2.gddwbm)) gds,getdmbmmc('YHLBDM',t2.yhlbdm) yhlb,
                   |    getdqbm(t2.gddwbm) dqbm,
                   |    ${nybm}
                   |from (
                   |        select
                   |            t1.xlxdbs,t1.tqbs,t1.yhbh,t1.yhmc,t1.yhlbdm,
                   |            t1.ygzdl/sqrt(t1.ygzdl*t1.ygzdl+t1.wgzdl*t1.wgzdl) glys,
                   |            t1.jfdl,t1.gddwbm,t1.tqbh,t1.tqmc
                   |        from (
                   |            select
                   |                t.xlxdbs,t.tqbs,t.yhbh,t.yhmc,t.yhlbdm,
                   |                sum(t.ygzdl) ygzdl,sum(t.wgzdl) wgzdl,
                   |                sum(t.jfdl) jfdl,t.gddwbm,tq.tqbh,tq.tqmc
                   |            from jldxx t
                   |            join tq tq on t.tqbs = tq.tqbs
                   |            where t.dfny = ${nowMonth} and t.yhlbdm = '60'
                   |            group by t.xlxdbs,t.tqbs,t.yhbh,t.yhmc,t.yhlbdm,t.gddwbm,tq.tqbs,tq.tqbh,tq.tqmc
                   |              ) t1
                   |         where (t1.ygzdl <> 0 or t1.wgzdl <> 0)
                   |    ) t2
                   |lateral view outer explode(split(getbdzkhb(t2.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t2.tqbs),',')) k2 as tqkhbhh
                   |where t2.glys < 0.7
                   |
                 """.stripMargin
            sparkSession.sql(tq_52).createOrReplaceTempView("res_gk_glyspdzgbqd")
            sparkSession.sql("select * from res_gk_glyspdzgbqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_glyspdzgbqd")

            //功率因数偏低的专、公变清单
            sparkSession.sql(s"select getUUID() id,* from res_gk_glyspdzgbqd").show(5)
            sparkSession.sql(s"select getUUID() id,* from res_gk_glyspdzgbqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_glyspdzgbqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GLYSPDZGBQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GLYSPDZGBQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_glyspdzgbqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_glyspdzgbqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_glyspdzgbqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq52',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则5.2运行${(end-start)/1000}秒")
    }
}
