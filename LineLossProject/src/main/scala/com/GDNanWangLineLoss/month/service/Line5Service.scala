package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Line5Service {
    def line5Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        //线路5.1当月违窃工作单      gdrq未确定
        //2019年11月18日17:29:06 lixc 去掉工作单信息历史申请时间查询条件
        start = System.currentTimeMillis()
        try{
            val xl_51 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},l.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,null,null,null,getbdzbs(l.xlxdbs) bdzbs,
                   |    getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,null,y.yhbh,y.yhmc,y.yhlbdm,
                   |    g.gzdbh,handleTime(w.chsj) chwqydsj,handleNumber(w.yzbdl) wqygdl,handleTime(g.wcsj) gdrq,
                   |    getycgzbh(${Constant.XL_51}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from xlxd l
                   |inner join jld j on j.xlxdbs = l.xlxdbs and j.jldytdm <> '410'
                   |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm in ('10','11') and y.yhztdm <> '2'
                   |inner join wqxx w on w.yhbh = y.yhbh and w.dfny = ${nowMonth}  --违窃信息
                   |inner join gzdxxls g on g.gzdbh = w.gzdbh  --工作单信息历史
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                """.stripMargin
            sparkSession.sql(xl_51).createOrReplaceTempView("RES_GK_WQGZDQD")
            sparkSession.sql("select * from RES_GK_WQGZDQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_WQGZDQD")

            //违窃工作单清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wqgzdqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_wqgzdqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_WQGZDQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_WQGZDQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_wqgzdqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_wqgzdqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wqgzdqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl51',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则5.1运行${(end-start)/1000}秒")

        //线路5.2线路谐波源客户电量清单
        start = System.currentTimeMillis()
        try{
            val xl_52 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,null,null,null,
                   |    getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null,j.yhbh,j.yhmc,j.yhlbdm,handleNumber(j.jfdl),
                   |    getycgzbh(${Constant.XL_52}) ycgzbh,getDsjbm(xl.gddwbm) dsjbm,
                   |    getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,
                   |    getdqbm(xl.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx xl --线损异常(线路和台区)明细
                   |inner join jldxx j
                   |    on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |        and j.yhlbdm in ('10','11') and j.yhztdm <> '2'
                   |inner join dmbm dm  --代码编码
                   |    on dm.dmbm = j.hyfldm and dm.dmfl = 'HYFLDM' and (dm.dmbmmc like '%冶金%' or dm.dmbmmc like '%冶炼%')
                   |inner join (
                   |            select j.xlxdbs,sum(j.jfdl) yhzdl
                   |            from xsycxlhtqmx xl
                   |            inner join jldxx j
                   |                on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |                    and j.yhlbdm in ('10','11') and j.yhztdm <> '2'
                   |            inner join dmbm dm
                   |                on dm.dmbm = j.hyfldm and dm.dmfl = 'HYFLDM' and (dm.dmbmmc like '%冶金%' or dm.dmbmmc like '%冶炼%')
                   |            where xl.xltqbz = '1' and xl.ny = ${nowMonth} and xl.byxsl < 3
                   |            group by j.xlxdbs
                   |            ) a
                   |on a.xlxdbs = xl.xlxdbs and if(sign(xl.bysdl)=1,a.yhzdl/xl.bysdl,0) > 0.6
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |where xl.xltqbz = '1' and xl.ny = ${nowMonth} and xl.byxsl < 3
                """.stripMargin
            sparkSession.sql(xl_52).createOrReplaceTempView("res_gk_xbykhdlqd")
            sparkSession.sql("select * from res_gk_xbykhdlqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_xbykhdlqd")

            //谐波源客户电量清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_xbykhdlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"tt"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_XBYKHDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_XBYKHDLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_xbykhdlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_xbykhdlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_xbykhdlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl52',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则5.2运行${(end-start)/1000}秒")

        //线路5.3 功率因数偏低专变清单
        start = System.currentTimeMillis()
        try{
            val xl_53 =
                        s"""
                           |select
                           |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                           |    d.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,d.xlxdbs,
                           |    getxlbh(d.xlxdbs),getxlmc(d.xlxdbs),null tqbs,null tqbh,null tqmc,
                           |    getbdzbs(d.xlxdbs) bdzbs,getbdzbh(d.xlxdbs) bdzbh,getbdzmc(d.xlxdbs) bdzmc,
                           |    k1.bdzkhbhh,null,d.yhbh,d.yhmc,d.yhlbdm,handleNumber(d.glys),handleNumber(d.jfdl),
                           |    getycgzbh(${Constant.XL_53}) ycgzbh,getDsjbm(d.gddwbm) dsjbm,
                           |    getQxjbm(d.gddwbm) qxjbm,getGdsbm(d.gddwbm) gdsbm,
                           |    getzzmc(getDsjbm(d.gddwbm)) dsj,getzzmc(getQxjbm(d.gddwbm)) qxj,getzzmc(getGdsbm(d.gddwbm)) gds,
                           |    getdmbmmc('YHLBDM',d.yhlbdm) yhlb,getdqbm(d.gddwbm) dqbm,${nybm}
                           |from (
                           |     select t.xlxdbs
                           |     from jldxx t
                           |     where t.dfny = ${nowMonth} and t.yhlbdm = '80' and t.ygzdl <> 0 and t.wgzdl <> 0
                           |        and ygzdl/sqrt(t.ygzdl*t.ygzdl+t.wgzdl*t.wgzdl) < 0.8) a
                           |join (
                           |    select t2.xlxdbs,sum(t2.jfdl) zbzjfdl
                           |    from (
                           |        select t1.xlxdbs,t1.yhbh,t1.ygzdl/sqrt(t1.ygzdl*t1.ygzdl+t1.wgzdl*t1.wgzdl) glys,t1.jfdl
                           |        from (
                           |            select t.xlxdbs,t.yhbh,sum(t.ygzdl) ygzdl,sum(t.wgzdl) wgzdl,sum(t.jfdl) jfdl
                           |            from jldxx t
                           |            where t.dfny = ${nowMonth} and t.yhlbdm in ('10','11') group by t.xlxdbs,t.yhbh
                           |            ) t1
                           |        where (t1.ygzdl <> 0 or t1.wgzdl <> 0)
                           |        ) t2
                           |     where t2.glys < 0.8 group by t2.xlxdbs
                           |    ) b
                           |on b.xlxdbs = a.xlxdbs
                           |join (
                           |    select t.xlxdbs,t.bysdl
                           |    from xlxstjxx t  --线路线损统计信息
                           |    where t.ny = ${nowMonth}
                           |    ) c
                           |on c.xlxdbs = b.xlxdbs
                           |join (
                           |    select t2.xlxdbs,t2.yhbh,t2.yhmc,t2.yhlbdm,t2.glys,t2.jfdl,t2.gddwbm
                           |    from (
                           |        select t1.xlxdbs,t1.yhbh,t1.yhmc,t1.yhlbdm,
                           |        t1.ygzdl/sqrt(t1.ygzdl*t1.ygzdl+t1.wgzdl*t1.wgzdl) glys,t1.jfdl,t1.gddwbm
                           |        from (
                           |            select t.xlxdbs,t.yhbh,t.yhmc,t.yhlbdm,sum(t.ygzdl) ygzdl,
                           |                sum(t.wgzdl) wgzdl,sum(t.jfdl) jfdl,t.gddwbm
                           |            from jldxx t
                           |            join xlxd l
                           |                on t.xlxdbs = l.xlxdbs
                           |            where t.dfny = ${nowMonth} and t.yhlbdm in ('10','11') group by t.xlxdbs,t.yhbh,t.yhmc,t.yhlbdm,t.gddwbm
                           |            ) t1
                           |        where (t1.ygzdl <> 0 or t1.wgzdl <> 0)
                           |        ) t2
                           |        where t2.glys < 0.8
                           |    ) d
                           |on d.xlxdbs = c.xlxdbs
                           |lateral view outer explode(split(getbdzkhb(d.xlxdbs),',')) k1 as bdzkhbhh
                           |where if(sign(c.bysdl)=1,b.zbzjfdl/c.bysdl,0) > 0.7
                        """.stripMargin
            sparkSession.sql(xl_53).createOrReplaceTempView("res_gk_glyspdzgbqd")
            sparkSession.sql("select * from res_gk_glyspdzgbqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_glyspdzgbqd")

            //功率因数偏低的专、公变清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_glyspdzgbqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_glyspdzgbqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GLYSPDZGBQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GLYSPDZGBQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_glyspdzgbqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_glyspdzgbqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_glyspdzgbqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl53',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则5.3运行${(end-start)/1000}秒")

    }

}
