package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._

object TaiQu1Service {

    def taiqu1Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        start = System.currentTimeMillis()
        try{
            //_tqcjwzl 台区采集完整率 取营销  
            sparkSession.sql(
                s"""
                   |select
                   |    x.tqbs,
                   |    round(count(distinct case when (c.scbss is not null and c.bcbss is not null and c.scbss != 0 and c.bcbss != 0) then c.jldbh else null end)/count(distinct c.jldbh)*100,3) cjwzl --采集完整率
                   |from cbxx c
                   |join jld j on j.jldbh=c.jldbh
                   |join all_tq x on x.tqbs = j.tqbs
                   |where c.dfny= ${nowMonth}
                   |    and ((c.yhlbdm = '60' and c.sslxdm in ('121','221')) or (c.yhlbdm = '20' and c.sslxdm = '121')
                   |    or (c.yhlbdm = '40' and c.sslxdm = '221'))
                   |group by x.tqbs
                   |
                 """.stripMargin).createOrReplaceTempView("tqcjwzl") //台区采集完整率

            val tq_11 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm yhlbdm,j.jldbh,c.zcbh,handleNumber(c.zhbl),
                   |    getycgzbh(${Constant.TQ_11}) ycgzbh,getdsjbm(t.gddwbm) dsjbm,getqxjbm(t.gddwbm) qxjbm,
                   |    getgdsbm(t.gddwbm) gdsbm,getzzmc(getdsjbm(t.gddwbm)) dsj,getzzmc(getqxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getgdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from cbxx c
                   |join ydkh y on y.yhbh = c.yhbh
                   |join jld j on j.jldbh = c.jldbh and j.jldytdm <> '410'
                   |join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |join tqcjwzl w on w.tqbs = t.tqbs and w.cjwzl < 100  --台区采集完整率
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where c.dfny = ${nowMonth}
                   |    and (c.scbss is null or c.bcbss is null or c.scbss = 0 or c.bcbss = 0)
                   |    and ((c.yhlbdm = '60' and c.sslxdm in ('121','221')) or (c.yhlbdm = '20' and c.sslxdm = '121')
                   |    or (c.yhlbdm = '40' and c.sslxdm = '221'))
                   |    and (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm<>'2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_11).createOrReplaceTempView("res_gk_wcjyhqd")
            sparkSession.sql("select * from res_gk_wcjyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_wcjyhqd")

            //月台区未采集用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wcjyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_ytqwcjyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_YTQWCJYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_WCJYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_wcjyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_wcjyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wcjyhqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq11',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.1运行${(end-start)/1000}秒")

        //台区1.2 起止表码时标平移检查       fzqjfdl未确定
        //2019年11月12日16:42:45 lixc 添加不存在换表信息记录中gpdmzq.npmis_lc_hbxxjl  11.18 已修改
        start = System.currentTimeMillis()
        try{
            val tq_12 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,c.yhbh,c.yhmc,y.yhlbdm,c.jldbh,c.zcbh,handleNumber(c.scbss) qm,
                   |    handleTime(c.sccbrq) qmcjsj,handleNumber(c.bcbss) zm,handleTime(c.cbsj) zmcjsj,
                   |    handleNumber(c.zhbl),handleNumber(c.bjdl) dl,null fzqjfdl,getycgzbh(${Constant.TQ_12}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from cbxx c
                   |join ydkh y on y.yhbh = c.yhbh
                   |join jld j on j.jldbh = c.jldbh and j.jldytdm <> '410'
                   |join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where c.dfny = ${nowMonth}
                   |    and c.scbss is not null and c.bcbss is not null
                   |    and (c.cbsj <> '${_addOneMonth}-01 00:00:00' or c.sccbrq <> '${_nowMonth}-01 00:00:00')
                   |    and c.bqcbcs = 1
                   |    and getYearMonth(y.lhrq) != ${nowMonth}
                   |    and ((c.yhlbdm = '60' and c.sslxdm in ('121','221')) or (c.yhlbdm = '20' and c.sslxdm = '121')
                   |    or (c.yhlbdm = '40' and c.sslxdm = '221'))
                   |    and (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm <> '2' and not exists (select 1 from hbxxjl t where t.yxdnbbs = c.yxdnbbs and t.dfny = c.dfny)
                   |
                 """.stripMargin
            sparkSession.sql(tq_12).createOrReplaceTempView("res_gk_bmpyyhqd")
            sparkSession.sql("select * from res_gk_bmpyyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bmpyyhqd")

            //表码平移用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bmpyyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_bmpyyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_BMPYYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BMPYYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_bmpyyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bmpyyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bmpyyhqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq12',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.2运行${(end-start)/1000}秒")

        //台区1.3手工抄表方式检查   sslxdm,sslx这两个字段是新加的\
        start = System.currentTimeMillis()
        try{
            val tq_13 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,c.yhbh,c.yhmc,y.yhlbdm,c.jldcbfsdm cbfsdm,c.jldbh,c.zcbh,
                   |    handleNumber(c.scbss) qm,handleTime(c.sccbrq) qmcjsj,handleNumber(c.bcbss) zm,
                   |    handleTime(c.cbsj) zmcjsj,handleNumber(c.zhbl),handleNumber(c.bjdl) dl,
                   |    getycgzbh(${Constant.TQ_13}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdmbmmc('CBFSDM',c.jldcbfsdm) cbfs,
                   |    getdqbm(t.gddwbm) dqbm,${nybm},c.sslxdm,getdmbmmc('SSLXDM',c.sslxdm) sslx
                   |from cbxx c
                   |join ydkh y on y.yhbh = c.yhbh
                   |join jld j on j.jldbh = c.jldbh and j.jldytdm <> '410'
                   |join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where c.dfny = ${nowMonth}
                   |    and c.jldcbfsdm not like '4%'
                   |    and ((c.yhlbdm = '60' and c.sslxdm in ('121','221')) or (c.yhlbdm = '20' and c.sslxdm = '121')
                   |    or (c.yhlbdm = '40' and c.sslxdm = '221'))
                   |    and (y.yhlbdm in ('20','60') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj in ('02','03'))))
                   |    and y.yhztdm<>'2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_13).createOrReplaceTempView("res_gk_sgcbyhqd")
            sparkSession.sql("select * from res_gk_sgcbyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_sgcbyhqd")

            //手工抄表用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_sgcbyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_sgcbyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_SGCBYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_SGCBYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_sgcbyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_sgcbyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_sgcbyhqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq13',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则 1.3运行${(end-start)/1000}秒")

        //台区1.4 营销与计量表码不一致  
        start = System.currentTimeMillis()
        try{
            val tq_14 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(j.xlxdbs) bdzbs,getbdzbh(j.xlxdbs) bdzbh,getbdzmc(j.xlxdbs) bdzmc,k1.bdzkhbhh,
                   |    k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,c.jldbh,c.zcbh,c.scbss yxxtqm,c.sccbrq yxxtqmcjsj,
                   |    c.bcbss yxxtzm,c.cbsj yxxtzmcjsj,c.zhbl yxxtbl,c.bjdl yxxtdl,b.zxygz_sc jlxtqm,
                   |    b.sjsj_sc jlxtqmcjsj,b.zxygz jlxtzm,b.sjsj jlxtzmcjsj,b.zhbl jlxtbl,
                   |    coalesce((case when sslxdm = '121' then b.zxygz - b.zxygz_sc when sslxdm = '221' then b.fxygz - b.fxygz_sc end) * b.zhbl,0) jlxtdl,
                   |    getycgzbh(${Constant.TQ_14}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from cbxx c
                   |join jld j on c.jldbh = j.jldbh
                   |join ydkh y on j.yhbh = y.yhbh and y.yhlbdm in ('60','20') and y.yhztdm <> 2
                   |join tq t on t.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |join jlbmhb b on b.yhbh = c.yhbh and b.jldbh = c.jldbh and b.bjzcbh = c.zcbh  --计量编码合并
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where c.dfny = ${nowMonth} and ((c.sslxdm = '121' and c.yhlbdm in ('60','20') and (c.scbss <> b.zxygz_sc or c.bcbss <> b.zxygz))
                   |    or (c.sslxdm = '221' and c.yhlbdm = '60' and (c.scbss <> b.fxygz_sc or c.bcbss <> b.fxygz)))
                   |
                 """.stripMargin

            sparkSession.sql(tq_14).repartition(resultPartition).createOrReplaceTempView("res_gk_yxyjlbmbyzqd")
            sparkSession.sql("select * from res_gk_yxyjlbmbyzqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_yxyjlbmbyzqd")

            //营销与计量表码不一致清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxyjlbmbyzqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yxyjlbmbyzqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_YXYJLBMBYZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_YXYJLBMBYZQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yxyjlbmbyzqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxyjlbmbyzqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxyjlbmbyzqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq14',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.4运行${(end-start)/1000}秒")

        //台区1.5
        start = System.currentTimeMillis()
        try{
            sparkSession.sql(
                s"""
                   |select
                   |    tqbs,
                   |    regexp_replace( --正则表达式匹配替换
                   |        concat_ws(',',sort_array(   --正序排序
                   |            collect_list(   --列转行
                   |                concat_ws(':',cast(a.rownum as string),cast(nvl(bygdl-bysdl,0) as string)))) --拼接
                   |                ),'\\\\d+\\\\:','') shdl
                   |from (
                   |    select *,row_number() over(partition by tqbs order by tqbs,ny) rownum
                   |    from tqxstjxx  --台区线损统计信息
                   |    where ny in (${nowMonth},${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5})
                   |    ) a
                   |group by tqbs
                   |
                   |
                 """.stripMargin).createOrReplaceTempView("xs")

            sparkSession.sql(
                s"""
                   |select
                   |    b.yhbh,b.jldbh,b.jfdl,j.tqbs,j.xlxdbs
                   |from(
                   |    select
                   |        yhbh,jldbh,
                   |        regexp_replace(concat_ws(',',sort_array(collect_list(concat_ws(':',cast(a.rownum as string),cast(if(jfdl=null or jfdl='',0.00,jfdl) as string))))),'\\\\d+\\\\:','') jfdl
                   |    from (
                   |        select
                   |            *,row_number() over(partition by yhbh,jldbh order by yhbh,jldbh,dfny) rownum
                   |        from  jldxx_all
                   |        where dfny in (${nowMonth},${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5}) and yhlbdm = '20' and yhztdm <> 2) a
                   |    group by yhbh,jldbh) b
                   |join jldxx j on j.yhbh = b.yhbh and j.jldbh = b.jldbh and j.dfny = ${nowMonth}
                   |
                 """.stripMargin).createOrReplaceTempView("yh")

            sparkSession.sql(
                s"""
                   |select *
                   |from (
                   |    select
                   |        y.yhbh,y.jldbh,getCorr(y.jfdl,x.shdl) xgxs
                   |    from xs x
                   |    join yh y on x.tqbs = y.tqbs) a
                   |where xgxs < -0.7
                   |
                 """.stripMargin).createOrReplaceTempView("xgxs")

            val tq_15 =
                s"""
                   |select
                   |    distinct ${creator_id} ,${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,j1.xlxdbs,l.xlbh,l.xlmc,
                   |    t.tqbs,t.tqbh,t.tqmc,getbdzbs(j1.xlxdbs) bdzbs,getbdzbh(j1.xlxdbs) bdzbh,
                   |    getbdzmc(j1.xlxdbs) bdzmc,k1.bdzkhbhh,k2.tqkhbhh,j1.yhbh,j1.yhmc,j1.yhlbdm,
                   |    j1.jldbh,c1.zcbh,handleNumber(c1.scbss) bydlqm,
                   |    handleNumber(c1.bcbss) bydlzq,handleNumber(c1.zhbl),handleNumber(j1.jfdl),
                   |	handleNumber(c2.scbss) sydlqm,handleNumber(c2.bcbss) sydlzq,handleNumber(c2.zhbl),
                   |    handleNumber(j2.jfdl),x.xgxs,getycgzbh(${Constant.TQ_15}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j1.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from xgxs x
                   |join jldxx j1 on x.yhbh = j1.yhbh and x.jldbh = j1.jldbh and j1.dfny = ${nowMonth}
                   |join jldxx j2 on x.yhbh = j2.yhbh and x.jldbh = j2.jldbh and j2.dfny = ${lastMonth}
                   |join all_xlxd l on l.xlxdbs = j1.xlxdbs
                   |join tq t on t.tqbs = j1.tqbs
                   |join cbxx c1 on c1.yhbh = x.yhbh and c1.jldbh = x.jldbh and c1.dfny = ${nowMonth} and c1.sslxdm = '121'
                   |join cbxx c2 on c2.yhbh = x.yhbh and c2.jldbh = x.jldbh and c2.dfny = ${lastMonth} and c2.sslxdm = '121'
                   |lateral view outer explode(split(getbdzkhb(j1.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |
                 """.stripMargin

            sparkSession.sql(tq_15).createOrReplaceTempView("res_gk_dlycbdyhqd")
            sparkSession.sql("select * from res_gk_dlycbdyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dlycbdyhqd")

            //电量异常波动用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dlycbdyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dlycbdyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

//            sparkSession.sql(s"insert into ${writeSchema}.gk_dlycbdyhqd_his select getUUID(),*,tjsj fqrq from res_gk_dlycbdyhqd")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dlycbdyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dlycbdyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dlycbdyhqd_ycgddwxlgx")
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
        sparkSession.sql(
            s"""
               |select *
               |from ruleState
               |union all
               |select getFormatDate(${end}) recordtime,'tq15',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime
               |
             """.stripMargin).createOrReplaceTempView("ruleState")
        println(s"规则 1.5运行${(end-start)/1000}秒")

        //台区1.6
        start = System.currentTimeMillis()
        try{
            val sumJfdl =
                s"""
                   |select
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.tqbs,j.jldbh,sum(j.jfdl) yhjfdl
                   |from jldxx j
                   |where j.dfny = ${nowMonth} and j.yhlbdm in ('20','60')
                   |group by j.yhbh,j.yhmc,j.yhlbdm,j.tqbs,j.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(sumJfdl).createOrReplaceTempView("sumJfdl")

            val avlJfdl =
                s"""
                   |select
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.tqbs,j.jldbh,sum(j.jfdl)/5 yhpjjfdl
                   |from jldxx_all j
                   |where j.dfny in (${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5}) and j.yhlbdm in ('20','60')
                   |group by j.yhbh,j.yhmc,j.yhlbdm,j.tqbs,j.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(avlJfdl).createOrReplaceTempView("avlJfdl")

            val lastFiveJfdl =
                s"""
                   |select
                   |    j.yhbh,j.jldbh,sum(case when j.dfny = ${lastMonth} then j.jfdl end) dl1,
                   |    sum(case when j.dfny = ${lastMonth2} then j.jfdl end) dl2,
                   |    sum(case when j.dfny = ${lastMonth3} then j.jfdl end) dl3,
                   |    sum(case when j.dfny = ${lastMonth4} then j.jfdl end) dl4,
                   |    sum(case when j.dfny = ${lastMonth5} then j.jfdl end) dl5
                   |from jldxx_all j
                   |where j.dfny in (${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5}) and j.yhlbdm in ('20','60')
                   |group by j.yhbh,j.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(lastFiveJfdl).createOrReplaceTempView("lastFiveJfdl")

            val tqbdl =
                s"""
                   |select
                   |    a.yhbh,a.yhmc,a.yhlbdm,a.tqbs,a.jldbh,(a.yhjfdl-b.yhtqjfdl)/b.yhtqjfdl tbbdl
                   |from sumJfdl a
                   |join (
                   |    select
                   |        j.yhbh,j.yhmc,j.yhlbdm,j.tqbs,j.jldbh,sum(j.jfdl) yhtqjfdl
                   |    from jldxx_all j
                   |    where j.dfny = ${tqny} and j.yhlbdm in ('20','60')
                   |    group by j.yhbh,j.yhmc,j.yhlbdm,j.tqbs,j.jldbh
                   |    ) b
                   |on b.yhbh = a.yhbh and b.jldbh = a.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(tqbdl).createOrReplaceTempView("tqbdl")

            val tq_16 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,l.tqbs,l.tqbh,l.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,a.yhbh,a.yhmc,a.yhlbdm,cb.jldbh,cb.zcbh,
                   |    handleNumber(b.dl5),handleNumber(b.dl4),handleNumber(b.dl3),handleNumber(b.dl2),
                   |    handleNumber(b.dl1),handleNumber(a.yhjfdl),handleNumber(c.yhpjjfdl),
                   |    handleNumber(round((a.yhjfdl-c.yhpjjfdl)/c.yhpjjfdl*100,6)) hbbdl,
                   |    handleNumber(round(d.tbbdl*100,6)),handleNumber(round(a.yhjfdl/l.bygdl*100,6)) dlzxsgdlbl,
                   |    getycgzbh(${Constant.TQ_16}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,
                   |getdmbmmc('YHLBDM',a.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from tqxstjxx l
                   |join sumJfdl a on a.tqbs = l.tqbs
                   |join lastFiveJfdl b on a.yhbh = b.yhbh and a.jldbh = b.jldbh
                   |join avlJfdl c on a.yhbh = c.yhbh and a.jldbh = c.jldbh
                   |join tqbdl d on a.yhbh = d.yhbh and a.jldbh = d.jldbh
                   |join cbxx cb on a.yhbh = cb.yhbh and a.jldbh = cb.jldbh and cb.sslxdm = '121'
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(l.tqbs),',')) k2 as tqkhbhh
                   |where l.ny = ${nowMonth} and l.bygdl <> 0
                   |    and (a.yhjfdl/l.bygdl > 0.05 or a.yhjfdl > (case a.yhlbdm when '60' then 10000 when '20' then 900 end)
                   |    or(a.yhjfdl-c.yhpjjfdl)/c.yhpjjfdl > 0.3 or d.tbbdl > 0.3)
                   |
                 """.stripMargin

            sparkSession.sql(tq_16).createOrReplaceTempView("res_gk_dlzbyclsdlqd")
            sparkSession.sql("select * from res_gk_dlzbyclsdlqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_dlzbyclsdlqd")

            //电量占比异常历史电量清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dlzbyclsdlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dlzbyclsdlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_DLZBYCLSDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_DLZBYCLSDLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dlzbyclsdlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dlzbyclsdlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dlzbyclsdlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq16',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则 1.6运行${(end-start)/1000}秒")

        //2019年12月16日 join _tbdlxx改为left join _tbdlxx
        //台区1.7电量退补用户信息
        start = System.currentTimeMillis()
        try{
            val tq_17 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,
                   |    t.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,c.zcbh,handleNumber(c.scbss) qm,
                   |    handleNumber(c.bcbss) zm,handleNumber(c.zhbl),handleNumber(c.bjdl) dl,d.gzdbh tbgdh,
                   |    handleNumber(d.ygdl) tbdl,getycgzbh(${Constant.TQ_17}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from jldxx j
                   |inner join cbxx c on c.jldbh = j.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '121'
                   |inner join tq t on j.tqbs = t.tqbs
                   |left join tbdlxx d on d.jldbh = j.jldbh and d.ccyf = ${nowMonth}  --退补电量信息
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |    and (j.bqcbcs >= 2 or j.tbgzdbh is not null)
                   |    and j.yhztdm <> '2'
                   |union
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,c.zcbh,handleNumber(c.scbss) qm,
                   |    handleNumber(c.bcbss) zm,handleNumber(c.zhbl),handleNumber(c.bjdl) dl,d.gzdbh tbgdh,
                   |    handleNumber(d.tbdl) tbdl,getycgzbh(${Constant.TQ_17}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from jldxx j
                   |inner join cbxx c on c.jldbh = j.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '121'
                   |inner join tq t on j.tqbs = t.tqbs
                   |join tbmx d on d.jldbh = j.jldbh and d.dfny = ${nowMonth} --退补明细
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where j.jldytdm <> '410' and j.dfny = ${nowMonth} and d.tblx = '1'
                   |    and (j.bqcbcs >= 2 or j.tbgzdbh is not null)
                   |    and j.yhztdm <> '2'
                   |
                 """.stripMargin
            sparkSession.sql(tq_17).createOrReplaceTempView("res_gk_dltbyhqd")
            sparkSession.sql("select * from res_gk_dltbyhqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_dltbyhqd")

            //电量退补用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dltbyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dltbyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_DLTBYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_DLTBYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dltbyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dltbyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dltbyhqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq17',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则 1.7运行${(end-start)/1000}秒")

        //台区1.8 未统计地方电厂户电量   		fxdl 未确定
        //2019年11月19日19:24:57 lixc 修改地方电厂户电压等级判断条件，1电量是0,2不存在地方电厂户的反向有功总示数类型的抄表数据
        //2020-01-13  (select 1 from _gdljfhzxx a,_dcfzxx b where b.dcbh = a.dcbh and a.dcbh = y.yhbh and a.gdyf = ${nowMonth} and b.dydj = '08') 去掉_gdljfhzxx
        start = System.currentTimeMillis()
        try{
            val tq_18 =
                s"""
                   |select
                   |    ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,
                   |    k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,d.jldbh,b.zcbh,0 fxdl,getycgzbh(${Constant.TQ_18}) ycgzbh,
                   |    getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm},dc.dydj,getdmbmmc('DYDJDM',dc.dydj) dydj
                   |from ydkh y
                   |join jld d on d.yhbh = y.yhbh
                   |join jlddnbgx g on g.jldbh = d.jldbh --计量点运行电能表关系
                   |join yxdnb b on b.yxdnbbs = g.yxdnbbs --运行电能表
                   |join all_xlxd l on l.xlxdbs = d.xlxdbs
                   |join tq t on t.tqbs = d.tqbs
                   |join cbxx c on c.yhbh = y.yhbh and c.jldbh = d.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '221' and c.bjdl = 0 and c.jbdl = 0
                   |join dcfzxx dc on dc.dcbh = y.yhbh and dc.dydj in ('02','03')  --电厂辅助信息表
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where y.yhlbdm = '40' and y.yhztdm <> '2'
                   |
                   |union all
                   |select
                   |    ${creator_id},${create_time},${update_time},${updator_id},t.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,d.jldbh,b.zcbh,0 fxdl,
                   |    getycgzbh(${Constant.TQ_18}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(t.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,
                   |    ${nybm},dc.dydj,getdmbmmc('DYDJDM',dc.dydj) dydj
                   |from ydkh y
                   |join jld d on d.yhbh = y.yhbh
                   |join jlddnbgx g on g.jldbh = d.jldbh
                   |join yxdnb b on b.yxdnbbs = g.yxdnbbs
                   |join all_xlxd l on l.xlxdbs = d.xlxdbs
                   |join tq t on t.tqbs = d.tqbs
                   |join dcfzxx dc on dc.dcbh = y.yhbh and dc.dydj in ('02','03')
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where y.yhlbdm = '40' and y.yhztdm <> '2'
                   |    and not exists (select 1 from cbxx c where c.sslxdm = '221'
                   |    and c.dfny = ${nowMonth} and c.yhbh = y.yhbh and c.jldbh = d.jldbh)
                   |
                 """.stripMargin
            sparkSession.sql(tq_18).createOrReplaceTempView("res_gk_wtjdfdchdlqd")
            sparkSession.sql("select * from res_gk_wtjdfdchdlqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_wtjdfdchdlqd")

            //未统计地方电厂户电量清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wtjdfdchdlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_wtjdfdchdlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_WTJDFDCHDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_WTJDFDCHDLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_wtjdfdchdlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_wtjdfdchdlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wtjdfdchdlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq18',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则 1.8运行${(end-start)/1000}秒")

        //台区1.9 非周期性计费电量用户   fzqjfdl未确定
        start = System.currentTimeMillis()
        try{
            val tq_19 =
                s"""
                   |select
                   |    ${creator_id},${create_time},${update_time},${updator_id},tq.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,tq.tqbs,tq.tqbh,tq.tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,
                   |    k2.tqkhbhh,y.yhbh,y.yhmc,y.yhlbdm,j.jldbh,c.zcbh,handleNumber(c.scbss) qm,
                   |    handleNumber(c.bcbss) zm,handleNumber(c.zhbl),handleNumber(c.bjdl) dl,
                   |    f.gzdbh fzqjfgdh,(coalesce(j.jfdl, 0) + coalesce(j.mfdl, 0)) fzqjfdl,
                   |    getycgzbh(${Constant.TQ_19}) ycgzbh,getDsjbm(tq.gddwbm) dsjbm,getQxjbm(tq.gddwbm) qxjbm,
                   |    getGdsbm(tq.gddwbm) gdsbm,getzzmc(getDsjbm(tq.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(tq.gddwbm)) qxj,getzzmc(getGdsbm(tq.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(tq.gddwbm) dqbm,${nybm}
                   |from ydkh y
                   |join fzqjfsq f on f.yhbh = y.yhbh and f.ywlbdm = '0700' and f.cldfny = ${nowMonth}
                   |    and f.ywzlbh in ('F-CSG-MK0514-10-01','F-CSG-MK0514-10-02','F-CSG-MK0514-11-01','F-CSG-MK0514-11-02','F-CSG-MK0540-01-01')  --非周期计费申请
                   |join jldxx j on j.gzdbh = f.clgzdbh and j.yhbh = f.yhbh and j.dfny = f.cldfny
                   |join cbxx c on c.yhbh = j.yhbh and c.jldbh = j.jldbh and c.dfny = j.dfny and c.sslxdm = '121'
                   |join tq tq on tq.tqbs = j.tqbs
                   |join all_xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(tq.tqbs),',')) k2 as tqkhbhh
                   |where y.yhlbdm = '20'
                   |    and not exists (select 1 from tqsdlmx x where x.yhbh = j.yhbh and x.jldbh = j.jldbh
                   |    and (coalesce(j.jfdl, 0) + coalesce(j.mfdl, 0)) = x.ygzdl)
                   |    and f.yhbh in (
                   |                    select t.yhbh
                   |                    from fzqjfsq t
                   |                    join ydkh y on y.yhbh = t.yhbh and y.yhlbdm = '20'
                   |                    where t.cldfny = ${nowMonth}
                   |                    group by t.yhbh having count(1) > 1)
                   |
                 """.stripMargin
            sparkSession.sql(tq_19).createOrReplaceTempView("res_gk_fzqxjfdlyhqd")
            sparkSession.sql("select * from res_gk_fzqxjfdlyhqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_fzqxjfdlyhqd")

            //非周期性计费电量用户清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_fzqxjfdlyhqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_fzqxjfdlyhqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_FZQXJFDLYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_FZQXJFDLYHQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_fzqxjfdlyhqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_fzqxjfdlyhqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_fzqxjfdlyhqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq19',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则 1.9运行${(end-start)/1000}秒")
    }

}
