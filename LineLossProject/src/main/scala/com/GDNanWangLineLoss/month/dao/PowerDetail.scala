package com.GDNanWangLineLoss.month.dao

import com.GDNanWangLineLoss.month.bean.Variables.{lastMonth, lastMonth2, nowMonth}
import org.apache.spark.sql.SparkSession

/**
  * 电量明细
  */
object PowerDetail {

    private val  url ="10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"
    var xhjyqybz = 0//  现货交易启用标志
    var gstqbz = 0 //购售同期标志
    var xsgdbz =0 //  线损过渡标志
    var gdqcbcs = 0 //过渡期抄表次数


    var cbnyq = 0
    var cbnyz = 0
    var dcqsyq = 0
    var dcqssy = 0

    def powerDetail(sparkSession:SparkSession)={

        // 库表全局系数
        val xt_qjxs=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "xt_qjxs")) //todo 库表全局系数
          .format("org.apache.kudu.spark.kudu")
          .load()

        xt_qjxs.createOrReplaceTempView("xt_qjxs")

        /*-------------------------------------------------------------------*/

        //v1  现货交易启用标志
        val xhjyqybzArr=xt_qjxs.collect()

        for (num <- xhjyqybzArr){
            xhjyqybz = num.getAs[Int]("xhjyqybz")  //这东西返回给哪里调用？  //  现货交易启用标志
        }
        println(xhjyqybz)

        /*-------------------------------------------------------------------*/


        //v2 购售同期标志
        val gstqbzArr =sparkSession.sql(
            s"""
               |select cast(t.xsz as Int) gstqbz
               |from xt_qjxs t
               |where t.xsbh = '52624'
               |
             """.stripMargin).collect()

//        var gstqbz = 0
        for (num <- gstqbzArr){
            gstqbz=num.getAs[Int]("gstqbz") // 购售同期标志
        }
                //销毁临时表
                //sparkSession.catalog.dropTempView("_yxxt_npmis_xt_qjxs")
        println(gstqbz)



        /*-------------------------------------------------------------------*/
        //v3  线损过渡标志
        val xsgdbzArr =sparkSession.sql(
            s"""
               |select cast(t.xsz as Int) xsgdbz
               |from xt_qjxs t
               |where t.xsbh = '54624'
               |
             """.stripMargin).collect()
//        var xsgdbz = 0
        for (num <- xsgdbzArr){
            xsgdbz = num.getAs[Int]("xsgdbz") //  线损过渡标志
        }
                //销毁临时表
                //sparkSession.catalog.dropTempView("_yxxt_npmis_xt_qjxs")
        println(xsgdbz)



        /*-------------------------------------------------------------------*/
        //4 过渡期抄表次数
        val gdqcbcsArr =sparkSession.sql(
            s"""
               |select cast(t.xsz as Int) gdqcbcs
               |from xt_qjxs t
               |where t.xsbh = '52625'
               |
             """.stripMargin).collect()

        for (num <- gdqcbcsArr){
            gdqcbcs = num.getAs[Int]("gdqcbcs") // 过渡期抄表次数
        }
                 //销毁临时表
                //sparkSession.catalog.dropTempView("_yxxt_npmis_xt_qjxs")
        println(gdqcbcs)
        /*-----------上面求了4个变量--------------------------------------------------------*/
//        var cbnyq = 0;
//        var cbnyz = 0;
//        var dcqsyq = 0;
//        var dcqssy = 0;

        /*-------------------------------------------------------------------*/
        // 准备专变非周期数据TMP_GK_XSFZQJFDL
        val xsfzqjfdl =
            s"""
               |select
               |    a.gddwbm,a.dfny,a.yhlbdm,a.xlxdbs,a.tqbs,a.jfdl,a.mfdl,
               |    a.cbrq,a.jldbh,a.ywlbdm,a.cbqdbh,a.gzdbh,a.dwbm,a.djdm,a.dqbm
               |from jldxx a
               |where a.dfny = ${nowMonth}
               |    and a.yhlbdm in ('10', '11', '30','98')
               |    and a.djdm not in ('00000000', '39002101')
               |    and a.jldydjdm not in ('10', '12', '13', '15')
               |    and a.cbjhbh like 'f%'
               |union all
               |select
               |    a.gddwbm,a.dfny,a.yhlbdm,a.xlxdbs,a.tqbs,a.jfdl,a.mfdl,
               |    a.cbrq,a.jldbh,a.ywlbdm,a.cbqdbh,a.gzdbh,a.dwbm,a.djdm,a.dqbm
               |from jldxx a
               |where a.dfny = ${lastMonth}
               |    and a.yhlbdm in ('10', '11', '30','98')
               |    and a.djdm not in ('00000000', '39002101')
               |    and a.jldydjdm not in ('10', '12', '13', '15')
               |    and a.cbjhbh like 'f%'
               |
             """.stripMargin
        sparkSession.sql(xsfzqjfdl).createOrReplaceTempView("xsfzqjfdl")  //todo 中文翻译是什么

        /*-------------------------------------------------------------------*/
        //线路售电量明细
        // 1线路售电量专变用户周期部分
        // 2线路售电量台区考核表本月一次抄表部分
        // 3线路售电量违约窃电部分
        // 4线路售电量变电站考核表反向电量部分
        // 5线路售电量变电站考核表反向退补电量部分
        // 6线路售电量专变公用的扣减电量部分
        // 7线路售电量专变用户非周期部分
        // 8线路售电量台区考核户反向部分
        // 9线路售电量台区考核表上月二次抄表部分
        val xlsdlmx =
            s"""
               |select * from (
               |-- 1 线路售电量专变用户周期部分
               |  select
               |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,null zcbh,cbqdbh,
               |    yhlbdm,(coalesce(jfdl, 0) + coalesce(mfdl, 0)) ygzdl,jfrl,null cbsxh,null cbrbs,
               |    null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'1' dllx
               |  from jldxx t
               |  where t.djdm <> '00000000'
               |      and t.jldydjdm not in ('10', '12', '13', '15')
               |      and t.yhlbdm in ('10', '11', '30','98')
               |      and t.dfny = ${nowMonth} and (t.cbjhbh not like 'f%' or t.cbjhbh is null)
               |  union all
               |-- 2 线路售电量台区考核表本月一次抄表部分
               |  select
               |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,null zcbh,cbqdbh,
               |    yhlbdm,(coalesce(ygzdl, 0) - coalesce(ygbsdl, 0)) ygzdl,jfrl,null cbsxh,
               |    null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'2' dllx
               |  from jldxx t
               |  where t.dfny = ${nowMonth} and t.yhlbdm = '60' and t.bqcbcs = 1
               |  union all
               |-- 3 线路售电量违约窃电部分
               |  select
               |    t.gddwbm,t.dfny,t.yhbh,t.yhmc,0 jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,null zcbh,
               |    t.cbqdbh,t.yhlbdm,coalesce(t.jfdl, 0) ygzdl,0 jfrl,null cbsxh,null cbrbs,t.czsj,t.jldbh,
               |    null scbss,null bcbss,nullzhbl,0 ygbsdl,null sjcbfsdm,'3' dllx
               |  from ysdfjl t
               |  where t.ywlbdm = '0500' and t.djdm not in ('00000000', '39002101')
               |      and t.yhlbdm <> '20'
               |      and t.dfny = ${nowMonth}
               |  union all
               |-- 4 线路售电量变电站考核表反向电量部分
               |  select
               |    e.gddwbm, e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs, e.yddz,e.tqbs,f.zcbh,
               |    e.cbqdbh,e.yhlbdm,(coalesce(f.bjdl,0) + coalesce(f.jbdl,0)) ygzdl,e.jfrl,f.cbsxh,null cbrbs,
               |    f.cbsj,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'4' dllx
               |  from jldxx e, cbxx f
               |  where e.dfny = ${nowMonth}
               |      and e.yhlbdm = '80' and f.yhbh = e.yhbh
               |      and f.gzdbh = e.gzdbh and e.ywlbdm = f.ywlbdm and f.dfny = e.dfny and f.jldbh = e.jldbh
               |      and f.sslxdm = '221'
               |  union all
               |-- 5 线路售电量变电站考核表反向退补电量部分
               |  select
               |    e.gddwbm,e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs,e.yddz,e.tqbs,f.zcbh,e.cbqdbh,
               |    e.yhlbdm,coalesce(e.ygtbdl, 0) ygzdl,e.jfrl,f.cbsxh,null cbrbs,f.cbsj,e.jldbh,f.scbss,f.bcbss,
               |    f.zhbl,e.ygbsdl,f.sjcbfsdm,'5' dllx
               |  from jldxx e, cbxx f
               |  where e.dfny = ${nowMonth}
               |      and e.yhlbdm = '80' and f.yhbh = e.yhbh
               |      and f.gzdbh = e.gzdbh and f.dfny = e.dfny and f.jldbh = e.jldbh
               |      and f.sslxdm = '221'
               |  union all
               |-- 6 线路售电量专变公用的扣减电量部分
               |  select
               |    e.gddwbm, e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs,e.yddz, e.tqbs,null zcbh,
               |    e.cbqdbh, e.yhlbdm, e.ygfbkjdl ygzdl,e.jfrl,null cbsxh,null cbrbs,null cbsj,e.jldbh,
               |    null scbss,null bcbss,null zhbl,e.ygbsdl,null sjcbfsdm,'6' dllx
               |  from jldxx e, xszbgyyh f
               |  where e.dfny = ${nowMonth}
               |      and e.yhlbdm = '10'
               |      and e.xlxdbs = f.xlxdbs and e.yhbh = f.yhbh
               |      and not exists (select 1 from hs_jldgx g where f.yhbh = g.yhbh and g.yhbh = g.glyhbh and g.dfny = e.dfny)
               |  union all
               |-- 7 线路售电量专变用户非周期部分
               |  select
               |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,null zcbh,cbqdbh,yhlbdm,
               |    (coalesce(jfdl, 0) + coalesce(mfdl, 0)) ygzdl,jfrl,null cbsxh,null cbrbs,null cbsj,
               |    t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'7' dllx
               |  from jldxx t
               |  where exists (
               |                select 1
               |                from xsfzqjfdl e --这个表没发现
               |                join (
               |                  select
               |                    a.cbqdbh,case when a.cblr < 10 then concat('0',a.cblr) else a.cblr end cblr
               |                  from (
               |                    select q.cbqdbh,max(q.cblr) cblr
               |                    from cbqdfz q
               |                    group by q.cbqdbh) a) f
               |                on f.cbqdbh = e.cbqdbh
               |                where e.gzdbh = t.gzdbh and e.ywlbdm = t.ywlbdm and e.jldbh = t.jldbh
               |                      and getDay(e.cbrq) <= concat('202005',f.cblr)
               |                      and getDay(e.cbrq) > concat('${nowMonth}',f.cblr)
               |              )
               |  union all
               |-- 8 线路售电量台区考核户反向部分
               |  select
               |    e.gddwbm, e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs, e.yddz,e.tqbs,f.zcbh,e.cbqdbh, e.yhlbdm,
               |    -coalesce(f.bjdl,0) ygzdl,e.jfrl,f.cbsxh,null cbrbs,
               |    f.cbsj,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'8' dllx
               |  from jldxx e, cbxx f
               |  where e.dfny = ${nowMonth}
               |        and e.yhlbdm = '60' and f.yhbh = e.yhbh
               |        and f.gzdbh = e.gzdbh and e.ywlbdm = f.ywlbdm and f.dfny = e.dfny and f.jldbh = e.jldbh
               |        and f.sslxdm = '221'
               |  union all
               |-- 9 线路售电量台区考核表上月二次抄表部分
               |  select
               |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |    null zcbh,cbqdbh,yhlbdm,(coalesce(ygzdl, 0) - coalesce(ygbsdl, 0)) ygzdl,jfrl,
               |    null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'9' dllx
               |  from jldxx t
               |  where t.dfny = ${lastMonth} and t.yhlbdm = '60' and t.bqcbcs = 2
               | ) a
               |order by a.dllx
               |
             """.stripMargin
        sparkSession.sql(xlsdlmx).createOrReplaceTempView("xlsdlmx") //线路售电量明细



        /*-------------------------------------------------------------------*/
        //台区售电量明细
        // 1台区售电量本月公变部分
        // 2台区售电量单、双月公变上月部分
        // 3台区售电量本月违约窃电部分
        // 4台区售电量单、双月台区上月违约窃电部分
        // 5台区售电量非周期本月一次部分
        // 6台区售电量单、双月非周期上月一次部分
        // 7台区售电量单、双月非周期上上月二次部分
        // 8台区售电量台区考核表户反向电量部分
        // 9台区售电量台单、双月区考核表户上月反向电量部分
        // 10台区售电量非周期上月二次部分
        val tqsdlmx =
            s"""
               |select * from (
               |-- 1台区售电量本月公变部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'1' dllx
               |    from jldxx t
               |    where t.dfny = ${nowMonth} and (t.cbjhbh not like 'f%' or t.cbjhbh is null)
               |         and t.djdm <> '00000000' and t.YHLBDM in('20','98')
               |    union all
               |-- 2台区售电量单、双月公变上月部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'2' dllx
               |    from jldxx t
               |    where t.dfny = ${lastMonth} and (t.cbjhbh not like 'f%' or t.cbjhbh is null)
               |        and t.djdm <> '00000000' and t.yhlbdm in('20','98')
               |        and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |-- 3台区售电量本月违约窃电部分
               |    select
               |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,null jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
               |        null zcbh,t.cbqdbh,t.yhlbdm,coalesce(t.jfdl, 0) ygzdl,null jfrl,
               |        null cbsxh,null cbrbs,t.czsj,t.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'3' dllx
               |    from ysdfjl t
               |    where t.ywlbdm = '0500' and t.djdm not in ('00000000', '39002101')
               |         and t.dfny = ${nowMonth}
               |    union all
               |-- 4台区售电量单、双月台区上月违约窃电部分
               |    select
               |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,null jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
               |        null zcbh,t.cbqdbh,t.yhlbdm,coalesce(t.jfdl, 0) ygzdl,null jfrl,
               |        null cbsxh,null cbrbs,t.czsj,t.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'4' dllx
               |    from ysdfjl t
               |    where t.ywlbdm = '0500' and t.djdm not in ('00000000', '39002101')
               |         and t.dfny = ${lastMonth}
               |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |-- 5台区售电量非周期本月一次部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'5' dllx
               |    from jldxx t
               |    where t.dfny = ${nowMonth} and t.cbjhbh like 'f%' and t.bqcbcs = 1
               |         and t.djdm <> '00000000' and t.yhlbdm = '20'
               |    union all
               |-- 6台区售电量单、双月非周期上月一次部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'6' dllx
               |    from jldxx t
               |    where t.dfny = ${lastMonth} and t.cbjhbh like 'f%' and t.bqcbcs = 1
               |         and t.djdm <> '00000000' and t.yhlbdm = '20'
               |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |-- 7台区售电量单、双月非周期上上月二次部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'7' dllx
               |    from jldxx t
               |    where t.dfny = ${lastMonth2} and t.cbjhbh like 'f%' and t.bqcbcs = 2
               |         and t.djdm <> '00000000' and t.yhlbdm = '20'
               |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |-- 8台区售电量台区考核表户反向电量部分
               |    select
               |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
               |        f.zcbh,t.cbqdbh,t.yhlbdm,(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,
               |        null cbrbs,f.cbsj,t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm,'8' dllx
               |    from jldxx t, _cbxx f
               |    where t.dfny =  ${nowMonth}
               |         and t.yhlbdm = '60'
               |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
               |         and f.sslxdm = '221'
               |    union all
               |-- 9台区售电量台单、双月区考核表户上月反向电量部分
               |    select
               |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
               |        f.zcbh,t.cbqdbh,t.yhlbdm,(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,
               |        null cbrbs,f.cbsj,t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm,'9' dllx
               |    from jldxx t, cbxx f
               |    where t.dfny =  ${lastMonth}
               |         and t.yhlbdm = '60'
               |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
               |         and f.sslxdm = '221'
               |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |    -- 10台区售电量非周期上月二次部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'10' dllx
               |    from jldxx t
               |    where t.dfny = ${lastMonth} and t.cbjhbh like 'f%' and t.bqcbcs = 2
               |         and t.djdm <> '00000000' and t.yhlbdm = '20'
               |) a
               |order by a.dllx
               |
             """.stripMargin
        sparkSession.sql(tqsdlmx).createOrReplaceTempView("tqsdlmx") //台区售电量明细
        /*-------------------------------------------------------------------*/

        // 线路供电量增加地方电厂上网电量 取根据 计量点 从 量测抄表信息 获取电量
        // 线路地方电厂上网电量
        val xldfdcswdlNew =
            s"""
               |select sum(coalesce(c.bjdl, 0) + coalesce(c.jbdl, 0)) bjdl,
               |     c.jldbh,
               |     'bffg' fgbz,
               |     c.yhbh,
               |     e.xlxdbs,
               |     c.dfny ny,
               |     c.dqbm,
               |     c.bqcbcs,
               |     c.sslxdm
               |from cbxx c, jld e
               |where c.dfny = ${nowMonth}
               | and c.yhbh = e.yhbh
               | and c.yhbh = e.yhbh
               | and c.jldbh = e.jldbh
               | and e.jldytdm <> '410'
               | and c.sslxdm in ('121', '221') --正向有功总 反向有功总
               | and e.jldydjdm not in ('02', '03') --排除电压等级为 交流220v、交流为380v
               |  and exists (select 1
               |    from gdljfhzxx a, dcfzxx b
               |   where c.yhbh = a.dcbh
               |     and c.dfny = a.gdyf
               |     and b.dcbh = a.dcbh)
               |group by e.xlxdbs, c.jldbh, c.yhbh, c.dfny, c.dqbm, c.bqcbcs,c.sslxdm
               |union all
               |select sum(coalesce(c.bjdl, 0) + coalesce(c.jbdl, 0)) bjdl,
               |     c.jldbh,
               |     'ffg' fgbz,
               |     c.yhbh,
               |     e.xlxdbs,
               |     c.dfny ny,
               |     c.dqbm,
               |     c.bqcbcs,
               |     c.sslxdm
               |from cbxx c, jld e
               |where c.dfny = ${nowMonth}
               | and c.yhbh = e.yhbh
               | and c.yhbh = e.yhbh
               | and c.jldbh = e.jldbh
               | and e.jldytdm <> '410'
               | and c.sslxdm in ('123', '124', '125', '222', '223', '224') --正向和反向有功峰平谷
               | and e.jldydjdm not in ('02', '03') --排除电压等级为 交流220v、交流为380v
               |  and exists (select 1
               |    from gdljfhzxx a, dcfzxx b
               |   where c.yhbh = a.dcbh
               |     and c.dfny = a.gdyf
               |     and b.dcbh = a.dcbh)
               |group by e.xlxdbs, c.jldbh, c.yhbh, c.dfny, c.dqbm, c.bqcbcs,c.sslxdm
               |
             """.stripMargin

        sparkSession.sql(xldfdcswdlNew).createOrReplaceTempView("xldfdcswdlNew") // TODO:线路地方电厂上网电量

        /*-------------------------------------------------------------------*/


        //线路供电量明细
        // 1线路供电量线路总表部分
        // 2线路供电量线路总表退补电量部分
        // 3线路供电量地方电厂退补电量部分
        // 4线路供电量地方电厂上网电量本月一次部分
        // 5线路供电量光伏反向上网电量部分
        // 6线路供电量地方电厂上网电量上月二次部分
        // 7线路供电量地方电厂上网电量部分
        val xlgdlmx =
            s"""
               |select * from (
               |-- 1线路供电量线路总表部分
               |  select
               |    f.gddwbm,e.dfny, e.yhbh, e.yhmc, e.jldxh, e.xlxdbs,e.yddz,f.zcbh,e.cbqdbh,e.yhlbdm,
               |     (coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0) - coalesce(e.ygfbkjdl, 0)) ygzdl,e.jfrl,f.cbsxh,f.cbrbs,e.jldbh,f.scbss,
               |     f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'1' dllx
               |  from jldxx e, cbxx f
               |  where e.dfny = ${nowMonth}
               |     and e.yhlbdm = '80' and f.yhbh = e.yhbh
               |     and f.gzdbh = e.gzdbh and f.dfny = e.dfny and e.ywlbdm = f.ywlbdm and f.jldbh = e.jldbh and f.dqbm = e.dqbm and f.sslxdm = '121'
               |  union all
               |
               |-- 2线路供电量线路总表退补电量部分
               |  select
               |    e.gddwbm,e.dfny, e.yhbh, e.yhmc, e.jldxh, e.xlxdbs,e.yddz,f.zcbh,e.cbqdbh, e.yhlbdm,coalesce(e.ygtbdl, 0) ygzdl,e.jfrl,
               |    f.cbsxh,f.cbrbs,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'2' dllx
               |  from jldxx e, cbxx f
               |  where e.dfny = ${nowMonth}
               |      and e.yhlbdm = '80' and f.yhbh = e.yhbh
               |      and f.gzdbh = e.gzdbh and f.dfny = e.dfny and f.jldbh = e.jldbh and f.sslxdm = '121'
               |  union all
               |
               |-- 3线路供电量地方电厂退补电量部分
               |  select
               |    c.gddwbm,c.ccyf dfny,c.dcbh yhbh,c.dcmc yhmc,0 jldxh, e.xlxdbs, null yddz, null zcbh, null cbqdbh, '40' yhlbdm,
               |    coalesce(c.tbdl, 0) ygzdl,0 jfrl,null cbsxh,null cbrbs,d.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'3' dllx
               |  from gdjycctbxx c,dcjzxx d,jld e
               |  where c.ccyf = ' || v_dcqsny || '
               |     and c.jsdybh = d.jsdybh and d.jldbh = e.jldbh
               |     and c.tbzt = '1' and exists (select 1 from dcfzxx b where b.dcbh = c.dcbh
               |     and b.dydj not in ('02', '03'))
               |  union all
               |
               |-- 4线路供电量地方电厂上网电量本月一次部分
               |  select g.gddwbm, g.dfny, g.yhbh, g.yhmc,
               |      1 jldxh,c.xlxdbs,g.yddz,g.zcbh,g.cbqdbh,'40' yhlbdm,c.bjdl ygzdl,
               |      null jfrl,g.cbsxh,g.cbrbs,null jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'4' dllx
               |  from xldfdcswdlNew c,cbxx g
               |  where c.jldbh = g.jldbh and c.yhbh = g.yhbh
               |      and c.ny = g.dfny and c.sslxdm = g.sslxdm and c.bqcbcs = g.bqcbcs
               |      and c.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = c.yhbh) = '0' then 'BFFG' else 'FFG' end)
               |      and not exists (select 1 from dcfzxx d where c.yhbh = d.dcbh and d.gdldz = '2')
               |      and c.ny = ' || v_dcqsny || ' and c.bqcbcs = 1
               |      and exists(select 1 from _xldfdcswdlNew d where d.fgbz = c.fgbz and d.ny = ' || v_dcqssy || '
               |      and d.bqcbcs = 2 and d.xlxdbs = c.xlxdbs)
               |  union all
               |-- 5线路供电量光伏反向上网电量部分
               |  select
               |    f.gddwbm,e.dfny,e.yhbh,e.yhmc,e.jldxh,e.xlxdbs,e.yddz,f.zcbh,e.cbqdbh,e.yhlbdm,
               |      coalesce(f.bjdl, 0) ygzdl,e.jfrl,f.cbsxh,f.cbrbs,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'5' dllx
               |  from jldxx e, cbxx f
               |  where e.dfny = ${nowMonth}
               |      and e.yhlbdm = '60' and f.yhbh = e.yhbh and f.gzdbh = e.gzdbh and f.dfny = e.dfny
               |      and e.ywlbdm = f.ywlbdm and f.jldbh = e.jldbh and f.sslxdm = '221'
               |      and exists (select 1 from gdljfhzxx a ,dcfzxx b,yhdyxx d where d.yhbh = a.dcbh
               |      and d.xlxdbs = e.xlxdbs and a.gdyf = ${nowMonth} and b.dcbh = a.dcbh
               |      and b.dydj in('02','03'))
               |  union all
               |-- 6线路供电量地方电厂上网电量上月二次部分
               |  select
               |    g.gddwbm, g.dfny, g.yhbh, g.yhmc,
               |    1 jldxh,c.xlxdbs,g.yddz,g.zcbh,g.cbqdbh,'40' yhlbdm,c.bjdl ygzdl,
               |    null jfrl,g.cbsxh,g.cbrbs,null jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'6' dllx
               |  from xldfdcswdlNew c,cbxx g
               |  where c.jldbh = g.jldbh and c.yhbh = g.yhbh
               |      and c.ny = g.dfny and c.sslxdm = g.sslxdm and c.bqcbcs = g.bqcbcs
               |      and c.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = c.yhbh) = '0' then 'BFFG' else 'FFG' end)
               |      and not exists (select 1 from dcfzxx d where c.yhbh = d.dcbh and d.gdldz = '2')
               |      and  c.ny = ' || v_dcqsny || ' and c.bqcbcs = 2
               |  union all
               |-- 7线路供电量地方电厂上网电量部分
               |  select
               |    g.gddwbm, g.dfny, g.yhbh, g.yhmc,
               |    1 jldxh,c.xlxdbs,g.yddz,g.zcbh,g.cbqdbh,'40' yhlbdm,c.bjdl ygzdl,
               |    null jfrl,g.cbsxh,g.cbrbs,null jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'7' dllx
               |  from xldfdcswdlNew c,cbxx g
               |  where c.jldbh = g.jldbh and c.yhbh = g.yhbh
               |      and c.ny = g.dfny and c.sslxdm = g.sslxdm and c.bqcbcs = g.bqcbcs
               |      and c.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = c.yhbh) = '0' then 'BFFG' else 'FFG' end)
               |      and not exists (select 1 from dcfzxx d where c.yhbh = d.dcbh and d.gdldz = '2')
               |      and  c.ny = ' || v_dcqsny || '
               |      and not exists(select 1 from xldfdcswdlNew d where d.fgbz = c.fgbz and d.ny = ' || v_dcqsny || ' and d.bqcbcs = 2
               |      and d.xlxdbs = c.xlxdbs)
               |) a
               |order by a.dllx
               |
             """.stripMargin
        sparkSession.sql(xlgdlmx).createOrReplaceTempView("xlgdlmx") // TODO: 线路供电量明细

        /*-------------------------------------------------------------------*/
        //台区供电量明细
        // 1台区供电量台区考核表当月部分
        // 2单、双月台区供电量台区考核表上月部分
        // 3台区供电量光伏上网上月部分
        // 4单、双月台区供电量光伏上网上上月部分
        // 5台区供电量反向电量扣减部分
        // 6单、双月台区供电量反向电量扣减部分
        // 7台区供电量光伏上网退补上月部分
        // 8单、双月台区供电量光伏上网退补上上月部分
        val tqgdlmx =
            s"""
               |select *
               |from (
               |-- 1台区供电量台区考核表当月部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(e.ygzdl, 0) - coalesce(e.ygbsdl, 0)) ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,e.ygbsdl,null sjcbfsdm,'1' dllx
               |    from jldxx e
               |    where e.dfny = ${nowMonth} and e.yhlbdm = '60'
               |    union all
               |-- 2单、双月台区供电量台区考核表上月部分
               |    select
               |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
               |        null zcbh,cbqdbh,yhlbdm,(coalesce(e.ygzdl, 0) - coalesce(e.ygbsdl, 0)) ygzdl,jfrl,
               |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,e.ygbsdl,null sjcbfsdm,'2' dllx
               |    from jldxx e
               |    where e.dfny = ${lastMonth} and e.yhlbdm = '60'
               |    and exists (select 1 from all_tq a where e.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |-- 3台区供电量光伏上网上月部分
               |    select
               |        max(c.gddwbm),c.dfny,c.yhbh,max(c.yhmc),max(c.jldcbsxh),max(c.bqcbcs),d.xlxdbs,max(c.yddz),d.tqbs,max(c.zcbh),max(c.cbqdbh),max(c.yhlbdm),
               |        sum(coalesce(c.bjdl,0) + coalesce(c.jbdl,0)) ygzdl,null jfrl,
               |        max(c.cbsxh),max(c.cbrbs),max(c.cbsj),c.jldbh,max(c.scbss),max(c.bcbss),max(c.zhbl),null ygbsdl,max(c.sjcbfsdm),'3' dllx
               |    from cbxx c, yhdyxx d, jld e
               |    where c.dfny = ' || v_dcqsny ||' and c.yhbh = d.yhbh
               |    and d.xlxdbs = e.xlxdbs and c.yhbh = e.yhbh and c.jldbh = e.jldbh and c.yhlbdm = '40' and c.sslxdm = '221'
               |    and exists (select 1 from gdljfhzxx a, dcfzxx b
               |            where c.yhbh = a.dcbh and c.dfny = a.gdyf and b.dcbh = a.dcbh
               |                and b.dydj in ('02', '03')
               |                )
               |    group by d.xlxdbs, d.tqbs,c.jldbh,c.yhbh,c.dfny,c.dqbm
               |    union all
               |-- 4单、双月台区供电量光伏上网上上月部分
               |    select
               |        max(c.gddwbm),c.dfny,c.yhbh,max(c.yhmc),max(c.jldcbsxh),max(c.bqcbcs),d.xlxdbs,
               |        max(c.yddz),d.tqbs,max(c.zcbh),max(c.cbqdbh),max(c.yhlbdm),
               |        sum(coalesce(c.bjdl,0) + coalesce(c.jbdl,0)) ygzdl,null jfrl,
               |        max(c.cbsxh),max(c.cbrbs),max(c.cbsj),c.jldbh,max(c.scbss),max(c.bcbss),max(c.zhbl),null ygbsdl,max(c.sjcbfsdm),'4' dllx
               |    from cbxx c, yhdyxx d, jld e
               |    where c.dfny = ' || v_dcqssy ||' and c.yhbh = d.yhbh and d.xlxdbs = e.xlxdbs
               |        and c.yhbh = e.yhbh and c.jldbh = e.jldbh and c.yhlbdm = '40' and c.sslxdm = '221' and e.jldydjdm in ('02', '03')
               |        and exists (select 1 from gdljfhzxx a, dcfzxx b where c.yhbh = a.dcbh and c.dfny = a.gdyf and b.dcbh = a.dcbh)
               |        and exists (select 1 from all_tq a where d.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    group by d.xlxdbs, d.tqbs,c.jldbh,c.yhbh,c.dfny,c.dqbm
               |    union all
               |-- 5台区供电量反向电量扣减部分
               |    select
               |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
               |        f.zcbh,t.cbqdbh,t.yhlbdm,-(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,null cbrbs,f.cbsj,
               |        t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm ,'5' dllx
               |    from jldxx t, cbxx f
               |    where t.dfny =  ${nowMonth}
               |         and t.yhlbdm = '60'
               |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
               |         and f.sslxdm = '221'
               |    union all
               |    -- 6单、双月台区供电量反向电量扣减部分
               |    select
               |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
               |        f.zcbh,t.cbqdbh,t.yhlbdm,-(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,null cbrbs,f.cbsj,
               |        t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm,'6' dllx
               |    from jldxx t, cbxx f
               |    where t.dfny =  ${lastMonth}
               |         and t.yhlbdm = '60'
               |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
               |         and f.sslxdm = '221'
               |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |    union all
               |    -- 7台区供电量光伏上网退补上月部分
               |    select
               |        c.gddwbm,c.ccyf dfny,c.dcbh yhbh,c.dcmc yhmc,0 jldcbsxh,null bqcbcs, e.xlxdbs,null yddz,null tqbs,
               |        null zcbh,null cbqdbh, '40' yhlbdm,coalesce(c.tbdl, 0) ygzdl,0 jfrl,
               |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'7' dllx
               |    from gdjycctbxx c,dcjzxx d,jld e
               |    where c.ccyf = ' || v_dcqsny || '
               |         and c.jsdybh = d.jsdybh and d.jldbh = e.jldbh
               |         and c.tbzt = '1' and exists (select 1 from dcfzxx b where b.dcbh = c.dcbh
               |         and e.jldydjdm in ('02', '03'))
               |    union all
               |    -- 8单、双月台区供电量光伏上网退补上上月部分
               |    select
               |        c.gddwbm,c.ccyf dfny,c.dcbh yhbh,c.dcmc yhmc,0 jldcbsxh,null bqcbcs, e.xlxdbs,null yddz,null tqbs,
               |        null zcbh,null cbqdbh, '40' yhlbdm,coalesce(c.tbdl, 0) ygzdl,0 jfrl,
               |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'8' dllx
               |    from gdjycctbxx c,dcjzxx d,jld e
               |    where c.ccyf = ' || v_dcqssy || '
               |         and c.jsdybh = d.jsdybh and d.jldbh = e.jldbh
               |         and c.tbzt = '1' and exists (select 1 from dcfzxx b where b.dcbh = c.dcbh
               |         and e.jldydjdm in ('02', '03'))
               |         and exists (select 1 from all_tq a where e.tqbs = a.tqbs and a.cbzq in ('2','3'))
               |
               |    ) a
               |order by a.dllx
               |
             """.stripMargin
        sparkSession.sql(tqgdlmx).createOrReplaceTempView("tqgdlmx") // TODO: 台区供电量明细
        /*-------------------------------------------------------------------*/






    }



}
