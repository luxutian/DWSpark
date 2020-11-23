package com.GDNanWangLineLoss.month.service

import java.sql.Date

import com.GDNanWangLineLoss.month.bean.Constant
import com.GDNanWangLineLoss.month.dao.DataSourceDao
import com.GDNanWangLineLoss.month.bean.Variables._
import com.GDNanWangLineLoss.month.util.{Functions, UDFfunction}
import org.apache.spark.sql.{SaveMode, SparkSession}


object Line1Service {


    /**
     * 线损率异常，进一步分析异常原因
     *
     * @param sparkSession
     * @param url
     */
    def line1Service(sparkSession: SparkSession, url: String) = {

        try {
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

        } catch {
            case e: Exception => e.printStackTrace()
        }


        /*---------------------------------------------------------------------------------------------------*/


        /*---------------------------------------------------------------------------------------------------*/
        import sparkSession.implicits._

        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1
        start = System.currentTimeMillis() //todo 用于记录程序运行时间

        //todo 1 线路1.1 起止表码数据采集完整性检查
        try {
            //_xlcjwzl 线路采集完整率 取营销
//            sparkSession.sql(
//                s"""
//                   |select x.xlxdbs,
//                   |    round(count(distinct case when (c.scbss is not null and c.bcbss is not null and c.scbss != 0 and c.bcbss != 0) then c.jldbh else null end)/count(distinct c.jldbh)*100,3) cjwzl --采集完整率
//                   |from cbxx c
//                   |join jld j on j.jldbh=c.jldbh
//                   |join all_xlxd x on x.xlxdbs = j.xlxdbs
//                   |where c.dfny= ${nowMonth}
//                   |    and ((c.yhlbdm in ('80','60') and c.sslxdm in ('121','221')) or (c.yhlbdm in ('10','11') and c.sslxdm = '121')
//                   |	or (c.yhlbdm = '40' and c.sslxdm in ('221','222','223','224')))
//                   |group by x.xlxdbs
//                   |
//                 """.stripMargin).createOrReplaceTempView("xlcjwzl") //计算线路采集完整率
//            //            sparkSession.sql("select * from xlcjwzl").show(5)
//
//            try {
//                //                sparkSession.sql("select * from xlxd").show(5)
//            } catch {
//                case e: Exception => e.printStackTrace()
//            }finally {
//                println(" 1.1 起止表码数据采集完整性检查 ")
//            }
//
//
//            val xl_11 =
//                s"""
//                   |select getUUID() id,${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
//                   |null tqbs,null tqbh,null tqmc,
//                   |getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |k1.bdzkhbhh,null tqkhbhh,
//                   |y.yhbh,y.yhmc,y.yhlbdm yhlbdm,j.jldbh,c.zcbh,
//                   |handleNumber(c.zhbl) zhbl,--综合倍率（string）
//                   |getycgzbh(${Constant.XL_11}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
//                   |getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nowMonth} nybm
//                   |from cbxx c
//                   |join ydkh y on c.yhbh=y.yhbh --用电客户的唯一编号
//                   |join jld j on j.jldbh=c.jldbh and j.jldytdm <> '410' --'计量点编号' (410应该是不存在)
//                   |join xlxd l on l.xlxdbs=j.xlxdbs --线路的系统内部唯一标识'
//                   |join xlcjwzl w on w.xlxdbs = l.xlxdbs and w.cjwzl < 100  --线路线段标识
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where c.dfny= ${nowMonth}
//                   |and (c.scbss is null or c.bcbss is null or c.scbss = 0 or c.bcbss = 0) --上次（本次表示数）
//                   |and ((c.yhlbdm in ('80','60') and c.sslxdm in ('121','221')) or (c.yhlbdm in ('10','11') and c.sslxdm = '121')
//                   | or (c.yhlbdm = '40' and c.sslxdm in ('221','222','223','224')))
//                   |and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
//                   |and y.yhztdm <> '2' --客户状态代码
//                   |""".stripMargin
//            try {
//
//                sparkSession.sql(xl_11).createOrReplaceTempView("res_gk_wcjyhqd")
//                sparkSession.sql("select * from res_gk_wcjyhqd where isFiveDsj(gddwbm) = 1") // 2020/10/28（ gddwbm 供电单位编码） （isFiveDsj 获取供电单位编码前4个编码）
//                  .repartition(resultPartition).createOrReplaceTempView("res_gk_wcjyhqd")
//
////                sparkSession.sql(s"select * from res_gk_wcjyhqd")//.show(5) //tjsj 年月
////                  .write.options(Map("kudu.master" -> url, "kudu.table" -> s"${writeSchema}.gk_yxlwcjyhqd")) //todo 1 月线路未采集用户清单(表)
////                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save
//
//            } catch {
//                case e: Exception => e.printStackTrace()
//            }finally {
//                println(" 1 月线路未采集用户清单(表) ")
//            }




            //sparkSession.sql(s"insert into ${writeSchema}.gk_yxlwcjyhqd_his select getUUID(),*,tjsj fqrq from RES_GK_WCJYHQD")  //todo 历史表目前没有看到引用

            //            sparkSession.sql(
            //                s"""
            //                   |select
            //                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
            //                   |    gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
            //                   |from res_gk_wcjyhqd
            //                   |
            //                 """.stripMargin).repartition(resultPartition)
            //              .createOrReplaceTempView("res_gk_wcjyhqd_ycgddwxlgx")

            // TODO:       ycgddwxlgx 这是什么表
            //            sparkSession.sql(
            //                s"""
            //                   |select
            //                   |    getUUID(),*,tjsj fqrq
            //                   |from res_gk_wcjyhqd_ycgddwxlgx
            //                 """.stripMargin).write.options(Map("kudu.master" -> url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx")) //ycgddwxlgx --暂时找不到这个表
            //                .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            reason = ""
            isSuccess = 1
        } catch {
            case e: Exception => {
                isSuccess = 0
                val message = e.getMessage
                if (message.length > 800) reason = message.substring(0, 800) else reason = message.substring(0, message.length)
            }
        }

        end = System.currentTimeMillis()
//        sparkSession.sql(
//            s"""
//               |select getFormatDate(${end}) recordtime,'xl11',${isSuccess} state,'${reason}' reason,${(end - start) / 1000} runtime
//               |
//             """.stripMargin).createOrReplaceTempView("ruleState")
        println(s"规则1.1运行${(end - start) / 1000}秒")

        /*------------------------------------------------------------------------------------------------*/





        //todo 2 线路1.2 起止表码时标平移检查
        //2019年11月12日11:36:25 lixc 添加不存在换表信息记录中    11.18已修改
        start = System.currentTimeMillis()
        try {
//            val xl_12 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,l.gddwbm,
//                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
//                   |    null tqbs,null tqbh,null tqmc,
//                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,
//                   |    getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,null tqkhbhh,
//                   |    c.yhbh,c.yhmc,y.yhlbdm,c.jldbh,
//                   |    c.zcbh,
//                   |    handleNumber(c.scbss) qm,
//                   |    handleTime(c.sccbrq) qmcjsj,--// 2020/10/28 建表语句类型是 timestamp
//                   |    handleNumber(c.bcbss) zm,
//                   |	handleTime(c.cbsj) zmcjsj,
//                   |    handleNumber(c.zhbl) zhbl,
//                   |    handleNumber(c.bjdl) dl,
//                   |    null fzqjfdl,
//                   |    getycgzbh(${Constant.XL_12}) ycgzbh,
//                   |    getDsjbm(l.gddwbm) dsjbm,
//                   |	getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
//                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
//                   |	getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nowMonth} nybm
//                   |from cbxx c
//                   |join ydkh y on y.yhbh = c.yhbh
//                   |join jld j on j.jldbh = c.jldbh and j.jldytdm <> '410'
//                   |join xlxd l on l.xlxdbs = j.xlxdbs
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where c.dfny = ${nowMonth}
//                   |    and c.scbss is not null and c.bcbss is not null
//                   |and (c.cbsj <> '${_addOneMonth}-01 00:00:00' or c.sccbrq <> '${_nowMonth}-01 00:00:00')
//                   |and c.bqcbcs = 1
//                   |and getYearMonth(y.lhrq) != ${nowMonth}
//                   |and ((c.yhlbdm in ('80','60') and c.sslxdm in ('121','221')) or (c.yhlbdm in ('10','11') and c.sslxdm = '121')
//                   | or (c.yhlbdm = '40' and c.sslxdm in ('221','222','223','224')))
//                   |and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
//                   |and y.yhztdm <> '2' and not exists (select 1 from hbxxjl t where t.yxdnbbs = c.yxdnbbs and t.dfny = c.dfny)
//                   |
//                 """.stripMargin
//
//            try {
//                sparkSession.sql(xl_12).createOrReplaceTempView("res_gk_bmpyyhqd")
//                sparkSession.sql("select * from res_gk_bmpyyhqd where isFiveDsj(gddwbm) = 1")
//                  .repartition(resultPartition).createOrReplaceTempView("res_gk_bmpyyhqd")
//
//                //todo 2 表码平移用户清单
////                sparkSession.sql(s"select getUUID() id,* from res_gk_bmpyyhqd")//.show(5)
////                                  .write.options(Map("kudu.master" -> url,"kudu.table" -> s"${writeSchema}.gk_bmpyyhqd"))
////                                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save
//            } catch {
//                case e: Exception => e.printStackTrace()
//            }finally {
//                println(" 2 表码平移用户清单 ")
//            }



//            sparkSession.sql(s"insert into ${writeSchema}.GK_BMPYYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BMPYYHQD")

//            sparkSession.sql(
//                s"""
//                   |select
//                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,
//                   |    xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
//                   |from RES_GK_BMPYYHQD
//                   |
//                 """.stripMargin)
//              .repartition(resultPartition)
//              .createOrReplaceTempView("res_gk_bmpyyhqd_ycgddwxlgx") // TODO: 这是什么表
//
//            sparkSession.sql(
//                s"""
//                   |insert into ${writeSchema}.ycgddwxlgx
//                   |select
//                   |    getUUID(),*,tjsj fqrq
//                   |from RES_GK_BMPYYHQD_YCGDDWXLGX
//                 """.stripMargin).write.options(Map("kudu.master" -> url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
//                .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl12',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.2运行${(end-start)/1000}秒")


        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //todo 3 线路1.3 手工抄表方式检查   比原先加多了示数类型代码，示数类型
        try{
//            val xl_13 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |    l.xlxdbs,l.xlbh,l.xlmc,
//                   |    null tqbs,null tqbh,null tqmc,
//                   |    getbdzbs(l.xlxdbs) bdzbs,
//                   |    getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,null tqkhbhh,
//                   |    c.yhbh,c.yhmc,y.yhlbdm,c.jldcbfsdm cbfsdm,c.jldbh,
//                   |    c.zcbh,
//                   |    handleNumber(c.scbss) qm,handleTime(c.sccbrq) qmcjsj,
//                   |    handleNumber(c.bcbss) zm,handleTime(c.cbsj) zmcjsj,
//                   |    handleNumber(c.zhbl) zhbl,handleNumber(c.bjdl) dl,
//                   |    getycgzbh(${Constant.XL_13}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,--// 2020/10/28  getycgzbh(${Constant.XL_13}) ycgzbh
//                   |    getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
//                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
//                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
//                   |    getdmbmmc('CBFSDM',c.jldcbfsdm) cbfs,getdqbm(l.gddwbm) dqbm,${nybm} nybm,
//                   |    c.sslxdm,getdmbmmc('SSLXDM',c.sslxdm) sslx
//                   |from cbxx c
//                   |join ydkh y on y.yhbh = c.yhbh
//                   |join jld j on j.jldbh = c.jldbh and j.jldytdm <> '410' --410代表什么意思
//                   |join xlxd l on l.xlxdbs = j.xlxdbs
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where  c.dfny = ${nowMonth}
//                   |    and c.jldcbfsdm not like '4%'
//                   |    and ((c.yhlbdm in ('80','60') and c.sslxdm in ('121','221')) or (c.yhlbdm in ('10','11') and c.sslxdm = '121')
//                   |    or (c.yhlbdm = '40' and c.sslxdm in ('221','222','223','224')))
//                   |    and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
//                   |    and y.yhztdm <> '2'
//                   |
//                 """.stripMargin
//            sparkSession.sql(xl_13).createOrReplaceTempView("res_gk_sgcbyhqd")
//            sparkSession.sql("select * from res_gk_sgcbyhqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_sgcbyhqd")
//
//            //todo 3 手工抄表用户清单
//            try {
////                sparkSession.sql(
////                    s"""
////                       |select
////                       |    getUUID() id,*
////                       |from res_gk_sgcbyhqd
////                 """.stripMargin).write.options(Map("kudu.master"-> url,"kudu.table"->s"${writeSchema}.gk_sgcbyhqd"))
////                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            }catch {
//                case e:Exception => e.printStackTrace()
//            }finally {
//                println(" 3 手工抄表用户清单 ")
//            }


//            sparkSession.sql(s"insert into ${writeSchema}.gk_sgcbyhqd_his select getUUID(),*,tjsj fqrq from RES_GK_SGCBYHQD")

//            sparkSession.sql(
//                s"""
//                   |select
//                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
//                   |    gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
//                   |from res_gk_sgcbyhqd
//                   |
//                 """.stripMargin)
//              .repartition(resultPartition)
//              .createOrReplaceTempView("res_gk_sgcbyhqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from RES_GK_SGCBYHQD_YCGDDWXLGX")
//                .write.options(Map("kudu.master" -> url,"kudu.table"->s"${writeSchema}.ycgddwxlgx"))
//                .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl13',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.3运行${(end-start)/1000}秒")


        /*-------------------------------------------------------------------*/
        //todo 4 线路1.4 营销与计量表码不一致
        start = System.currentTimeMillis()
        try{
//            val xl_14 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |    l.xlxdbs,l.xlbh,l.xlmc,
//                   |    null tqbs,null tqbh,null tqmc,
//                   |    getbdzbs(j.xlxdbs) bdzbs,
//                   |    getbdzbh(j.xlxdbs) bdzbh,getbdzmc(j.xlxdbs) bdzmc,
//                   |    k1.bdzkhbhh,
//                   |    null tqkhbhh,
//                   |    y.yhbh,y.yhmc,y.yhlbdm,c.jldbh,c.zcbh,
//                   |    c.scbss yxxtqm,c.sccbrq yxxtqmcjsj,c.bcbss yxxtzm,
//                   |    c.cbsj yxxtzmcjsj,c.zhbl yxxtbl,c.bjdl yxxtdl,b.zxygz_sc jlxtqm,
//                   |    b.sjsj_sc jlxtqmcjsj,b.zxygz jlxtzm,b.sjsj jlxtzmcjsj,
//                   |    b.zhbl jlxtbl,coalesce((case when sslxdm = '121' then b.zxygz - b.zxygz_sc when sslxdm = '221' then b.fxygz - b.fxygz_sc end) * b.zhbl,0) jlxtdl,
//                   |    getycgzbh(${Constant.XL_14}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,
//                   |    getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
//                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
//                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
//                   |    getdqbm(l.gddwbm) dqbm,${nybm} nybm
//                   |from cbxx c
//                   |join jld j on c.jldbh = j.jldbh
//                   |join ydkh y on j.yhbh = y.yhbh and y.yhlbdm in ('80','60','10','11') and y.yhztdm <> 2
//                   |join xlxd l on l.xlxdbs = j.xlxdbs
//                   |join jlbmhb b on b.yhbh = c.yhbh and b.jldbh = c.jldbh and b.bjzcbh = c.zcbh  --// 2020/10/29 jlbmhb 计量编码合并(暂时没有)
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where c.dfny = ${nowMonth} and ((c.sslxdm = '121' and c.yhlbdm in ('80','60','10','11')
//                   |    and (c.scbss <> b.zxygz_sc or c.bcbss <> b.zxygz))
//                   |    or (c.sslxdm = '221' and c.yhlbdm in ('80','60')
//                   |    and (c.scbss <> b.fxygz_sc or c.bcbss <> b.fxygz)))
//                   |
//                 """.stripMargin
//
//            sparkSession.sql(xl_14).repartition(resultPartition).createOrReplaceTempView("res_gk_yxyjlbmbyzqd")
//            sparkSession.sql("select * from res_gk_yxyjlbmbyzqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxyjlbmbyzqd")

            //todo 4 营销与计量表码不一致清单 // 2020/10/29 计量相关数据没有
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxyjlbmbyzqd")
//                .write.options(Map("kudu.master" -> url,"kudu.table"-> s"${writeSchema}.gk_yxyjlbmbyzqd"))
//                .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

//            sparkSession.sql(s"insert into ${writeSchema}.gk_yxyjlbmbyzqd_his select getUUID(),*,tjsj fqrq from RES_GK_YXYJLBMBYZQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_YXYJLBMBYZQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_YXYJLBMBYZQD_YCGDDWXLGX")
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxyjlbmbyzqd_ycgddwxlgx")
//                .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx"))
//                .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl14',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.4运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/



        start = System.currentTimeMillis()
        //todo 5 线路1.5 电量异常波动用户清单
        try{
//            sparkSession.sql(
//                s"""
//                   |select
//                   |    xlxdbs,
//                   |    regexp_replace(concat_ws(',',sort_array(collect_list(concat_ws(':',cast(a.rownum as string),cast(nvl(bygdl-bysdl,0) as string))))),'\\\\d+\\\\:','') shdl
//                   |from (
//                   |    select *,
//                   |        row_number() over(partition by xlxdbs order by xlxdbs,ny) rownum
//                   |    from xlxstjxx --线路线损统计信息
//                   |    where ny in (${nowMonth},${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5})
//                   |    ) a
//                   |group by xlxdbs
//                   |
//                 """.stripMargin).createOrReplaceTempView("xs")
//            sparkSession.sql("select * from xs").show(5)
//            println("线路1.5  xs")
//
//            sparkSession.sql(
//                s"""
//                   |select
//                   |    b.yhbh,b.jldbh,b.jfdl,j.tqbs,j.xlxdbs
//                   |from(
//                   |    select
//                   |        yhbh,jldbh,
//                   |        regexp_replace(concat_ws(',',sort_array(collect_list(concat_ws(':',cast(a.rownum as string),cast(if(jfdl=null or jfdl='',0.00,jfdl) as string))))),'\\\\d+\\\\:','') jfdl
//                   |    from (
//                   |        select *,
//                   |            row_number() over(partition by yhbh,jldbh order by yhbh,jldbh,dfny) rownum
//                   |        from jldxx_all --//计量点信息
//                   |        where dfny in (${nowMonth},${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5})
//                   |            and yhlbdm in ('60','10','11') and yhztdm <> 2
//                   |           ) a
//                   |        group by yhbh,jldbh
//                   |     ) b
//                   |join jldxx j on j.yhbh = b.yhbh and j.jldbh = b.jldbh and j.dfny = ${nowMonth}
//                   |
//                 """.stripMargin).createOrReplaceTempView("yh")
//            sparkSession.sql("select * from yh").show(5)
//            println("线路1.5  yh") //todo 2020/10/30 这个有问题，：Caused by: org.apache.kudu.client.NonRecoverableException: Tablet is lagging too much to be able to serve snapshot scan.
//
//            sparkSession.sql(
//                s"""
//                   |select *
//                   |from (
//                   |    select
//                   |        y.yhbh,y.jldbh,getCorr(y.jfdl,x.shdl) xgxs
//                   |    from xs x
//                   |    join yh y on x.xlxdbs = y.xlxdbs
//                   |    ) a
//                   |where xgxs < -0.7
//                   |
//                 """.stripMargin).createOrReplaceTempView("xgxs")
//            sparkSession.sql("select * from xgxs").show(5)
//            println("线路1.5  xgxs")
//
//            val xl_15 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |    j1.xlxdbs,l.xlbh,l.xlmc,
//                   |    null tqbs,null tqbh,null tqmc,
//                   |    getbdzbs(j1.xlxdbs) bdzbs,
//                   |    getbdzbh(j1.xlxdbs) bdzbh,getbdzmc(j1.xlxdbs) bdzmc,k1.bdzkhbhh,
//                   |    null tqkhbhh,
//                   |    j1.yhbh,j1.yhmc,j1.yhlbdm,j1.jldbh,c1.zcbh,
//                   |    handleNumber(c1.scbss) bydlqm,
//                   |    handleNumber(c1.bcbss) bydlzm,
//                   |    handleNumber(c1.zhbl) bydlbl,
//                   |    handleNumber(j1.jfdl) bydldl,
//                   |    handleNumber(c2.scbss) sydlqm,
//                   |    handleNumber(c2.bcbss) sydlzm,
//                   |    handleNumber(c2.zhbl) sydlbl,
//                   |    handleNumber(j2.jfdl) sydldl,
//                   |    x.xgxs,
//                   |    getycgzbh(${Constant.XL_15}) ycgzbh,
//                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
//                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |    getdmbmmc('YHLBDM',j1.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
//                   |from xgxs x
//                   |join jldxx j1 on x.yhbh = j1.yhbh and x.jldbh = j1.jldbh and j1.dfny = ${nowMonth}
//                   |join jldxx j2 on x.yhbh = j2.yhbh and x.jldbh = j2.jldbh and j2.dfny = ${lastMonth}
//                   |join xlxd l on l.xlxdbs = j1.xlxdbs ----//异常线路线段
//                   |join cbxx c1 on c1.yhbh = x.yhbh and c1.jldbh = x.jldbh and c1.dfny = ${nowMonth} and c1.sslxdm = '121'
//                   |join cbxx c2 on c2.yhbh = x.yhbh and c2.jldbh = x.jldbh and c2.dfny = ${lastMonth} and c2.sslxdm = '121'
//                   |lateral view outer explode(split(getbdzkhb(j1.xlxdbs),',')) k1 as bdzkhbhh
//                   |
//                 """.stripMargin
//            sparkSession.sql(xl_15).createOrReplaceTempView("res_gk_dlycbdyhqd")
//            sparkSession.sql("select * from res_gk_dlycbdyhqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition)
//              .createOrReplaceTempView("res_gk_dlycbdyhqd")
//
//            //todo 1.5 电量异常波动用户清单
//            try {
////                sparkSession.sql(s"select getUUID() id,* from res_gk_dlycbdyhqd")
////                  .write.options(Map("kudu.master"->url,"kudu.table"->s"${writeSchema}.gk_dlycbdyhqd"))
////                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            }catch {
//                case e:Exception => e.printStackTrace()
//            }finally {
//                println(" 1.5 电量异常波动用户清单  ")
//            }


//            sparkSession.sql(s"insert into ${writeSchema}.gk_dlycbdyhqd_his select getUUID(),*,tjsj fqrq from res_gk_dlycbdyhqd")

//            sparkSession.sql(
//                s"""
//                   |select
//                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
//                   |    gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
//                   |from res_gk_dlycbdyhqd
//                   |
//                 """.stripMargin)
//              .repartition(resultPartition)
//              .createOrReplaceTempView("res_gk_dlycbdyhqd_ycgddwxlgx")
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dlycbdyhqd_YCGDDWXLGX")
//                .write.options(Map("kudu.master"->url,"kusu.table"->s"${writeSchema}.ycgddwxlgx"))
//                .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()

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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl15',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.5运行${(end-start)/1000}秒")



        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //todo 6 线路1.6 电量占比异常历史电量清单
        try{
            val sumJfdl =
                s"""
                   |select
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.xlxdbs,j.jldbh,sum(j.jfdl) yhjfdl
                   |from jldxx j
                   |where j.dfny = ${nowMonth} and j.yhlbdm in ('80','60','10','11')
                   |group by j.yhbh,j.yhmc,j.yhlbdm,j.xlxdbs,j.jldbh
                 """.stripMargin
            sparkSession.sql(sumJfdl).createOrReplaceTempView("sumJfdl")
            sparkSession.sql("select * from sumJfdl").show(5)
            println("1.6 sumJfdl")

            val avlJfdl =
                s"""
                   |select
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.xlxdbs,j.jldbh,sum(j.jfdl)/5 yhpjjfdl
                   |from jldxx_all j
                   |where j.dfny in (${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5})
                   |    and j.yhlbdm in ('80','60','10','11')
                   |group by j.yhbh,j.yhmc,j.yhlbdm,j.xlxdbs,j.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(avlJfdl).createOrReplaceTempView("avlJfdl")
            sparkSession.sql("select * from avlJfdl").show(5)
            println("1.6 avlJfdl")



            val lastFiveJfdl =
                s"""
                   |select
                   |    j.yhbh,j.jldbh,
                   |    sum(case when j.dfny = ${lastMonth} then j.jfdl end) dl1,
                   |    sum(case when j.dfny = ${lastMonth2} then j.jfdl end) dl2,
                   |    sum(case when j.dfny = ${lastMonth3} then j.jfdl end) dl3,
                   |    sum(case when j.dfny = ${lastMonth4} then j.jfdl end) dl4,
                   |    sum(case when j.dfny = ${lastMonth5} then j.jfdl end) dl5
                   |from jldxx_all j
                   |where j.dfny in (${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5}) and j.yhlbdm in ('80','60','10','11')
                   |group by j.yhbh,j.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(lastFiveJfdl).createOrReplaceTempView("lastFiveJfdl")
            sparkSession.sql("select * from lastFiveJfdl").show(5)
            println("1.6 lastFiveJfdl")


            val tqbdl =
                s"""
                   |select a.yhbh,a.yhmc,a.yhlbdm,a.xlxdbs,a.jldbh,(a.yhjfdl-b.yhtqjfdl)/b.yhtqjfdl tbbdl
                   |from sumJfdl a
                   |join (select j.yhbh,j.yhmc,j.yhlbdm,j.xlxdbs,j.jldbh,sum(j.jfdl) yhtqjfdl
                   |      from jldxx_all j
                   |      where j.dfny = ${tqny} and j.yhlbdm in ('80','60','10','11')
                   |      group by j.yhbh,j.yhmc,j.yhlbdm,j.xlxdbs,j.jldbh) b
                   |on b.yhbh = a.yhbh and b.jldbh = a.jldbh
                   |
                 """.stripMargin
            sparkSession.sql(tqbdl).createOrReplaceTempView("tqbdl")
            sparkSession.sql("select * from tqbdl").show(5)
            println("1.6 tqbdl")



            val xl_16 =
                s"""
                   |select distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
                   |      l.xlxdbs,l.xlbh,l.xlmc,
                   |      null tqbs,null tqbh,null tqmc,
                   |      getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |      k1.bdzkhbhh,null tqkhbhh,
                   |      a.yhbh,a.yhmc,a.yhlbdm,cb.jldbh,cb.zcbh,
                   |      handleNumber(b.dl5) dlw,
                   |      handleNumber(b.dl4) dlsi,
                   |      handleNumber(b.dl3) dls,
                   |      handleNumber(b.dl2) dle,
                   |      handleNumber(b.dl1) dly,
                   |      handleNumber(a.yhjfdl) dl,
                   |      handleNumber(c.yhpjjfdl) ypjdl,
                   |      handleNumber(round((a.yhjfdl-c.yhpjjfdl)/c.yhpjjfdl*100,6)) hbbdl,
                   |      handleNumber(round(d.tbbdl*100,6)) tbbdl,
                   |      handleNumber(round(a.yhjfdl/l.bygdl*100,6)) dlzxlzgdlbl,
                   |      getycgzbh(${Constant.XL_16}) ycgzbh,
                   |      getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |      getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |      getdmbmmc('YHLBDM',a.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from xlxstjxx l
                   |join sumJfdl a on a.xlxdbs = l.xlxdbs
                   |join lastFiveJfdl b on a.yhbh = b.yhbh and a.jldbh = b.jldbh
                   |join avlJfdl c on a.yhbh = c.yhbh and a.jldbh = c.jldbh
                   |join tqbdl d on a.yhbh = d.yhbh and a.jldbh = d.jldbh
                   |join cbxx cb on a.yhbh = cb.yhbh and a.jldbh = cb.jldbh and cb.sslxdm = '121'
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where l.ny = ${nowMonth} and l.bygdl <> 0 and (a.yhjfdl/l.bygdl > 0.05 or a.yhjfdl > (case a.yhlbdm when '60' then 9000 when '80' then 30000 when '11' then 6000 when '10' then 6000 end) or
                   |(a.yhjfdl-c.yhpjjfdl)/c.yhpjjfdl > 0.3 or d.tbbdl > 0.3)
                   |
                 """.stripMargin

            sparkSession.sql(xl_16).createOrReplaceTempView("res_gk_dlzbyclsdlqd")
            sparkSession.sql("select * from res_gk_dlzbyclsdlqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition)
              .createOrReplaceTempView("res_gk_dlzbyclsdlqd")
            sparkSession.sql("select * from res_gk_dlzbyclsdlqd").show(5)
            println("1.6 res_gk_dlzbyclsdlqd")


            //todo 1.6 电量占比异常历史电量清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_dlzbyclsdlqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dlzbyclsdlqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println(" 1.6 电量占比异常历史电量清单")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.gk_dlzbyclsdlqd_his select getUUID(),*,tjsj fqrq from RES_GK_DLZBYCLSDLQD")

//            sparkSession.sql(
//                s"""
//                   |select
//                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
//                   |    gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
//                   |from res_gk_dlzbyclsdlqd
//                   |
//                 """.stripMargin).repartition(resultPartition).createOrReplaceTempView("res_gk_dlzbyclsdlqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dlzbyclsdlqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl16',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.6运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //2019年12月16日 join _tbdlxx改为left join _tbdlxx
        //todo 7 线路1.7 电量退补用户信息
        try{
//            val xl_17 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |      l.xlxdbs,l.xlbh,l.xlmc,
//                   |      null tqbs,null tqbh,null tqmc,
//                   |      getbdzbs(l.xlxdbs) bdzbs,
//                   |      getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |      k1.bdzkhbhh,null,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,c.zcbh,
//                   |      handleNumber(c.scbss) qm,
//                   |      handleNumber(c.bcbss) zm,
//                   |      handleNumber(c.zhbl) zhbl,
//                   |      handleNumber(c.bjdl) dl,
//                   |      d.gzdbh tbgdh,
//                   |      handleNumber(d.ygdl) tbdl,
//                   |      getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |      getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
//                   |      getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |      getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |      getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
//                   |from jldxx j
//                   |join cbxx c on c.jldbh = j.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '121'
//                   |join xlxd l on j.xlxdbs = l.xlxdbs
//                   |left join tbdlxx d on d.jldbh = j.jldbh and d.ccyf = ${nowMonth}
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where j.jldytdm <> '410' and j.dfny = ${nowMonth}
//                   |and (j.bqcbcs >= 2 or j.tbgzdbh is not null)
//                   |and j.yhztdm <> '2'
//                   |
//                   |union
//                   |select distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |      l.xlxdbs,l.xlbh,l.xlmc,
//                   |      null tqbs,null tqbh,null tqmc,
//                   |      getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |      k1.bdzkhbhh,null,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,c.zcbh,
//                   |      handleNumber(c.scbss) qm,
//                   |      handleNumber(c.bcbss) zm,
//                   |      handleNumber(c.zhbl) zhbl,
//                   |      handleNumber(c.bjdl) dl,
//                   |      d.gzdbh tbgdh,
//                   |      handleNumber(d.tbdl) tbdl,
//                   |      getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |      getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |      getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,
//                   |      getdqbm(l.gddwbm) dqbm,${nybm} nybm
//                   |from jldxx j
//                   |join cbxx c on c.jldbh = j.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '121'
//                   |join xlxd l on j.xlxdbs = l.xlxdbs
//                   |left join tbmx d on d.jldbh = j.jldbh and d.dfny = ${nowMonth}
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where j.jldytdm <> '410' and j.dfny = ${nowMonth} and d.tblx = '1'
//                   |and (j.bqcbcs >= 2 or j.tbgzdbh is not null)
//                   |and j.yhztdm <> '2'
//                   |
//                 """.stripMargin
//            sparkSession.sql(xl_17).createOrReplaceTempView("res_gk_dltbyhqd")
//            sparkSession.sql("select * from res_gk_dltbyhqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition)
//              .createOrReplaceTempView("res_gk_dltbyhqd")
//
//            //todo 1.7 电量退补用户清单
//            try {
//                sparkSession.sql(s"select getUUID() id,* from res_gk_dltbyhqd")
//                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dltbyhqd"))
//                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            }catch {
//                case e:Exception => e.printStackTrace()
//            }finally {
//                println(" 1.7 电量退补用户清单")
//            }


//            sparkSession.sql(s"insert into ${writeSchema}.gk_dltbyhqd_his select getUUID(),*,tjsj fqrq from res_gk_dltbyhqd")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_DLTBYHQD")
//              .repartition(resultPartition)
//              .createOrReplaceTempView("RES_GK_DLTBYHQD_YCGDDWXLGX")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dltbyhqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl17',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.7运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //todo 8 线路1.8 未统计地方电厂电量     fxdl未确定
        //2019年11月20日14:07:13 lixc 1判断结算单元购售关系是2，峰谷标识是0的电厂户有功总的电量为0；2判断结算单元购售关系是2，峰谷标识是0的电厂户有功总的数据不存在
        //3判断结算单元购售关系不是2，峰谷标识不是0的电厂户有功峰平谷的电量为0；2判断结算单元购售关系不是2，峰谷标识不是0的电厂户有功峰平谷的数据不存在
        //2020-01-13  (select 1 from _gdljfhzxx a,_dcfzxx b where b.dcbh = a.dcbh and a.dcbh = y.yhbh and a.gdyf = ${nowMonth} and b.dydj = '08') 去掉_gdljfhzxx
        try{
//            val xl_18 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |   l.xlxdbs,l.xlbh,l.xlmc,
//                   |   null tqbs,null tqbh,null tqmc,getbdzbs(l.xlxdbs) bdzbs,
//                   |   getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |   k1.bdzkhbhh,null tqkhbhh,
//                   |   y.yhbh,y.yhmc,y.yhlbdm,d.jldbh,b.zcbh,0 fxdl,
//                   |   getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |   getDsjbm(l.gddwbm) dsjbm,
//                   |   getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |   getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,
//                   |   ${nybm} nybm,dc.dydj dydjbm,getdmbmmc('DYDJDM',dc.dydj) dydj
//                   |from ydkh y
//                   |join jld d on d.yhbh = y.yhbh
//                   |join jlddnbgx g on g.jldbh = d.jldbh
//                   |join yxdnb b on b.yxdnbbs = g.yxdnbbs
//                   |join xlxd l on l.xlxdbs = d.xlxdbs
//                   |join cbxx c on c.yhbh = y.yhbh and c.jldbh = d.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '221' and c.bjdl = 0 and c.jbdl = 0
//                   |join dcfzxx dc on dc.dcbh = y.yhbh and dc.dydj = '08'
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where y.yhlbdm = '40' and y.yhztdm <> '2'
//                   |    and exists (select 1 from (select e.dcbh,max(e.fgbs) fgbs from jsdy e where e.gsgx = '2' group by e.dcbh) js where js.dcbh = y.yhbh and js.fgbs = '0')
//                   |
//                   |union all
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |   l.xlxdbs,l.xlbh,l.xlmc,
//                   |   null tqbs,null tqbh,null tqmc,
//                   |   getbdzbs(l.xlxdbs) bdzbs,
//                   |   getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |   k1.bdzkhbhh,null tqkhbhh,
//                   |   y.yhbh,y.yhmc,y.yhlbdm,d.jldbh,b.zcbh,0 fxdl,
//                   |   getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |   getDsjbm(l.gddwbm) dsjbm,
//                   |   getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |   getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm,dc.dydj dydjbm,
//                   |   getdmbmmc('DYDJDM',dc.dydj) dydj
//                   |from ydkh y
//                   |join jld d on d.yhbh = y.yhbh
//                   |join jlddnbgx g on g.jldbh = d.jldbh
//                   |join yxdnb b on b.yxdnbbs = g.yxdnbbs
//                   |join xlxd l on l.xlxdbs = d.xlxdbs
//                   |join dcfzxx dc on dc.dcbh = y.yhbh and dc.dydj = '08'
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where y.yhlbdm = '40' and y.yhztdm <> '2'
//                   |    and exists (select 1 from (select e.dcbh,max(e.fgbs) fgbs from jsdy e where e.gsgx = '2' group by e.dcbh) js where js.dcbh = y.yhbh and js.fgbs = '0')
//                   |    and not exists (select 1 from cbxx c where c.sslxdm = '221' and c.dfny = ${nowMonth} and c.yhbh = y.yhbh and c.jldbh = d.jldbh)
//                   |
//                   |union all
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |   l.xlxdbs,l.xlbh,l.xlmc,
//                   |   null tqbs,null tqbh,null tqmc,
//                   |   getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |   k1.bdzkhbhh,null tqkhbhh,
//                   |   y.yhbh,y.yhmc,y.yhlbdm,d.jldbh,b.zcbh,0 fxdl,
//                   |   getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |   getDsjbm(l.gddwbm) dsjbm,
//                   |   getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |   getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm,dc.dydj dydjbm,
//                   |   getdmbmmc('DYDJDM',dc.dydj) dydj
//                   |from ydkh y
//                   |join jld d on d.yhbh = y.yhbh
//                   |join jlddnbgx g on g.jldbh = d.jldbh
//                   |join yxdnb b on b.yxdnbbs = g.yxdnbbs
//                   |join xlxd l on l.xlxdbs = d.xlxdbs
//                   |join cbxx c on c.yhbh = y.yhbh and c.jldbh = d.jldbh and c.dfny = ${nowMonth} and c.sslxdm = '222' and c.bjdl = 0 and c.jbdl = 0
//                   |join cbxx c2 on c2.yhbh = y.yhbh and c2.jldbh = d.jldbh and c2.dfny = ${nowMonth} and c2.sslxdm = '223' and c2.bjdl = 0 and c2.jbdl = 0
//                   |join cbxx c3 on c3.yhbh = y.yhbh and c3.jldbh = d.jldbh and c3.dfny = ${nowMonth} and c3.sslxdm = '224' and c3.bjdl = 0 and c3.jbdl = 0
//                   |join dcfzxx dc on dc.dcbh = y.yhbh and dc.dydj = '08'
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where y.yhlbdm = '40' and y.yhztdm <> '2'
//                   |and not exists (select 1 from (select e.dcbh,max(e.fgbs) fgbs from jsdy e where e.gsgx = '2' group by e.dcbh) js where js.dcbh = y.yhbh and js.fgbs = '0')
//                   |
//                   |union all
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |   l.xlxdbs,l.xlbh,l.xlmc,null tqbs,null tqbh,null tqmc,getbdzbs(l.xlxdbs) bdzbs,
//                   |   getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |   k1.bdzkhbhh,null tqkhbhh,
//                   |   y.yhbh,y.yhmc,y.yhlbdm,d.jldbh,b.zcbh,0 fxdl,
//                   |   getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |   getDsjbm(l.gddwbm) dsjbm,
//                   |   getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |   getzzmc(getQxjbm(l.gddwbm)) qxj,
//                   |   getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
//                   |   getdqbm(l.gddwbm) dqbm,${nybm} nybm,dc.dydj dydjbm,getdmbmmc('DYDJDM',dc.dydj) dydj
//                   |from ydkh y
//                   |join jld d on d.yhbh = y.yhbh
//                   |join jlddnbgx g on g.jldbh = d.jldbh
//                   |join yxdnb b on b.yxdnbbs = g.yxdnbbs
//                   |join xlxd l on l.xlxdbs = d.xlxdbs
//                   |join dcfzxx dc on dc.dcbh = y.yhbh and dc.dydj = '08'
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where y.yhlbdm = '40' and y.yhztdm <> '2'
//                   |and not exists (select 1 from (select e.dcbh,max(e.fgbs) fgbs from jsdy e where e.gsgx = '2' group by e.dcbh) js where js.dcbh = y.yhbh and js.fgbs = '0')
//                   |and not exists (select 1 from cbxx c where c.sslxdm in ('222','223','224') and c.dfny = ${nowMonth} and c.yhbh = y.yhbh and c.jldbh = d.jldbh)
//                   |
//                 """.stripMargin
//            sparkSession.sql(xl_18).createOrReplaceTempView("res_gk_wtjdfdchdlqd")
//            sparkSession.sql("select * from res_gk_wtjdfdchdlqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_wtjdfdchdlqd")
//
//            //todo 1.8 未统计地方电厂户电量清单
//            try {
//                sparkSession.sql(s"select getUUID() id,* from res_gk_wtjdfdchdlqd")
//                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_wtjdfdchdlqd"))
//                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            }catch {
//                case e:Exception => e.printStackTrace()
//            }finally {
//                println("1.8 未统计地方电厂户电量清单 ")
//            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_WTJDFDCHDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_WTJDFDCHDLQD")


//            sparkSession.sql(
//                s"""
//                   |select
//                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
//                   |    gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
//                   |from res_gk_wtjdfdchdlqd
//                   |
//                 """.stripMargin).repartition(resultPartition).createOrReplaceTempView("res_gk_wtjdfdchdlqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_wtjdfdchdlqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl18',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.8运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //todo 9 线路1.9 线损异常的公变台区清单
        try{
            val xl_19 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    xl.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,tq.xlxdbs,
                   |    tq.xlbh,tq.xlmc,tq.tqbs,tq.tqbh,tq.tqmc,getbdzbs(tq.xlxdbs) bdzbs,
                   |    getbdzbh(tq.xlxdbs) bdzbh,getbdzmc(tq.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,
                   |    handleNumber(tq.bygdl) gdl,
                   |    handleNumber(tq.bysdl) sdl,
                   |    handleNumber(tq.byxsl) xsl,
                   |    0 xgxxs,
                   |    getycgzbh(${Constant.XL_17}) ycgzbh,
                   |    getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
                   |    getdqbm(xl.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx xl
                   |inner join xsycxlhtqmx tq on tq.xlxdbs = xl.xlxdbs and tq.xltqbz = '2' and tq.ny = ${nowMonth} and tq.byxsl > 0
                   |inner join jld j on tq.tqbs = j.tqbs and j.jldytdm <> '410'
                   |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '60' and y.yhztdm <> '2'
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(tq.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '1' and xl.ny = ${nowMonth} and xl.byxsl < 0
                   |
                   |union all
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    xl.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,tq.xlxdbs,
                   |    tq.xlbh,tq.xlmc,tq.tqbs,tq.tqbh,tq.tqmc,getbdzbs(tq.xlxdbs) bdzbs,
                   |    getbdzbh(tq.xlxdbs) bdzbh,getbdzmc(tq.xlxdbs) bdzmc,k1.bdzkhbhh,
                   |    k2.tqkhbhh,
                   |    handleNumber(tq.bygdl) gdl,
                   |    handleNumber(tq.bysdl) sdl,
                   |    handleNumber(tq.byxsl) xsl,
                   |    0 xgxxs,
                   |    getycgzbh(${Constant.XL_17}) ycgzbh,
                   |    getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
                   |    getdqbm(xl.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx xl
                   |inner join xsycxlhtqmx tq on tq.xlxdbs = xl.xlxdbs and tq.xltqbz = '2' and tq.ny = ${nowMonth} and tq.byxsl < 0
                   |inner join jld j on tq.tqbs = j.tqbs and j.jldytdm <> '410'
                   |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '60' and y.yhztdm <> '2'
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(tq.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '1' and xl.ny = ${nowMonth} and xl.byxsl > 0
                   |
                 """.stripMargin

            sparkSession.sql(xl_19).createOrReplaceTempView("res_gk_xsycdgbtqqd")
            sparkSession.sql("select * from res_gk_xsycdgbtqqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_xsycdgbtqqd")

            //todo  1.9线损异常的公变台区清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_xsycdgbtqqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_xsycdgbtqqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println(" 1.9线损异常的公变台区清单")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.gk_xsycdgbtqqd_his select getUUID(),*,tjsj fqrq from RES_GK_XSYCDGBTQQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_XSYCDGBTQQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_XSYCDGBTQQD_YCGDDWXLGX")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_xsycdgbtqqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl19',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.9运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //todo 10 线路1.10 负线损线路高供低计用户变损电量
        try{
//            val xl_110 =
//                s"""
//                   |select
//                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    xs.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |   j.xlxdbs,xs.xlbh,xs.xlmc,
//                   |   null tqbs,null tqbh,null tqmc,
//                   |   getbdzbs(j.xlxdbs) bdzbs,
//                   |   getbdzbh(j.xlxdbs) bdzbh,getbdzmc(j.xlxdbs) bdzmc,
//                   |   k1.bdzkhbhh,null tqkhbhh,
//                   |   j.yhbh,j.yhmc,j.yhlbdm,
//                   |   handleNumber(b.edrl) byqrl,
//                   |   j.jlfsdm,
//                   |   handleNumber(j.ygcjdl+j.ygbsdl) jsdl,
//                   |   handleNumber(j.ygcjdl) cjdl,
//                   |   handleNumber(j.ygbsdl) bsdl,
//                   |   getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |   getDsjbm(xs.gddwbm) dsjbm,
//                   |   getQxjbm(xs.gddwbm) qxjbm,getGdsbm(xs.gddwbm) gdsbm,
//                   |   getzzmc(getDsjbm(xs.gddwbm)) dsj,getzzmc(getQxjbm(xs.gddwbm)) qxj,
//                   |   getzzmc(getGdsbm(xs.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,
//                   |   getdmbmmc('JLFSDM',j.jlfsdm) jlfs,getdqbm(xs.gddwbm) dqbm,${nybm} nybm
//                   |from xlxstjxx xs
//                   |inner join jldxx j on j.xlxdbs = xs.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
//                   |    and j.yhlbdm in ('10','11') and j.jlfsdm = '2' and j.yhztdm <> '2'
//                   |inner join yxbyq b on b.yhbh = j.yhbh and b.gbzbbz = '2'
//                   |inner join (
//                   |         select
//                   |            j.xlxdbs,sum(j.ygbsdl) bsdl
//                   |         from xlxstjxx xs
//                   |         inner join jldxx j on  j.xlxdbs = xs.xlxdbs
//                   |            and j.jldytdm <> '410' and j.dfny = ${nowMonth}
//                   |            and j.yhlbdm in ('10','11') and j.jlfsdm = '2' and j.yhztdm <> '2'
//                   |         where xs.ny = ${nowMonth} and xs.byxsl < 0
//                   |         group by j.xlxdbs
//                   |         ) a
//                   |on a.xlxdbs = xs.xlxdbs and xs.byxsl < 0 and if(sign(xs.bygdl)=1,a.bsdl/xs.bygdl,0)>0.005
//                   |lateral view outer explode(split(getbdzkhb(j.xlxdbs),',')) k1 as bdzkhbhh
//                   |where xs.ny = ${nowMonth} and xs.byxsl < 0
//                   |
//                 """.stripMargin
//
//            sparkSession.sql(xl_110).createOrReplaceTempView("res_gk_fsxlbszbdlpdzbkhqd")
//            sparkSession.sql("select * from res_gk_fsxlbszbdlpdzbkhqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition)
//              .createOrReplaceTempView("res_gk_fsxlbszbdlpdzbkhqd")
//
//            //todo 1.10负线损线路高供低计用户变损电量
//            try {
//                sparkSession.sql(s"select getUUID() id,* from res_gk_fsxlbszbdlpdzbkhqd")
//                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_fsxlbszbdlpdzbkhqd"))
//                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            }catch {
//                case e:Exception => e.printStackTrace()
//            }finally {
//                println("1.10负线损线路高供低计用户变损电量  ")
//            }

//            sparkSession.sql(s"insert into ${writeSchema}.GK_FSXLBSZBDLPDZBKHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_FSXLBSZBDLPDZBKHQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_FSXLBSZBDLPDZBKHQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_FSXLBSZBDLPDZBKHQD_YCGDDWXLGX")
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_fsxlbszbdlpdzbkhqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.ycgddwxlgx "))
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl110',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.10运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        start = System.currentTimeMillis()
        //todo 11 线路1.11 非周期性计费电量用户   fzqjfdl未确定
        try{
            //lixc20200510改造sql
            //--F-CSG-MK0514-10-01 过户,F-CSG-MK0514-10-02 批量过户,F-CSG-MK0514-11-01 销户,F-CSG-MK0514-11-02 批量销户,F-CSG-MK0540-01-01 更名过户.
//            val xl_111 =
//                s"""
//                   |select
//                   |    ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
//                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,
//                   |   l.xlxdbs,l.xlbh,l.xlmc,
//                   |   null tqbs,null tqbh,null tqmc,
//                   |   getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
//                   |   k1.bdzkhbhh,null tqkhbhh,
//                   |   y.yhbh,y.yhmc,y.yhlbdm,j.jldbh,c.zcbh,
//                   |   handleNumber(c.scbss) qm,
//                   |   handleNumber(c.bcbss) zm,
//                   |   handleNumber(c.zhbl) zhbl,
//                   |   handleNumber(c.bjdl) dl,
//                   |   f.gzdbh fzqjfgdh,(coalesce(j.jfdl, 0) + coalesce(j.mfdl, 0)) fzqjfdl,
//                   |   getycgzbh(${Constant.XL_17}) ycgzbh,
//                   |   getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
//                   |   getzzmc(getDsjbm(l.gddwbm)) dsj,
//                   |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
//                   |   getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
//                   |from ydkh y
//                   |join fzqjfsq f on f.yhbh = y.yhbh and f.ywlbdm = '0700' and f.cldfny = ${nowMonth}
//                   |    and f.ywzlbh in ('F-CSG-MK0514-10-01','F-CSG-MK0514-10-02','F-CSG-MK0514-11-01','F-CSG-MK0514-11-02','F-CSG-MK0540-01-01')
//                   |join jldxx j on j.gzdbh = f.clgzdbh and j.yhbh = f.yhbh and j.dfny = f.cldfny
//                   |join cbxx c on c.yhbh = j.yhbh and c.jldbh = j.jldbh and c.dfny = j.dfny and c.sslxdm = '121'
//                   |join xlxd l on l.xlxdbs = j.xlxdbs
//                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
//                   |where y.yhlbdm in ('10', '11', '30','98')
//                   |    and not exists (select 1 from xlsdlmx x where x.yhbh = j.yhbh and x.jldbh = j.jldbh
//                   |    and (coalesce(j.jfdl, 0) + coalesce(j.mfdl, 0)) = x.ygzdl)
//                   |
//                 """.stripMargin
//            sparkSession.sql(xl_111).createOrReplaceTempView("res_gk_fzqxjfdlyhqd")
//            sparkSession.sql("select * from res_gk_fzqxjfdlyhqd where isFiveDsj(gddwbm) = 1")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_fzqxjfdlyhqd")
//
//            //todo 1.11 非周期性计费电量用户清单
//            try {
//                sparkSession.sql(s"select getUUID() id,* from res_gk_fzqxjfdlyhqd")
//                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_fzqxjfdlyhqd"))
//                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//
//            }catch {
//                case e:Exception => e.printStackTrace()
//            }finally {
//                println("1.11 非周期性计费电量用户清单 ")
//            }



//            sparkSession.sql(s"insert into ${writeSchema}.GK_FZQXJFDLYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_FZQXJFDLYHQD")

//            sparkSession.sql(
//                s"""
//                   |select
//                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
//                   |    gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh)
//                   |from RES_GK_FZQXJFDLYHQD
//                   |
//                 """.stripMargin).repartition(resultPartition).createOrReplaceTempView("res_gk_fzqxjfdlyhqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_fzqxjfdlyhqd_ycgddwxlgx")
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
//        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl111',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.11运行${(end-start)/1000}秒")


        }

}
