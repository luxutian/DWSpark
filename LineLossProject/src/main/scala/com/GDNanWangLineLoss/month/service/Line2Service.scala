package com.GDNanWangLineLoss.month.service

import java.sql.Date

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._
import com.GDNanWangLineLoss.month.dao.DataSourceDao
import com.GDNanWangLineLoss.month.util.{Functions, UDFfunction}

object Line2Service {
    def line2Service(sparkSession:SparkSession, url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1


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

        start = System.currentTimeMillis()
        try{
            val xl_21 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    j.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${xlbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(xl.xlxdbs) bdzbs,
                   |    getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,
                   |    jl.gddwbm sygddwbm,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_21})不存在
                   |    getDsjbm(xl.gddwbm) dsjbm,
                   |    getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(xl.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx xl
                   |inner join jldxx j on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |inner join jldxx jl on jl.yhbh = j.yhbh and jl.jldytdm <> '410' and jl.dfny = ${lastMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |where xl.xltqbz = '1' and xl.ny = ${nowMonth} and jl.gddwbm <> j.gddwbm
                   |    and (j.yhlbdm in ('60','10','11') or (j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj = '08'))) and j.yhztdm <> '2'
                   |    and (jl.yhlbdm in ('60','10','11') or (jl.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = jl.yhbh and b.dydj = '08'))) and jl.yhztdm <> '2'
                """.stripMargin
            sparkSession.sql(xl_21).createOrReplaceTempView("res_gk_cshgddwbgqd")

            sparkSession.sql("select * from res_gk_cshgddwbgqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_cshgddwbgqd")

            //todo 2.1初始化供电单位变更清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_cshgddwbgqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_cshgddwbgqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.1初始化供电单位变更清单")
            }



//            sparkSession.sql(s"insert into ${writeSchema}.gk_cshgddwbgqd_his select getUUID(),*,tjsj fqrq from RES_GK_CSHGDDWBGQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_CSHGDDWBGQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_CSHGDDWBGQD_YCGDDWXLGX")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_cshgddwbgqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl21',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.1运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        //线路2.2供电单位为空       sygddwbm未确定
        start = System.currentTimeMillis()
        try{
            val xl_22 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,
                   |    getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,
                   |    y.yhlbdm,null sygddwbm,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_22})不存在
                   |    getDsjbm(y.gddwbm) dsjbm,
                   |    getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from _dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
                   |    and y.yhztdm <> '2' and y.gddwbm is null
                """.stripMargin
            sparkSession.sql(xl_22).createOrReplaceTempView("res_gk_gddwwkhbgqd")
            sparkSession.sql("select * from RES_GK_GDDWWKHBGQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_GDDWWKHBGQD")

            //todo 2.2供电单位为空户变更清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_gddwwkhbgqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gddwwkhbgqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.2供电单位为空户变更清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_GDDWWKHBGQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GDDWWKHBGQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_GDDWWKHBGQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_GDDWWKHBGQD_YCGDDWXLGX")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gddwwkhbgqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"tt"))
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl22',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.2运行${(end-start)/1000}秒")

        //线路2.3电压等级有变更
        start = System.currentTimeMillis()
        try{  //什么样的条件下A表要joinB表2次，join的条件不一样
            val xl_23 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    xl.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${xlbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,jl.jldydjdm sqdydjdm,j.jldydjdm jldydjdm,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_23})不存在
                   |    getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdmbmmc('DYDJDM',jl.jldydjdm) sqdydj,
                   |    getdmbmmc('DYDJDM',j.jldydjdm) dydj,getdqbm(xl.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx xl
                   |inner join jldxx j on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |inner join jldxx jl on jl.yhbh = j.yhbh and jl.jldytdm <> '410' and jl.dfny = ${lastMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |where xl.xltqbz = '1' and xl.ny = ${nowMonth} and jl.jldydjdm <> j.jldydjdm
                   |    and j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj = '08') and j.yhztdm <> '2'
                   |    and jl.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = jl.yhbh and b.dydj = '08') and jl.yhztdm <> '2'
                """.stripMargin
            sparkSession.sql(xl_23).createOrReplaceTempView("res_gk_dfdchdydjbgqd")
            sparkSession.sql("select * from res_gk_dfdchdydjbgqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_dfdchdydjbgqd")

            //todo 2.3地方电厂户电压等级变更清单
            try {

                sparkSession.sql(s"select getUUID() id,* from res_gk_dfdchdydjbgqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dfdchdydjbgqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.3地方电厂户电压等级变更清单  ")
            }

//            sparkSession.sql(s"insert into ${writeSchema}.GK_DFDCHDYDJBGQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_DFDCHDYDJBGQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dfdchdydjbgqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_dfdchdydjbgqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dfdchdydjbgqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl23',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.3运行${(end-start)/1000}秒")

        /*-------------------------------------------------------------------*/
        //线路2.4 计量点档案所属线路为空检查      线路为空，就没有线路和变电站信息
        start = System.currentTimeMillis()
        try{
            val xl_24 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    null gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,null xlxdbs,null xlbh,null xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    null bdzbs,null bdzbh,null bdzmc,
                   |    null bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_24})不存在
                   |    getDsjbm(j.gddwbm) dsjbm,getQxjbm(j.gddwbm) qxjbm,
                   |    getGdsbm(j.gddwbm) gdsbm,getzzmc(getDsjbm(j.gddwbm)) dsj,getzzmc(getQxjbm(j.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(j.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(j.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410' and j.xlxdbs is null
                   |where (y.yhlbdm in ('80','60','10','11')
                   |    or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))  -- dcfzxx 电厂辅助信息表
                   |    and y.yhztdm <> '2'
                """.stripMargin
            sparkSession.sql(xl_24).createOrReplaceTempView("res_gk_jldxxdassxlwkqd")
            sparkSession.sql("select * from res_gk_jldxxdassxlwkqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jldxxdassxlwkqd")

            //todo 2.4计量点信息档案所属线路为空清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_jldxxdassxlwkqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jldxxdassxlwkqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.4计量点信息档案所属线路为空清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLDXXDASSXLWKQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLDXXDASSXLWKQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jldxxdassxlwkqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_jldxxdassxlwkqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jldxxdassxlwkqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl24',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.4运行${(end-start)/1000}秒")

        //线路2.5计量点档案不一致检查       这个结果有线路，也有台区。台区考核户要不要写？
        start = System.currentTimeMillis()
        try{
            val xl_25 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    xsxl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,xsxl.xlxdbs,xsxl.xlbh,xsxl.xlmc,
                   |    xstq.tqbs,xstq.tqbh,xstq.tqmc,getbdzbs(xsxl.xlxdbs) bdzbs,getbdzbh(xsxl.xlxdbs) bdzbh,
                   |    getbdzmc(xsxl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,g.xlxdbs xsjlddaxlxdbs,
                   |    xsxl.xlbh xsjlddaxlbh,xsxl.xlmc xsjlddaxlmc,g.tqbs xsjlddatqbs,xstq.tqbh xsjlddatqbh,
                   |    xstq.tqmc xsjlddatqmc,d.xlxdbs zxbhxlxdbs,zxxl.xlbh zxbhtqbh,zxxl.xlmc zxbhxlmc,
                   |    d.tqbs zxbhtqbs,zxtq.tqbh zxbhtqbh,zxtq.tqmc zxbhtqmc,j.xlxdbs khzhdaxlxdbs,khxl.xlbh khzhdaxlbh,
                   |    khxl.xlmc khzhdaxlmc,j.tqbs khzhdatqbs,khtq.tqbh khzhdatqbh,khtq.tqmc khzhdatqmc,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_25})不存在
                   |    getDsjbm(xsxl.gddwbm) dsjbm,getQxjbm(xsxl.gddwbm) qxjbm,
                   |    getGdsbm(xsxl.gddwbm) gdsbm,getzzmc(getDsjbm(xsxl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xsxl.gddwbm)) qxj,getzzmc(getGdsbm(xsxl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(xsxl.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join jldxx g on g.yhbh = y.yhbh and g.dfny = ${nowMonth}
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410' and g.xlxdbs <> j.xlxdbs
                   |inner join yhdyxx d on d.yhbh = j.yhbh
                   |inner join xlxd xsxl on xsxl.xlxdbs = g.xlxdbs
                   |left join all_tq xstq on xstq.tqbs = g.tqbs
                   |inner join all_xlxd zxxl on zxxl.xlxdbs = d.xlxdbs
                   |left join all_tq zxtq on zxtq.tqbs = d.tqbs
                   |inner join all_xlxd khxl on khxl.xlxdbs = j.xlxdbs
                   |left join all_tq khtq on khtq.tqbs = j.tqbs
                   |lateral view outer explode(split(getbdzkhb(j.xlxdbs),',')) k1 as bdzkhbhh
                   |where (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
                   |union
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    xsxl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,xsxl.xlxdbs,xsxl.xlbh,xsxl.xlmc,
                   |    xstq.tqbs,xstq.tqbh,xstq.tqmc,getbdzbs(xsxl.xlxdbs) bdzbs,getbdzbh(xsxl.xlxdbs) bdzbh,
                   |    getbdzmc(xsxl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,g.xlxdbs xsjlddaxlxdbs,
                   |    xsxl.xlbh xsjlddaxlbh,xsxl.xlmc xsjlddaxlmc,g.tqbs xsjlddatqbs,xstq.tqbh xsjlddatqbh,xstq.tqmc xsjlddatqmc,
                   |    d.xlxdbs zxbhxlxdbs,zxxl.xlbh zxbhtqbh,zxxl.xlmc zxbhxlmc,d.tqbs zxbhtqbs,zxtq.tqbh zxbhtqbh,zxtq.tqmc zxbhtqmc,
                   |    j.xlxdbs khzhdaxlxdbs,khxl.xlbh khzhdaxlbh,khxl.xlmc khzhdaxlmc,j.tqbs khzhdatqbs,
                   |    khtq.tqbh khzhdatqbh,khtq.tqmc khzhdatqmc,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_25})不存在
                   |    getDsjbm(xsxl.gddwbm) dsjbm,getQxjbm(xsxl.gddwbm) qxjbm,
                   |    getGdsbm(xsxl.gddwbm) gdsbm,getzzmc(getDsjbm(xsxl.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(xsxl.gddwbm)) qxj,getzzmc(getGdsbm(xsxl.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(xsxl.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join jldxx g on g.yhbh = y.yhbh and g.dfny = ${nowMonth}
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join yhdyxx d on d.yhbh = j.yhbh and g.xlxdbs <> d.xlxdbs
                   |inner join xlxd xsxl on xsxl.xlxdbs = g.xlxdbs
                   |left join all_tq xstq on xstq.tqbs = g.tqbs
                   |inner join all_xlxd zxxl on zxxl.xlxdbs = d.xlxdbs
                   |left join all_tq zxtq on zxtq.tqbs = d.tqbs
                   |inner join all_xlxd khxl on khxl.xlxdbs = j.xlxdbs
                   |left join all_tq khtq on khtq.tqbs = j.tqbs
                   |lateral view outer explode(split(getbdzkhb(j.xlxdbs),',')) k1 as bdzkhbhh
                   |where (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
                """.stripMargin
            sparkSession.sql(xl_25).createOrReplaceTempView("res_gk_yxljlddayyhdabyzqd")
            sparkSession.sql("select * from res_gk_yxljlddayyhdabyzqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_yxljlddayyhdabyzqd")

            //todo 2.5计量点档案与用户档案不一致清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_yxljlddayyhdabyzqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yxljlddayyhdabyzqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.5计量点档案与用户档案不一致清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_YXLJLDDAYYHDABYZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_YXLJLDDAYYHDABYZQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yxljlddayyhdabyzqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxljlddayyhdabyzqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxljlddayyhdabyzqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl25',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.5运行${(end-start)/1000}秒")

        //线路2.6营销系统和GIS系统电源点不一致     这个结果要不要有台区信息？已确定要
        //11.04 修改byqbh为byqbs
        start = System.currentTimeMillis()
        try{
            val xl_26 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,b.sbbs byqbs,b.mc byqmc,b.sbbs byqbh,b.gisid byqgisid,
                   |    l.xlxdbs yxxtdydxlxdbs,l.xlbh yxxtdydxlbh,l.xlmc yxxtdydxlmc,l.gisid yxxtdydxlgisid,
                   |    lg.id gisxsdydxlxdbs,'' gisxsdydxlbh,lg.fl_name gisxsdydxlmc,c.circuit_id gisxsdydxlgisid,
                   |    null yxxtdydtqbs,null yxxtdydtqbh,null yxxtdydtqmc,null yxxtdydtqgisid,null gisxsdydtqbs,null gisxsdydtqbh,null gisxsdydtqmc,null gisxsdydxlbh,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_26}) 不存在
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join yxbyq b on b.yhbh = y.yhbh
                   |inner join all_tq t on t.tqbs = b.tqbs
                   |inner join xltqgx gx on gx.tqbs = t.tqbs
                   |inner join xlxd l on l.xlxdbs = gx.xlxdbs
                   |inner join transcircuitsupply c on c.trans_id = b.gisid and c.circuit_id <> l.gisid and c.main_supply = '1'
                   |left join gis_g_dm_function_location lg on lg.id = c.circuit_id
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where y.yhlbdm in ('60','10','11')
                   |    and y.yhztdm <> '2'
                   |union all
                   |select
                   |    distinct${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,b.sbbs byqbs,b.mc byqmc,b.sbbs byqbh,b.gisid byqgisid,l.xlxdbs yxxtdydxlxdbs,
                   |    l.xlbh yxxtdydxlbh,l.xlmc yxxtdydxlmc,l.gisid yxxtdydxlgisid,
                   |    lg.id gisxsdydxlxdbs,'' gisxsdydxlbh,lg.fl_name gisxsdydxlmc,c.circuit_id gisxsdydxlgisid,
                   |    null yxxtdydtqbs,null yxxtdydtqbh,null yxxtdydtqmc,null yxxtdydtqgisid,null gisxsdydtqbs,null gisxsdydtqbh,null gisxsdydtqmc,null gisxsdydxlbh,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_26})不存在
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join yhdyxx d on d.yhbh = y.yhbh
                   |inner join all_tq t on t.tqbs = d.tqbs
                   |inner join yxbyq b on b.tqbs = t.tqbs
                   |inner join xlxd l on l.xlxdbs = d.xlxdbs
                   |inner join transcircuitsupply c on c.trans_id = b.gisid and c.circuit_id <> l.gisid and c.main_supply = '1'
                   |left join gis_g_dm_function_location lg on lg.id = c.circuit_id
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (y.yhlbdm = '80' or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
                   |    and y.yhztdm <> '2'
                """.stripMargin
            sparkSession.sql(xl_26).createOrReplaceTempView("res_gk_gisxthyxxtdydbyzqd")
            sparkSession.sql("select * from res_gk_gisxthyxxtdydbyzqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_gisxthyxxtdydbyzqd")

            //todo 2.6 GIS系统和营销系统电源点不一致清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_gisxthyxxtdydbyzqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gisxthyxxtdydbyzqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.6 GIS系统和营销系统电源点不一致清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_GISXTHYXXTDYDBYZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GISXTHYXXTDYDBYZQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gisxthyxxtdydbyzqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_gisxthyxxtdydbyzqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gisxthyxxtdydbyzqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl26',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.6运行${(end-start)/1000}秒")

        //线路2.7光伏三户所属线路不一致
        start = System.currentTimeMillis()
        try{
            val xl_27 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,ydx.xlxdbs,ydx.xlbh,
                   |    ydx.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(ydx.xlxdbs) bdzbs,getbdzbh(ydx.xlxdbs) bdzbh,
                   |    getbdzmc(ydx.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,gfx.xlxdbs fdhxlxdbs,gfx.xlbh fdyhxlbh,
                   |    gfx.xlmc fdyhxlmc,dcx.xlxdbs fdhxlxdbs,dcx.xlbh dcswyhxlbh,
                   |    dcx.xlmc dcswyhxlmc,ydx.xlxdbs ydhxlxdbs,ydx.xlbh ydhxlbh,
                   |    ydx.xlmc ydhxlmc,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_27})不存在
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from gfyhdaxx d --光伏用户档案信息
                   |inner join gfyhglxx g on g.gfhyhbh = d.yhbh --光伏用户关联信息
                   |inner join ydkh y on y.yhbh = d.yhbh and y.yhztdm <> '2'
                   |inner join yhdyxx gf on gf.yhbh = g.gfhyhbh
                   |inner join all_xlxd gfx on gfx.xlxdbs = gf.xlxdbs
                   |inner join yhdyxx dc on dc.yhbh = g.gdhyhbh
                   |inner join all_xlxd dcx on dcx.xlxdbs = dc.xlxdbs
                   |inner join yhdyxx yd on yd.yhbh = g.ydhyhbh
                   |inner join all_xlxd ydx on ydx.xlxdbs = yd.xlxdbs
                   |inner join xlxd l on l.xlxdbs = ydx.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(ydx.xlxdbs),',')) k1 as bdzkhbhh
                   |where (gf.xlxdbs <> dc.xlxdbs or dc.xlxdbs <> yd.xlxdbs or gf.xlxdbs <> yd.xlxdbs)
                """.stripMargin
            sparkSession.sql(xl_27).createOrReplaceTempView("res_gk_gfshssxlbyzqd")
            sparkSession.sql("select * from res_gk_gfshssxlbyzqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gfshssxlbyzqd")

            //todo 2.7 光伏三户所属线路不一致清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_gfshssxlbyzqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gfshssxlbyzqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.7 光伏三户所属线路不一致清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_GFSHSSXLBYZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GFSHSSXLBYZQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gfshssxlbyzqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_gfshssxlbyzqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gfshssxlbyzqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl27',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.7运行${(end-start)/1000}秒")

        //线路2.8  yxxtzgdkssj,yxxtzgdjssj未确定
        start = System.currentTimeMillis()
        try{
            val xl_28 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    handleTime(null) yxxtzgdkssj,
                   |    handleTime(null) yxxtzgdjssj,
                   |    d.execute_begin_time zcglxtzgdsjkssj,d.execute_end_time zcglxtzgdsjjssj,
                   |    m.code zcglxtgdyxfsddh,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_28})不存在
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from xlxd l
                   |join nzwot_sp_so_runmode_device d on (d.rolloff_line_id = l.gisid or d.rollon_line_id = l.gisid)
                   |join nzwot_sp_so_runmode_manage m on m.id = d.mode_id
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (getYearMonth(d.execute_begin_time) = ${nowMonth} or getYearMonth(d.execute_end_time) = ${nowMonth})
                   |
                 """.stripMargin
            sparkSession.sql(xl_28).repartition(resultPartition).createOrReplaceTempView("res_gk_zgdyxfsqzsjbyzqd")

            //todo 2.8转供电运行方式起止时间不一致清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_zgdyxfsqzsjbyzqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_zgdyxfsqzsjbyzqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.8转供电运行方式起止时间不一致清单  ")
            }



//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_zgdyxfsqzsjbyzqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_zgdyxfsqzsjbyzqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj from res_gk_zgdyxfsqzsjbyzqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl28',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.8运行${(end-start)/1000}秒")

        //线路2.9线路档案变更电子化移交不及时清单
        //2019年11月22日10:48:04 lixc
        start = System.currentTimeMillis()
        try{
            val xl_29 =
                s"""
                    |select
                    |   distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                    |   l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                    |   null tqbs,null tqbh,null tqmc,
                    |   getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                    |   k1.bdzkhbhh,null tqkhbhh,
                    |   y.yhbh,y.yhmc,y.yhlbdm,handleNumber(j.jfdl),d.ykgdbh,handleTime(g.sqsj),handleTime(g.wcsj),
                    |   null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_29})不存在
                    |   getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                    |   getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                    |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                    |   getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                    |from ydkh y
                    |join dzhyjgzdxx d on d.yhbh = y.yhbh  --电子化移交工作单信息表
                    |join ykgzdjbxx yg on yg.gzdbh = d.ykgdbh  --业扩工作单基本信息
                    |join gzdxxls g on g.slbs = yg.slbs  --工作单信息历史
                    |join jldxx j on j.yhbh = y.yhbh and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                    |join xlxd l on l.xlxdbs = j.xlxdbs
                    |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                    |where (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08'))) and y.yhztdm <> '2'
                    |   and datediff(g.sqsj,${gdfyrq}) < 0 and ((g.gzdzt = '1' and g.wcsj is null) or (g.gzdzt = '2' and datediff(g.wcsj,${gdfyrq}) >= 0))
                """.stripMargin
            sparkSession.sql(xl_29).createOrReplaceTempView("res_gk_dabgdzhyjbjsqd")
            sparkSession.sql("select * from res_gk_dabgdzhyjbjsqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_dabgdzhyjbjsqd")

            //todo 2.9档案变更电子化移交不及时清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_dabgdzhyjbjsqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_dabgdzhyjbjsqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.9档案变更电子化移交不及时清单  ")
            }

            //            sparkSession.sql(s"insert into ${writeSchema}.GK_DABGDZHYJBJSQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_DABGDZHYJBJSQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_dabgdzhyjbjsqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_dabgdzhyjbjsqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_dabgdzhyjbjsqd_ycgddwxlgx")
//              .write.options(Map("kudu.master"->url,"kudu.table" -> s"tt"))
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl29',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.9运行${(end-start)/1000}秒")

        //线路2.10 考核表反向有功电量计量点缺失 swdl-上网电量，fxygdl-反向有功电量 未确定
        //2019年11月19日21:18:42 lixc 修改地方电厂户电压等级判断条件；1有10kV地方电厂户，没有变电站考核户表反向示数类型2有0.4kV地方电厂户，有台区反向电量的，没有变电站考核户表反向示数类型
        start = System.currentTimeMillis()
        try{
            val xl_210 =
                s"""
                    |select
                    |   ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                    |   l.gddwbm,
                    |   ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                    |   null tqbs,null tqbh,null tqmc,
                    |   getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                    |   k1.bdzkhbhh,null tqkhbhh,
                    |   j.yhbh,j.yhmc,j.yhlbdm,handleNumber(sw.swdl) swdl,null fxygdl,
                    |   null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_210})不存在
                    |   getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                    |   getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                    |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                    |   getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                    |from xlxd l
                    |join jldxx j on j.xlxdbs = l.xlxdbs and j.yhlbdm = '80' and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                    |left join xldfdcswdl sw on sw.xlxdbs = l.xlxdbs  --线路地方电厂上网电量
                    |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                    |where exists (select 1 from gdljfhzxx a,dcfzxx b,yhdyxx dy where b.dcbh = a.dcbh and a.dcbh = dy.yhbh and a.gdyf = ${nowMonth} and b.dydj = '08' and dy.xlxdbs = l.xlxdbs)
                    |   and not exists (select 1 from cbxx c where c.sslxdm = '221' and c.dfny = ${nowMonth} and c.yhbh = j.yhbh and c.jldbh = j.jldbh)
                    |
                    |union
                    |select ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                    |l.gddwbm,
                    |   ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                    |   null tqbs,null tqbh,null tqmc,
                    |   getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                    |   k1.bdzkhbhh,null tqkhbhh,
                    |   j.yhbh,j.yhmc,j.yhlbdm,handleNumber(sw.swdl) swdl,null fxygdl,
                    |   null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_210})不存在
                    |   getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                    |   getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                    |   getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                    |   getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                    |from xlxd l
                    |join jldxx j on j.xlxdbs = l.xlxdbs and j.yhlbdm = '80' and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                    |left join xldfdcswdl sw on sw.xlxdbs = l.xlxdbs  --线路地方电厂上网电量
                    |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                    |where (exists (select 1 from gdljfhzxx a,dcfzxx b,yhdyxx dy where b.dcbh = a.dcbh and a.dcbh = dy.yhbh and a.gdyf = ${nowMonth} and b.dydj in ('02','03') and dy.xlxdbs = l.xlxdbs)
                    |   and exists (select 1 from jldxx e,cbxx f where e.dfny = f.dfny and e.jldbh = f.jldbh and e.yhbh = f.yhbh and e.yhlbdm = '60' and e.hsztdm = '90'
                    |   and f.sslxdm = '221' and e.dfny = ${nowMonth} and e.xlxdbs = l.xlxdbs))
                    |   and not exists (select 1 from cbxx c where c.sslxdm = '221' and c.dfny = ${nowMonth} and c.yhbh = j.yhbh and c.jldbh = j.jldbh)
                """.stripMargin
            sparkSession.sql(xl_210).createOrReplaceTempView("res_gk_khbfxygdkjkdqsqd")
            sparkSession.sql("select * from res_gk_khbfxygdkjkdqsqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_khbfxygdkjkdqsqd")

            //todo 2.10考核表反向有功电量计量点缺失清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_khbfxygdkjkdqsqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_khbfxygdkjkdqsqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.10考核表反向有功电量计量点缺失清单  ")
            }

            //            sparkSession.sql(s"insert into ${writeSchema}.gk_khbfxygdkjkdqsqd_his select getUUID(),*,tjsj fqrq from RES_GK_KHBFXYGDKJKDQSQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_KHBFXYGDKJKDQSQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_KHBFXYGDKJKDQSQD_YCGDDWXLGX")
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_khbfxygdkjkdqsqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl210',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.10运行${(end-start)/1000}秒")

        //2.11线路运行状态检查
        start = System.currentTimeMillis()
        try{
            val xl_211 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,y.yhztdm yxztdm,handleNumber(x.jfdl),
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_211})不存在
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdmbmmc('YXZT',y.yhztdm) yhzt,
                   |    getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join jldxx x on x.yhbh = y.yhbh and x.jldytdm <> '410' and x.dfny = ${nowMonth} and x.jfdl <> 0
                   |inner join xlxd l on l.xlxdbs = x.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
                   |    and y.yhztdm not in ('1','3')
                 """.stripMargin
            sparkSession.sql(xl_211).createOrReplaceTempView("res_gk_yxztydlbppqd")
            sparkSession.sql("select * from res_gk_yxztydlbppqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_yxztydlbppqd")

            //todo 2.11运行状态与电量不匹配清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_yxztydlbppqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yxztydlbppqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_YXZTYDLBPPQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_YXZTYDLBPPQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yxztydlbppqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_yxztydlbppqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yxztydlbppqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl211',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.11运行${(end-start)/1000}秒")

        //线路2.12电能表基础档案检查
        start = System.currentTimeMillis()
        try{
            val xl_212 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    y.yhbh,y.yhmc,y.yhlbdm,c.eddydm dnbeddydm,j.jlfsdm jlfsdm,
                   |    null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_212})不存在
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdmbmmc('DNBEDDY',c.eddydm) dnbeddy,
                   |    getdmbmmc('JLFSDM',j.jlfsdm) jlfs,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from ydkh y
                   |inner join jld j on j.yhbh = y.yhbh and j.jldytdm <> '410'
                   |inner join jlddnbgx g on g.jldbh = j.jldbh --计量点运行电能表关系
                   |inner join yxdnb d on d.yxdnbbs = g.yxdnbbs  --运行电能表
                   |inner join dnbcs c on c.csbs = d.csbs --电能表参数
                   |inner join xlxd l on l.xlxdbs = j.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where ((y.yhlbdm in ('10','11') and j.jlfsdm = '3')
                   |    or(c.eddydm = '5' and j.jlfsdm in ('2','3')) or (c.eddydm = '2' and j.jlfsdm = '1'))
                   |    and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08')))
                   |    and y.yhztdm <> '2'
                """.stripMargin
            sparkSession.sql(xl_212).createOrReplaceTempView("res_gk_jlfzpdyhlbycqd")
            sparkSession.sql("select * from res_gk_jlfzpdyhlbycqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlfzpdyhlbycqd")

            //todo 2.12计量辅助判断用户类别异常清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_jlfzpdyhlbycqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jlfzpdyhlbycqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.12计量辅助判断用户类别异常清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLFZPDYHLBYCQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLFZPDYHLBYCQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jlfzpdyhlbycqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlfzpdyhlbycqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlfzpdyhlbycqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl212',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.12运行${(end-start)/1000}秒")

        //线路2.13月线路计量、业扩工单翻月清单
        //2019年11月18日20:50:22 lixc 关联业扩工单表，流程表，修改环节提交时间，装拆信息录入环节小于28号，业扩归档（合并）环节大于等于28号
        start = System.currentTimeMillis()
        try{
            val xl_213 =
                s"""
                    |select
                    |   distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                    |   xl.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,xl.xlxdbs,xl.xlbh,
                    |   xl.xlmc,
                    |   null tqbs,null tqbh,null tqmc,
                    |   getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                    |   k1.bdzkhbhh,null tqkhbhh,
                    |   y.yhbh,y.yhmc,y.yhlbdm,yw.ywbm ywlxdm,g.gzdbh ywgzdbh,
                    |   handleNumber(j.jfdl),
                    |   null ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_213})不存在
                    |   getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,
                    |   getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,
                    |   getzzmc(getQxjbm(xl.gddwbm)) qxj,getzzmc(getGdsbm(xl.gddwbm)) gds,
                    |   getdmbmmc('YHLBDM',y.yhlbdm) yhlb,yw.ywbmmc ywlx,getdqbm(xl.gddwbm) dqbm,${nybm} nybm
                    |from xsycxlhtqmx xl
                    |inner join jldxx j on j.xlxdbs = xl.xlxdbs
                    |inner join ydkh y on y.yhbh = j.yhbh
                    |inner join ykgzdjbxx g on g.yhbh = y.yhbh --业扩工作单基本信息
                    |inner join wfrt_task_exec_info wf on wf.instance_id = g.slbs and wf.tache_name = '装拆信息录入' and datediff(wf.task_commit_time,${gdfyrq}) < 0
                    |inner join wfrt_task_exec_info wf2 on wf2.instance_id = g.slbs and wf2.tache_name = '业扩归档（合并）' and datediff(wf2.task_commit_time,${gdfyrq}) >= 0
                    |left join ywjl yw on yw.ywbm = g.ywlbbh  --业务级联
                    |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                    |where xl.xltqbz = '1' and xl.ny = ${nowMonth}
                    |   and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = y.yhbh and b.dydj = '08'))) and y.yhztdm <> '2'
                    |   and (g.ywlbbh in ('F-CSG-MK0512-01','F-CSG-MK0513-01','F-CSG-MK0513-02','F-CSG-MK0513-03','F-CSG-MK0513-04','F-CSG-MK0513-06','F-CSG-MK0513-08','F-CSG-MK0513-09',
                    |   'F-CSG-MK0514-16','F-CSG-MK0514-11','F-CSG-MK0514-28','F-CSG-MK0514-03','F-CSG-MK0514-04')
                    |   or g.ywlbbh in (select ywbm from ywjl where sjywbm = 'MK082'))
                """.stripMargin
            sparkSession.sql(xl_213).createOrReplaceTempView("res_gk_jlykgdfyqd")
            sparkSession.sql("select * from res_gk_jlykgdfyqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlykgdfyqd")

            //todo 2.13 计量、业扩工单翻月清单
            try {
                sparkSession.sql(s"select getUUID() id,* from res_gk_jlykgdfyqd")
                  .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jlykgdfyqd"))
                  .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                println("2.13 计量、业扩工单翻月清单  ")
            }


//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLYKGDFYQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLYKGDFYQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jlykgdfyqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlykgdfyqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlykgdfyqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl213',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则2.13运行${(end-start)/1000}秒")
    }

}
