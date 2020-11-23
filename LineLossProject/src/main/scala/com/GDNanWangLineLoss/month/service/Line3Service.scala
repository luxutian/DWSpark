package com.GDNanWangLineLoss.month.service

import java.sql.Date

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._

object Line3Service {
    def line3Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        //线路3.1   gk_yqzxlgsdjljdwcqd    fzl未确定
        //负线损情况下：考核表倍率如新站6万，月供电量<20万判定是否是轻载
        //新站1.2万，月供电量<10万    倍率8000，月供电量<8万
        start = System.currentTimeMillis()
        try{
            val xl_31 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,t.xlxdbs,t.xlbh,t.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    null fzl,t.byxsl,t.bygdl,t.bysdl,getycgzbh(${Constant.XL_31}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,${nybm} nybm
                   |from xlxstjxx t --线路线损统计信息
                   |join jld j on j.xlxdbs = t.xlxdbs
                   |join jlddnbgx jd on jd.jldbh = j.jldbh  --计量点运行电能表关系
                   |join yxdnb d on d.yxdnbbs = jd.yxdnbbs  --运行电能表
                   |join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '80' --用电客户
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |where t.ny = ${nowMonth} and t.bysdl < 0 and ((d.zhbl=60000 and t.bygdl < 200000) or (d.zhbl=12000 and t.bygdl < 100000) or (d.zhbl=8000 and t.bygdl < 80000))
                   |
                 """.stripMargin
            sparkSession.sql(xl_31).createOrReplaceTempView("res_gk_yqzxlgsdjljdwcqd")
            sparkSession.sql("select * from res_gk_yqzxlgsdjljdwcqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yqzxlgsdjljdwcqd")

            //供售端计量精度误差清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yqzxlgsdjljdwcqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yqzxlgsdjljdwcqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_yqzxlgsdjljdwcqd_his select getUUID(),*,tjsj fqrq from res_gk_yqzxlgsdjljdwcqd")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yqzxlgsdjljdwcqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_yqzxlgsdjljdwcqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yqzxlgsdjljdwcqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl31',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.1运行${(end-start)/1000}秒")


        //线路3.2月线路工单归档不及时用户电量波动异常清单     要改
        //2019年11月18日17:02:17 lixc 正式受理日期小于28号，工作单状态运行或者归档且归档时间大于28号
        //2019-12-16 t.ny = ${nowMonth} 改为 t.ny = ${lastMonth}
        start = System.currentTimeMillis()
        try{
            val xl_32 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    t.gddwbm,${nowMonth} tjsj,
                   |    ${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,g.gzdbh gdh,null gzdmc,
                   |    handleNumber(j.jfdl) bqdl,
                   |    handleNumber(j.scdl) sqdl,
                   |    getycgzbh(${Constant.XL_32}) ycgzbh,--// 2020/10/30 getycgzbh(${Constant.XL_32})不存在
                   |    getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(t.gddwbm)) dsj,getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(t.gddwbm) dqbm,${nybm} nybm
                   |from xlxd l
                   |inner join jldxx j on j.xlxdbs = l.xlxdbs and j.dfny = ${nowMonth} and j.jldytdm <> '410' and j.yhlbdm in ('10','11','60') and j.yhztdm <> '2' and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 0.5
                   |inner join xlxstjxx t on t.xlxdbs = j.xlxdbs and l.gddwbm = t.gddwbm and t.ny = ${lastMonth} and if(sign(t.bysdl)=1,j.scdl/t.bysdl,0) > 0.1
                   |inner join ykgzdjbxx g on g.yhbh = j.yhbh and g.ywlbbh in ('F-CSG-MK0821-02','F-CSG-MK0821-07')
                   |    and datediff(g.zsslrq,${gdfyrq}) < 0 and ((g.gzdztdm = '1' and g.gdsj is null) or (g.gzdztdm = '2' and datediff(g.gdsj,${gdfyrq}) >= 0))
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh  --计量点运行电能表关系
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs  --运行电能表
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |
                 """.stripMargin

            sparkSession.sql(xl_32).createOrReplaceTempView("res_gk_gdgdbjsyhdlbdycqd")
            sparkSession.sql("select * from res_gk_gdgdbjsyhdlbdycqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gdgdbjsyhdlbdycqd")

            //工单归档不及时用户电量波动异常清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gdgdbjsyhdlbdycqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gdgdbjsyhdlbdycqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GDGDBJSYHDLBDYCQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GDGDBJSYHDLBDYCQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gdgdbjsyhdlbdycqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_gdgdbjsyhdlbdycqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gdgdbjsyhdlbdycqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl32',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.2运行${(end-start)/1000}秒")

        //线路3.3月线路表码录入错误工单清单
        //2019年11月18日17:04:45 lixc 工作单状态归档且归档时间大于等于1号小于28号
        start = System.currentTimeMillis()
        try{
            val xl_33 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,g.gzdbh gdh,
                   |    handleNumber(c.scbss) qm,
                   |    handleNumber(c.bcbss) zm,
                   |    handleNumber(j.jfdl) bydl,
                   |    handleNumber(j.scdl) sydl,
                   |    getycgzbh(${Constant.XL_33}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx l  --线损异常(线路和台区)明细
                   |inner join jldxx j on j.xlxdbs = l.xlxdbs and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                   |inner join ykgzdjbxx g on g.yhbh = j.yhbh and g.ywlbbh in ('F-CSG-MK0821-02','F-CSG-MK0821-07')
                   |    and g.gzdztdm = '2' and datediff(g.gdsj,'${_nowMonth}-01') >= 0 and datediff(g.gdsj,${gdfyrq}) < 0
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh  --计量点运行电能表关系
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs --运行电能表
                   |inner join cbxx c on c.jldbh=j.jldbh and c.yxdnbbs = d.yxdnbbs and c.sslxdm = '121' and c.dfny = ${nowMonth}
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where l.xltqbz = '1' and l.ny = ${nowMonth}
                   |    and (j.yhlbdm in ('80','10','11','60') or (j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj = '08'))) and j.yhztdm <> '2'
                   |    and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 2
                   |
                 """.stripMargin

            sparkSession.sql(xl_33).createOrReplaceTempView("res_gk_bmlrcwgdqd")
            sparkSession.sql("select * from res_gk_bmlrcwgdqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_bmlrcwgdqd")

            //表码录入错误工单清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bmlrcwgdqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_bmlrcwgdqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_BMLRCWGDQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BMLRCWGDQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_bmlrcwgdqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_bmlrcwgdqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bmlrcwgdqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl33',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.3运行${(end-start)/1000}秒")

        //线路3.4 计量倒走工单用户电量波动异常    zngdh-智能工单号 未确定
        //修改后，需要数据源中添加_jlzdhgzrwxx
        start = System.currentTimeMillis()
        try{
            val xl_34 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,g.gzdbh zngdh,
                   |    handleNumber(j.scdl) sqdl,
                   |    handleNumber(j.jfdl) bqdl,getycgzbh(${Constant.XL_34}) ycgzbh,
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx l
                   |inner join jldxx j on j.xlxdbs = l.xlxdbs and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                   |inner join ykgzdjbxx g on g.yhbh = j.yhbh and g.ywlbbh = 'F-CSG-MK0821-09'
                   |    and g.gzdztdm = '2' and datediff(g.gdsj,'${_nowMonth}-01') >= 0 and datediff(g.gdsj,${gdfyrq}) < 0
                   |inner join jlzdhgzrwxx z on z.gzdbh = g.gzdbh and z.gzqkjs like '%倒走%'  --计量自动化故障任务信息
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where l.xltqbz = '1' and l.ny = ${nowMonth}
                   |    and (j.yhlbdm in ('80','10','11','60') or (j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj = '08'))) and j.yhztdm <> '2'
                   |    and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 0.5
                   |
                 """.stripMargin
            sparkSession.sql(xl_34).createOrReplaceTempView("RES_GK_JLDZGDYHDLBDYCQD")
            sparkSession.sql("select * from RES_GK_JLDZGDYHDLBDYCQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_JLDZGDYHDLBDYCQD")

            //计量倒走工单用户电量波动异常清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jldzgdyhdlbdycqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jldzgdyhdlbdycqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_jldzgdyhdlbdycqd_his select getUUID(),*,tjsj fqrq from res_gk_jldzgdyhdlbdycqd")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jldzgdyhdlbdycqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_jldzgdyhdlbdycqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jldzgdyhdlbdycqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl34',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.4运行${(end-start)/1000}秒")

        //线路3.5 计量飞走工单用户电量波动		  zngdh-智能工单号 未确定
        //修改后，需要数据源中添加_jlzdhgzrwxx
        start = System.currentTimeMillis()
        try{
            val xl_35 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,g.gzdbh zngdh,
                   |    handleNumber(j.scdl) sqdl,
                   |    handleNumber(j.jfdl) bqdl,
                   |    getycgzbh(${Constant.XL_35}) ycgzbh,
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx l
                   |inner join jldxx j on j.xlxdbs = l.xlxdbs and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                   |    and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 0.5
                   |inner join ykgzdjbxx g on g.yhbh = j.yhbh and g.ywlbbh = 'F-CSG-MK0821-12'
                   |    and g.gzdztdm = '2' and datediff(g.gdsj,'${_nowMonth}-01') >= 0 and datediff(g.gdsj,${gdfyrq}) < 0
                   |inner join jlzdhgzrwxx z on z.gzdbh = g.gzdbh and (z.gzqkjs like '%飞走%' or z.gzqkjs like '%失压%' or z.gzqkjs like '%失流%')
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where l.xltqbz = '1' and l.ny = ${nowMonth}
                   |    and (j.yhlbdm in ('80','10','11','60') or (j.yhlbdm = '40' and exists (select 1 from _dcfzxx b where b.dcbh = j.yhbh and b.dydj = '08'))) and j.yhztdm <> '2'
                   |    and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 0.5
                   |
                 """.stripMargin
            sparkSession.sql(xl_35).createOrReplaceTempView("res_gk_jlfzgdyhdlbdycqd")
            sparkSession.sql("select * from res_gk_jlfzgdyhdlbdycqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlfzgdyhdlbdycqd")

            //计量飞走工单用户电量波动异常清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlfzgdyhdlbdycqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_jlfzgdyhdlbdycqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_JLFZGDYHDLBDYCQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_JLFZGDYHDLBDYCQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_jlfzgdyhdlbdycqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_jlfzgdyhdlbdycqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_jlfzgdyhdlbdycqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl35',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.5运行${(end-start)/1000}秒")

        //线路3.6零电量波动率100%用户电量清单       bdl未确定
        start = System.currentTimeMillis()
        try{
            val xl_36 =
                s"""
                   |select
                   |    distinct ${creator_id} creator_id,${create_time} create_time,${update_time} update_time,${updator_id} updator_id,
                   |    xl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,
                   |    null tqbs,null tqbh,null tqmc,
                   |    getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null tqkhbhh,
                   |    j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,
                   |    handleNumber(j.scdl) sqjfdl,
                   |    handleNumber(j.jfdl) bqjfdl,
                   |    0 bdl,getycgzbh(${Constant.XL_36}) ycgzbh,
                   |    getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(xl.gddwbm) dqbm,${nybm} nybm
                   |from xsycxlhtqmx xl
                   |inner join jldxx j on j.xlxdbs = xl.xlxdbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |where xl.xltqbz = '1' and xl.ny = ${nowMonth}
                   |    and (j.yhlbdm in ('80','60','10','11') or (j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj = '08'))) and j.yhztdm <> '2'
                   |    and j.jfdl = 0 and j.scdl <> 0
                   |
                 """.stripMargin
            sparkSession.sql(xl_36).repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd")
            sparkSession.sql("select * from res_gk_bdlycyhdlqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd")

            //波动率异常用户电量清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bdlycyhdlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_bdlycyhdlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_BDLYCYHDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BDLYCYHDLQD")

//            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_bdlycyhdlqd")
//              .repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd_ycgddwxlgx")
//
//            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bdlycyhdlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl36',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.6运行${(end-start)/1000}秒")
        
    }

}
