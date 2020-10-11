package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._

/**
  * 台区3
  */
object TaiQu3Service {

    def taiqu3Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        //台区3.1  gk_yqzxlgsdjljdwcqd
        start = System.currentTimeMillis()
        try{
            val tq_31 =
                s"""
                   |select
                   |    distinct ${creator_id} ,${create_time},${update_time},${updator_id},t.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,t.xlbh,t.xlmc,t.tqbs,t.tqbh,t.tqmc,
                   |    getbdzbs(t.xlxdbs) bdzbs,getbdzbh(t.xlxdbs) bdzbh,getbdzmc(t.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,round(t.bygdl/(b.edrl*24*getdayOfMonth()),2) fzl,t.byxsl,t.bygdl,t.bysdl,
                   |    getycgzbh(${Constant.TQ_31}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,getQxjbm(t.gddwbm) qxjbm,
                   |    getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from tqxstjxx t  --台区线损统计信息
                   |join yxbyq b on b.tqbs = t.tqbs  --运行变压器
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where t.ny = ${nowMonth} and t.bygdl/(b.edrl*24*getdayOfMonth()) < 0.1 and t.byxsl < 0
                   |
                 """.stripMargin
            sparkSession.sql(tq_31).createOrReplaceTempView("res_gk_yqzxlgsdjljdwcqd")
            sparkSession.sql("select * from res_gk_yqzxlgsdjljdwcqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_yqzxlgsdjljdwcqd")

            //供售端计量精度误差清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yqzxlgsdjljdwcqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_yqzxlgsdjljdwcqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_yqzxlgsdjljdwcqd_his select getUUID(),*,tjsj fqrq from res_gk_yqzxlgsdjljdwcqd")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_yqzxlgsdjljdwcqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_yqzxlgsdjljdwcqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_yqzxlgsdjljdwcqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq31',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.1运行${(end-start)/1000}秒")

        //台区3.2 月线路工单归档不及时用户电量波动异常清单
        //2019年11月12日16:38:44 lixc 关联gpdmzq.npmis_fw_ykgzdjbxx     11.18已改
        //2019年11月18日17:02:17 lixc 正式受理日期小于28号，工作单状态运行或者归档且归档时间大于28号
        //2019-12-16 t.ny = ${nowMonth} 改为 t.ny = ${lastMonth}
        start = System.currentTimeMillis()
        try{
            val tq_32 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,j.xlxdbs,getxlbh(j.xlxdbs),
                   |    getxlmc(j.xlxdbs),l.tqbs,l.tqbh,l.tqmc,getbdzbs(j.xlxdbs) bdzbs,
                   |    getbdzbh(j.xlxdbs) bdzbh,getbdzmc(j.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,g.gzdbh gdh,
                   |    null gzdmc,handleNumber(j.jfdl) bqdl,handleNumber(j.scdl) sqdl,
                   |    getycgzbh(${Constant.TQ_32}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,
                   |    getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from tq l
                   |inner join jldxx j on j.tqbs = l.tqbs and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                   |    and j.yhlbdm = '20' and j.yhztdm <> '2' and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 0.5
                   |inner join tqxstjxx t on t.tqbs = j.tqbs and t.ny = ${lastMonth} and if(sign(t.bysdl)=1,j.scdl/t.bysdl,0) > 0.1
                   |inner join ykgzdjbxx g on g.yhbh = j.yhbh and g.ywlbbh in ('F-CSG-MK0821-02','F-CSG-MK0821-07')
                   |    and datediff(g.zsslrq,${gdfyrq}) < 0 and ((g.gzdztdm = '1' and g.gdsj is null) or (g.gzdztdm = '2' and datediff(g.gdsj,${gdfyrq}) >= 0))
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh  --计量点运行电能表关系
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs  --运行电能表
                   |lateral view outer explode(split(getbdzkhb(j.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(l.tqbs),',')) k2 as tqkhbhh
                   |
                 """.stripMargin
            sparkSession.sql(tq_32).createOrReplaceTempView("res_gk_gdgdbjsyhdlbdycqd")
            sparkSession.sql("select * from res_gk_gdgdbjsyhdlbdycqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gdgdbjsyhdlbdycqd")

            //工单归档不及时用户电量波动异常清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gdgdbjsyhdlbdycqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gdgdbjsyhdlbdycqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GDGDBJSYHDLBDYCQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GDGDBJSYHDLBDYCQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gdgdbjsyhdlbdycqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gdgdbjsyhdlbdycqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gdgdbjsyhdlbdycqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq32',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.2运行${(end-start)/1000}秒")

        //台区3.3 月台区表码录入错误工单清单
        //2019年11月12日16:38:44 lixc 关联gpdmzq.npmis_fw_ykgzdjbxx    11.18已改
        //2019年11月18日17:04:45 lixc 工作单状态归档且归档时间大于等于1号小于28号
        start = System.currentTimeMillis()
        try{
            val tq_33 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,l.tqbs,l.tqbh,
                   |    l.tqmc,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,g.gzdbh gdh,
                   |    handleNumber(c.scbss) qm,handleNumber(c.bcbss) zm,handleNumber(j.jfdl) bydl,
                   |    handleNumber(j.scdl) sydl,getycgzbh(${Constant.TQ_33}) ycgzbh,
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx l  --线损异常(线路和台区)明细
                   |inner join jldxx j on j.tqbs = l.tqbs and j.dfny = ${nowMonth} and j.jldytdm <> '410'
                   |inner join ykgzdjbxx g on g.yhbh = j.yhbh and g.ywlbbh in ('F-CSG-MK0821-02','F-CSG-MK0821-07')  --业扩工作单基本信息
                   |    and g.gzdztdm = '2' and datediff(g.gdsj,'${_nowMonth}-01') >= 0 and datediff(g.gdsj,${gdfyrq}) < 0
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh  --计量点运行电能表关系
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs
                   |inner join cbxx c on c.jldbh=j.jldbh and c.yxdnbbs = d.yxdnbbs and c.sslxdm = '121' and c.dfny = ${nowMonth}
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(l.tqbs),',')) k2 as tqkhbhh
                   |where l.xltqbz = '2' and l.ny = ${nowMonth}
                   |    and (j.yhlbdm in ('20','60') or (j.yhlbdm = '40' and exists (select 1 from dcfzxx b where b.dcbh = j.yhbh and b.dydj in ('02','03')))) and j.yhztdm <> '2'
                   |    and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)> 2
                   |
                 """.stripMargin
            sparkSession.sql(tq_33).createOrReplaceTempView("res_gk_bmlrcwgdqd")
            sparkSession.sql("select * from res_gk_bmlrcwgdqd where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("res_gk_bmlrcwgdqd")

            //表码录入错误工单清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bmlrcwgdqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_bmlrcwgdqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_BMLRCWGDQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BMLRCWGDQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_bmlrcwgdqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bmlrcwgdqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bmlrcwgdqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq33',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.3运行${(end-start)/1000}秒")

        //台区3.4 月电量波动率超过30%台区考核表清单
        start = System.currentTimeMillis()
        try{
            val tq_34 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,xl.tqbh,
                   |    xl.tqmc,getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,handleNumber(j.scdl) sqjfdl,
                   |    handleNumber(j.jfdl) bqjfdl,handleNumber(if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)) bdl,
                   |    getycgzbh(${Constant.TQ_34}) ycgzbh,getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,
                   |    getGdsbm(xl.gddwbm) gdsbm,getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,getdqbm(xl.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx xl
                   |inner join jldxx j on j.tqbs = xl.tqbs and j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |        and j.yhlbdm = '60' and j.yhztdm <> '2' and if(sign(j.scdl)=1,(j.jfdl-j.scdl)/j.scdl,0)>0.3
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '2' and xl.ny = ${nowMonth} and xl.byxsl < 0
                   |
                 """.stripMargin
            sparkSession.sql(tq_34).createOrReplaceTempView("res_gk_bdlycyhdlqd")
            sparkSession.sql("select * from res_gk_bdlycyhdlqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd")

            //波动率异常用户电量清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bdlycyhdlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_bdlycyhdlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_BDLYCYHDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BDLYCYHDLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_bdlycyhdlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bdlycyhdlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq34',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.4运行${(end-start)/1000}秒")

        //台区3.5 月电量波动率100%公变客户清单  todo 波动率100%应当为1，下面确写0   bdl波动率
        //2019年11月12日16:38:44 lixc j.jfdl = 0 and j.scdl <> 0 
        start = System.currentTimeMillis()
        try{
            val tq_35 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xl.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,xl.tqbh,
                   |    xl.tqmc,getbdzbs(xl.xlxdbs) bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,k2.tqkhbhh,j.yhbh,j.yhmc,j.yhlbdm,j.jldbh,d.zcbh,handleNumber(j.scdl) sqjfdl,
                   |    handleNumber(j.jfdl) bqjfdl,0 bdl,getycgzbh(${Constant.TQ_35}) ycgzbh,
                   |    getDsjbm(xl.gddwbm) dsjbm,getQxjbm(xl.gddwbm) qxjbm,getGdsbm(xl.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xl.gddwbm)) dsj,getzzmc(getQxjbm(xl.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xl.gddwbm)) gds,getdmbmmc('YHLBDM',j.yhlbdm) yhlb,
                   |    getdqbm(xl.gddwbm) dqbm,${nybm}
                   |from xsycxlhtqmx xl
                   |inner join jldxx j on j.tqbs = xl.tqbs and j.jldytdm <> '410'
                   |    and j.dfny = ${nowMonth} and j.yhlbdm = '20' and j.yhztdm <> '2' and j.jfdl = 0 and j.scdl <> 0
                   |inner join jlddnbgx jd on jd.jldbh = j.jldbh
                   |inner join yxdnb d on d.yxdnbbs = jd.yxdnbbs
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = '2' and xl.ny = ${nowMonth}
                   |
                 """.stripMargin
            sparkSession.sql(tq_35).createOrReplaceTempView("res_gk_bdlycyhdlqd")
            sparkSession.sql("select * from res_gk_bdlycyhdlqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd")

            //波动率异常用户电量清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bdlycyhdlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_bdlycyhdlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_BDLYCYHDLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_BDLYCYHDLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_bdlycyhdlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_bdlycyhdlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_bdlycyhdlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq35',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则3.5运行${(end-start)/1000}秒")
    }

}
