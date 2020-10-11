package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Line4Service {

    def line4Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        start = System.currentTimeMillis()
        try{
            val xl_41 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},d.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,d.xlxdbs,getxlbh(d.xlxdbs),getxlmc(d.xlxdbs),
                   |    null tqbs,null tqbh,null tqmc,getbdzbs(d.xlxdbs) bdzbs,getbdzbh(d.xlxdbs) bdzbh,
                   |    getbdzmc(d.xlxdbs) bdzmc,k1.bdzkhbhh,null,d.yhbh,d.yhmc,d.yhlbdm,handleNumber(d.glys),
                   |    handleNumber(d.jfdl),getycgzbh(${Constant.XL_41}) ycgzbh,getDsjbm(d.gddwbm) dsjbm,
                   |    getQxjbm(d.gddwbm) qxjbm,getGdsbm(d.gddwbm) gdsbm,getzzmc(getDsjbm(d.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(d.gddwbm)) qxj,getzzmc(getGdsbm(d.gddwbm)) gds,
                   |    getdmbmmc('YHLBDM',d.yhlbdm) yhlb,getdqbm(d.gddwbm) dqbm,${nybm}
                   |from (
                   |    select t.xlxdbs
                   |    from _jldxx t
                   |    where t.dfny = ${nowMonth} and t.yhlbdm = '80' and t.ygzdl <> 0 and t.wgzdl <> 0
                   |        and ygzdl/sqrt(t.ygzdl*t.ygzdl+t.wgzdl*t.wgzdl) < 0.8) a
                   |join (
                   |    select
                   |        t2.xlxdbs,count(t2.yhbh) gbsl
                   |    from (
                   |        select
                   |            t1.xlxdbs,t1.yhbh,t1.ygzdl/sqrt(t1.ygzdl*t1.ygzdl+t1.wgzdl*t1.wgzdl) glys
                   |        from (
                   |            select
                   |                t.xlxdbs,t.yhbh,sum(t.ygzdl) ygzdl,sum(t.wgzdl) wgzdl
                   |            from _jldxx t
                   |            where t.dfny = ${nowMonth} and t.yhlbdm = '60'
                   |            group by t.xlxdbs,t.yhbh
                   |            ) t1 where (t1.ygzdl <> 0 or t1.wgzdl <> 0)
                   |        ) t2 where t2.glys < 0.8 group by t2.xlxdbs
                   |    ) b
                   |on b.xlxdbs = a.xlxdbs
                   |join (
                   |    select
                   |        t.xlxdbs,count(distinct t.yhbh) byqsl
                   |    from _jldxx t
                   |    where t.dfny = ${nowMonth} and t.yhlbdm in ('10','11','60')
                   |    group by t.xlxdbs
                   |    ) c on c.xlxdbs = b.xlxdbs
                   |join (
                   |    select t2.xlxdbs,t2.yhbh,t2.yhmc,t2.yhlbdm,t2.glys,t2.jfdl,t2.gddwbm
                   |    from (
                   |        select t1.xlxdbs,t1.yhbh,t1.yhmc,t1.yhlbdm,t1.ygzdl/sqrt(t1.ygzdl*t1.ygzdl+t1.wgzdl*t1.wgzdl) glys,t1.jfdl,t1.gddwbm
                   |        from (
                   |            select
                   |                t.xlxdbs,t.yhbh,t.yhmc,t.yhlbdm,sum(t.ygzdl) ygzdl,sum(t.wgzdl) wgzdl,sum(t.jfdl) jfdl,t.gddwbm
                   |            from _jldxx t join _xlxd l on t.xlxdbs = l.xlxdbs
                   |            where t.dfny = ${nowMonth} and t.yhlbdm = '60'
                   |            group by t.xlxdbs,t.yhbh,t.yhmc,t.yhlbdm,t.gddwbm
                   |            ) t1
                   |        where (t1.ygzdl <> 0 or t1.wgzdl <> 0)
                   |        ) t2 where t2.glys < 0.8
                   |    ) d on d.xlxdbs = c.xlxdbs
                   |lateral view outer explode(split(getbdzkhb(d.xlxdbs),',')) k1 as bdzkhbhh
                   |where if(sign(c.byqsl)=1,b.gbsl/c.byqsl,0) > 0.6
                   |
                 """.stripMargin
            sparkSession.sql(xl_41).createOrReplaceTempView("res_gk_glyspdzgbqd")
            sparkSession.sql("select * from res_gk_glyspdzgbqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_glyspdzgbqd")

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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl41',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.1运行${(end-start)/1000}秒")

        //线路4.5 高损公用变压器信息
        start = System.currentTimeMillis()
        try{
            val xl_45 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},xs.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,xs.xlxdbs,xs.xlbh,xs.xlmc,
                   |    null,null,null,getbdzbs(xs.xlxdbs) bdzbs,getbdzbh(xs.xlxdbs) bdzbh,getbdzmc(xs.xlxdbs) bdzmc,
                   |    k1.bdzkhbhh,null,y.sbbs byqbh,y.mc byqmc,y.gisid byqgisid,y.sbxhdm,y.yxztdm,
                   |    getycgzbh(${Constant.XL_45}) ycgzbh,getDsjbm(xs.gddwbm) dsjbm,
                   |    getQxjbm(xs.gddwbm) qxjbm,getGdsbm(xs.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(xs.gddwbm)) dsj,getzzmc(getQxjbm(xs.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(xs.gddwbm)) gds,getdmbmmc('BYQXHDM',y.sbxhdm) sbxh,
                   |    getdmbmmc('BYQYXZTDM',y.yxztdm) yhzt,getdqbm(xs.gddwbm) dqbm,${nybm}
                   |from _xlxstjxx xs
                   |inner join _xltqgx x on x.xlxdbs = xs.xlxdbs  --线路台区关系
                   |inner join _yxbyq y on y.tqbs = x.tqbs and y.shlxdm in ('01','02','03') and y.gbzbbz = '1' --运行变压器
                   |inner join (
                   |            select j.xlxdbs,sum(j.jfdl) gbzdl
                   |            from _jldxx j
                   |            inner join _yxbyq b on b.tqbs = j.tqbs and b.shlxdm in ('01','02','03') and b.gbzbbz = '1'
                   |            where j.jldytdm <> '410' and j.dfny = ${nowMonth}
                   |            group by j.xlxdbs
                   |            ) a
                   |on a.xlxdbs = xs.xlxdbs and if(sign(xs.bysdl)=1,a.gbzdl/xs.bysdl,0)>0.5
                   |lateral view outer explode(split(getbdzkhb(xs.xlxdbs),',')) k1 as bdzkhbhh
                   |where xs.ny = ${nowMonth}
                   |
                 """.stripMargin
            sparkSession.sql(xl_45).createOrReplaceTempView("res_gk_gsgybyqqd")
            sparkSession.sql("select * from res_gk_gsgybyqqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gsgybyqqd")

            //高损公用变压器清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gsgybyqqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gsgybyqqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GSGYBYQQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GSGYBYQQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gsgybyqqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gsgybyqqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gsgybyqqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl45',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.5运行${(end-start)/1000}秒")

        //线路4.6 负荷切割工单
        start = System.currentTimeMillis()
        try{
            val xl_46 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,null,null,null,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,
                   |    null,m.code fhqggzdh,m.apply_time qdsj,getycgzbh(${Constant.XL_46}) ycgzbh,
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from _xlxd l
                   |join _nzwot_sp_so_runmode_device d on (d.rolloff_line_id = l.gisid or d.rollon_line_id = l.gisid)
                   |join _nzwot_sp_so_runmode_manage m on m.id = d.mode_id
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (getYearMonth(d.execute_begin_time) = ${nowMonth} or getYearMonth(d.execute_end_time) = ${nowMonth})
                   |    and not exists (select 1 from _xlbzxx h where h.xlxdbs = l.xlxdbs and h.bzny = ${nowMonth})
                   |
                 """.stripMargin
            sparkSession.sql(xl_46).repartition(resultPartition).createOrReplaceTempView("res_gk_fhqggzdqd")
            sparkSession.sql("select * from res_gk_fhqggzdqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_fhqggzdqd")

            //负荷切割工作单清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_fhqggzdqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_fhqggzdqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_FHQGGZDQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_FHQGGZDQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_fhqggzdqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_fhqggzdqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_fhqggzdqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl46',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime")
          .createOrReplaceTempView("ruleState")
        println(s"规则4.6运行${(end-start)/1000}秒")

        //线路4.7 公变变损信息      这里的变压器不用join，直接从_xlxstjxx可以拿
        //2019年11月20日17:42:27 lixc 添加关联变压器月固定损耗
        start = System.currentTimeMillis()
        try{
            val xl_47 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,null,null,null,
                   |    l.bdzbs,l.bdzbh,l.bdzmc,k1.bdzkhbhh,null,y.sbbs,y.mc,b.zxygbs shdl,
                   |    getycgzbh(${Constant.XL_47}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,
                   |    getGdsbm(l.gddwbm) gdsbm,getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from _xlxstjxx l
                   |inner join _xltqgx x on x.xlxdbs = l.xlxdbs
                   |inner join _yxbyq y on y.tqbs = x.tqbs and y.gbzbbz = '1' and y.yxztdm = '01'
                   |inner join _byqygdsh b on b.sbbs = y.sbbs  --变压器月固定损耗
                   |inner join (
                   |            select
                   |                l.xlxdbs,sum(coalesce(b.zxygbs,0)) gdsh
                   |            from _xlxstjxx l
                   |            inner join _xltqgx x on x.xlxdbs = l.xlxdbs
                   |            inner join _yxbyq y on y.tqbs = x.tqbs and y.gbzbbz = '1' and y.yxztdm = '01'
                   |            inner join _byqygdsh b on b.sbbs = y.sbbs
                   |            where l.ny = ${nowMonth}
                   |            group by l.xlxdbs
                   |            ) a
                   |on a.xlxdbs = l.xlxdbs and if(sign(l.bygdl-l.bysdl)=1,a.gdsh/(l.bygdl-l.bysdl),0) > 0.5
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where l.ny = ${nowMonth}
                   |
                 """.stripMargin
            sparkSession.sql(xl_47).createOrReplaceTempView("RES_GK_GBBSQD")
            sparkSession.sql("select * from RES_GK_GBBSQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_GBBSQD")

            //公变变损清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gbbsqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_gbbsqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_GBBSQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_GBBSQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_gbbsqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_gbbsqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_gbbsqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl47',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.7运行${(end-start)/1000}秒")

        //线路4.9 同组合的线路阈值信息     这里的变压器不用join，直接从_xlxstjxx可以拿到
        start = System.currentTimeMillis()
        try{
            val xl_49 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},x.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,b.xlxdbs,b.xlbh,b.xlmc,null,null,null,
                   |    x.bdzbs,x.bdzbh,x.bdzmc,k1.bdzkhbhh,null,b.xlbzbh,handleNumber(x.byxsl) zhhxsl,
                   |    handleNumber(x.khzb) xszb,getycgzbh(${Constant.XL_49}) ycgzbh,getDsjbm(x.gddwbm) dsjbm,
                   |    getQxjbm(x.gddwbm) qxjbm,getGdsbm(x.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(x.gddwbm)) dsj,getzzmc(getQxjbm(x.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(x.gddwbm)) gds,getdqbm(x.gddwbm) dqbm,${nybm}
                   |from xlbzxx b  --线路编组信息
                   |inner join xlxstjxx x on x.xlxdbs = b.xlxdbs and x.ny = ${nowMonth}
                   |inner join (
                   |            select b.xlbzbh,max(x.khzb) zdkhzb,min(x.khzb) zxkhzb
                   |            from xlbzxx b
                   |            inner join xlxstjxx x on x.xlxdbs = b.xlxdbs and x.ny = ${nowMonth}
                   |            group by b.xlbzbh
                   |            ) z on z.xlbzbh = b.xlbzbh
                   |lateral view outer explode(split(getbdzkhb(b.xlxdbs),',')) k1 as bdzkhbhh
                   |where x.byxsl < z.zdkhzb and x.byxsl > z.zxkhzb
                   |    and b.bzny = ${nowMonth}
                   |order by b.xlbzbh
                """.stripMargin
            sparkSession.sql(xl_49).createOrReplaceTempView("RES_GK_ZHXLFZQD")
            sparkSession.sql("select * from RES_GK_ZHXLFZQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_ZHXLFZQD")

            //组合线路阈值清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_zhxlfzqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_zhxlfzqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_ZHXLFZQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_ZHXLFZQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_zhxlfzqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_zhxlfzqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_zhxlfzqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl49',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.9运行${(end-start)/1000}秒")

        //线路4.10  gk_sjhwwzhxlqd  环网开始时间和环网结束时间是取的资产系统的，资产管理系统环网时间、资产管理系统工单编号这两个找不到
        start = System.currentTimeMillis()
        try{
            val xl_410 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},l.gddwbm,
                   |    ${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,null,null,null,
                   |    getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,k1.bdzkhbhh,
                   |    null,l.xlxdbs,l.xlbh,l.xlmc,d.execute_begin_time hwkssj,d.execute_end_time hwjssj,
                   |    null zcglxthwsj,m.code zcglxtgdbh,null zcglxtgdmc,getycgzbh(${Constant.XL_410}) ycgzbh,
                   |    getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(l.gddwbm)) gds,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from xlxd l
                   |join nzwot_sp_so_runmode_device d on (d.rolloff_line_id = l.gisid or d.rollon_line_id = l.gisid)
                   |join nzwot_sp_so_runmode_manage m on m.id = d.mode_id
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where (getYearMonth(d.execute_begin_time) = ${nowMonth} or getYearMonth(d.execute_end_time) = ${nowMonth})
                   |    and not exists (select 1 from xlbzxx h where h.xlxdbs = l.xlxdbs and h.bzny = ${nowMonth})
                   |
                 """.stripMargin
            sparkSession.sql(xl_410).repartition(resultPartition).createOrReplaceTempView("res_gk_sjhwwzhxlqd")
            sparkSession.sql("select * from res_gk_sjhwwzhxlqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_sjhwwzhxlqd")

            //实际环网未组合线路清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_sjhwwzhxlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_sjhwwzhxlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_SJHWWZHXLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_SJHWWZHXLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_sjhwwzhxlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_sjhwwzhxlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj from res_gk_sjhwwzhxlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'xl410',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则4.10运行${(end-start)/1000}秒")
    }

}
