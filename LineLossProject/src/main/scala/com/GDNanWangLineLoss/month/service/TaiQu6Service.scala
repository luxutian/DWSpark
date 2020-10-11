package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.GDNanWangLineLoss.month.bean.Variables._

object TaiQu6Service {

    def taiqu6Service(sparkSession: SparkSession,url:String)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        //台区6.1线损指标为空
        start = System.currentTimeMillis()
        try{
            val tq_61 =
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                   |    t.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${tqbz} xltqbz,t.xlxdbs,t.xlbh,
                   |    t.xlmc,t.tqbs,t.tqbh,t.tqmc,t.bdzbs,t.bdzbh,t.bdzmc,k1.bdzkhbhh,k2.tqkhbhh,
                   |    r.xszrrbs,r.rymc xszrr,getycgzbh(${Constant.TQ_61}) ycgzbh,getDsjbm(t.gddwbm) dsjbm,
                   |    getQxjbm(t.gddwbm) qxjbm,getGdsbm(t.gddwbm) gdsbm,getzzmc(getDsjbm(t.gddwbm)) dsj,
                   |    getzzmc(getQxjbm(t.gddwbm)) qxj,getzzmc(getGdsbm(t.gddwbm)) gds,getdqbm(t.gddwbm) dqbm,${nybm}
                   |from xlhtqzb t  --线路和台区指标
                   |join tq q on q.tqbs = t.tqbs
                   |left join xszrr r on r.xszrrbs = t.xszrrbs  --线损责任人
                   |lateral view outer explode(split(getbdzkhb(t.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(t.tqbs),',')) k2 as tqkhbhh
                   |where t.ny = ${year} and t.xltqbz = ${tqbz}
                   |    and (t.khzb is null or t.khzbxx is null)
                   |
                 """.stripMargin
            sparkSession.sql(tq_61).createOrReplaceTempView("res_gk_xszbwkxlqd")
            sparkSession.sql("select * from res_gk_xszbwkxlqd where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_xszbwkxlqd")

            //线损指标为空线路清单
            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_xszbwkxlqd")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_xszbwkxlqd"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.GK_XSZBWKXLQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_XSZBWKXLQD")

            sparkSession.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from res_gk_xszbwkxlqd")
              .repartition(resultPartition).createOrReplaceTempView("res_gk_xszbwkxlqd_ycgddwxlgx")

            sparkSession.sql(s"select getUUID(),*,tjsj fqrq from res_gk_xszbwkxlqd_ycgddwxlgx")
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
        sparkSession.sql(s"select * from ruleState union all select getFormatDate(${end}) recordtime,'tq61',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则6.1运行${(end-start)/1000}秒")
    }

}
