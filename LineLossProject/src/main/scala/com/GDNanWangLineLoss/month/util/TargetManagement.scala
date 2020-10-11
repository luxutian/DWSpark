package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import com.GDNanWangLineLoss.month.bean.Variables._

import scala.collection.mutable.Map

/**
  * 指标管理
  */
object TargetManagement {
    def targetManagement(sparkSession:SparkSession,url:String)={
//        val xsgddwtjType= StructType(List[StructField](
//            StructField("gddwbm", StringType, true),
//            StructField("ny", StringType, true),
//            StructField("sl", LongType, true)
//        ))

        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        //做临时表表的schema
        val xsgddwtjType=StructType(List[StructField](
            StructField("gddwbm",StringType,true),
            StructField("ny",StringType,true),
            StructField("sl",LongType,true)
        ))

        start = System.currentTimeMillis()
        try{
            //线路指标管理异常总数
            val xlzbglyczs =
                s"""
                   |select
                   |    concat_ws('_',y.gddwbm,y.ny) gddwbm_rq,count(distinct y.xlxdbs) sl
                   |from xsycxlhtqmx y  --线损异常(线路和台区)明细
                   |where y.xltqbz = ${xlbz}
                   |group by y.gddwbm,y.ny
                   |
                 """.stripMargin

            val xlzbglyczsArray= sparkSession.sql(xlzbglyczs).collect()
            val xlzbglyczsMap = Map[String,Map[String,Long]]()
            DependentStatistics.updateSpeMap(xlzbglyczsArray,xlzbglyczsMap ,"gddwbm_rq","sl")

            val xlzbglyczsIterable= xlzbglyczsMap.map(data=> (data._1,data._2.get("sl").getOrElse(0l)))
            val xlzbglyczsRDD= sparkSession.sparkContext.parallelize(xlzbglyczsIterable.toSeq).map(data => {
                val arr = data._1.split("_")
                Row(arr(0),arr(1),data._2)
            })
            sparkSession.createDataFrame(xlzbglyczsRDD,xsgddwtjType).createOrReplaceTempView("xlzbglyczs")
            // spark.sql("select * from xlzbglyczs where gddwbm = '0312' order by ny desc").show(100)


            //台区指标管理异常总数
            val tqzbglyczs =
                s"""
                   |select
                   |    concat_ws('_',y.gddwbm,y.ny) gddwbm_rq,count(distinct y.tqbs) sl
                   |from xsycxlhtqmx y
                   |where y.xltqbz = ${tqbz}
                   |group by y.gddwbm,y.ny
                   |
                 """.stripMargin

            val tqzbglyczsArray:Array[Row] = sparkSession.sql(tqzbglyczs).collect()
            val tqzbglyczsMap = Map[String,Map[String,Long]]()
            DependentStatistics.updateSpeMap(tqzbglyczsArray,tqzbglyczsMap ,"gddwbm_rq","sl")

            val tqzbglyczsIterable= tqzbglyczsMap.map(data=> (data._1,data._2.get("sl").getOrElse(0l)))
            val tqzbglyczsRDD= sparkSession.sparkContext.parallelize(tqzbglyczsIterable.toSeq).map(data => {
                val arr = data._1.split("_")
                Row(arr(0),arr(1),data._2)
            })
            sparkSession.createDataFrame(tqzbglyczsRDD,xsgddwtjType).createOrReplaceTempView("tqzbglyczs")

            //总数，线路指标管理异常总数，线路指标管理异常率
            sparkSession.sql(
                s"""
                   |select
                   |    y.ny,z.gddwbm,y.sl yczs,z.sl zs,
                   |    (case when (z.sl=0 or z.sl is null) then 0 else (ifnull(y.sl,0)/z.sl)*100 end) ycl,
                   |    ${xlbz} xltqbz
                   |from xlzbglyczs y
                   |join xlzs z on y.gddwbm = z.gddwbm
                   |
                   |union
                   |select
                   |    y.ny,z.gddwbm,y.sl yczs,z.sl zs,
                   |    (case when (z.sl=0 or z.sl is null) then 0 else (ifnull(y.sl,0)/z.sl)*100 end) ycl,
                   |    ${tqbz} xltqbz
                   |from tqzbglyczs y
                   |join tqzs z on y.gddwbm = z.gddwbm
                   |
                 """.stripMargin
              ).createOrReplaceTempView("zbgl_zs_yczs_ycl")


            sparkSession.sql(
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},
                   |    zNow.gddwbm,${nowMonth} tjrq,${ybs} tjzq,zNow.zs bqxlzs,zNow.yczs bqycxlzs,
                   |    zNow.ycl bqfxlzycl,handleNumber(zLast.zs) sqxlzs,handleNumber(zLast.yczs) sqycxlzs,
                   |    handleNumber(zLast.ycl) sqfxlzycl,handleNumber(null),handleNumber(null),
                   |    getDsjbm(zNow.gddwbm) dsjbm,getQxjbm(zNow.gddwbm) qxjbm,getGdsbm(zNow.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(zNow.gddwbm)) dsj,getzzmc(getQxjbm(zNow.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(zNow.gddwbm)) gds
                   |from zbgl_zs_yczs_ycl zNow
                   |left join zbgl_zs_yczs_ycl zLast
                   |    on zNow.gddwbm = zLast.gddwbm and  zLast.ny = ${lastMonth} and zLast.xltqbz = zNow.xltqbz
                   |where zNow.xltqbz = ${xlbz} and zNow.ny = ${nowMonth}
                   |
                 """.stripMargin
              ).createOrReplaceTempView("xlzbgl")
            sparkSession.sql("select * from xlzbgl where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("xlzbgl")

            //线路指标管理
            sparkSession.sql(s"select getUUID(),*,tjrq fqrq from xlzbgl")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_xlzbgl"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_xlzbgl_gd partition(fqrq) select getUUID(),*,tjrq fqrq from xlzbgl")

            sparkSession.sql(
                s"""
                   |select
                   |    distinct ${creator_id},${create_time},${update_time},${updator_id},zNow.gddwbm,
                   |    ${nowMonth} tjrq,${ybs} tjzq,zNow.zs bqxlzs,zNow.yczs bqycxlzs,zNow.ycl bqfxlzycl,
                   |    handleNumber(zLast.zs) sqxlzs,handleNumber(zLast.yczs) sqycxlzs,
                   |    handleNumber(zLast.ycl) sqfxlzycl,handleNumber(null),handleNumber(null),
                   |    getDsjbm(zNow.gddwbm) dsjbm,getQxjbm(zNow.gddwbm) qxjbm,getGdsbm(zNow.gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(zNow.gddwbm)) dsj,getzzmc(getQxjbm(zNow.gddwbm)) qxj,
                   |    getzzmc(getGdsbm(zNow.gddwbm)) gds
                   |from zbgl_zs_yczs_ycl zNow
                   |left join zbgl_zs_yczs_ycl zLast
                   |    on zNow.gddwbm = zLast.gddwbm and zNow.xltqbz = zLast.xltqbz and zLast.ny = ${lastMonth}
                   |where zNow.xltqbz = ${tqbz} and zNow.ny = ${nowMonth}
                   |
                 """.stripMargin
              ).createOrReplaceTempView("tqzbgl")

            sparkSession.sql("select * from tqzbgl where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition)
              .createOrReplaceTempView("tqzbgl")

            //台区指标管理
            sparkSession.sql(s"select getUUID(),*,tjrq fqrq from tqzbgl")
              .write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_tqzbgl"))
              .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
//            sparkSession.sql(s"insert into ${writeSchema}.gk_tqzbgl_HIS partition(fqrq) select getUUID(),*,tjrq fqrq from tqzbgl")
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
               |select
               |    getFormatDate(${end}) recordtime,'指标管理',${isSuccess} state,
               |    '${reason}' reason,${(end-start)/1000} runtime
               |
             """.stripMargin).createOrReplaceTempView("ruleState")
        println(s"指标管理运行${(end-start)/1000}秒")

    }

}
