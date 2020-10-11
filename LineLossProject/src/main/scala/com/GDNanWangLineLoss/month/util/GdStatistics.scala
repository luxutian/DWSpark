package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable.Map
import com.GDNanWangLineLoss.month.bean.Variables._

/**
  * 工单统计
  */
object GdStatistics {

    def gdStatistics(sparkSession:SparkSession,url:String)={


//        spark.sql("use yxxt")
        val gdStructType = StructType(List[StructField](
            StructField("gddwbm", StringType, true),
            StructField("ny", StringType, true),
            StructField("ywlbbh", StringType, true),
            StructField("sl", LongType, true)
        ))

        val gdfqs = // TODO: 表格有问题需要改  这个临时表没有发现在数据源
            s"""
               |select
               |    concat(x.gddwbm,'_',x.ny,'_',g.ywlb) gddwbm,count(1) sl
               |from gk_xlycfx x  --线路异常分析
               |join xt_gzdxxls g on x.gzdbh = g.gzdbh  --工作单信息历史
               |where g.ywlb in ('F-CSG-MK154-01','F-CSG-MK154-02')
               |group by x.ny,x.gddwbm,g.ywlb
               |
             """.stripMargin
        val gdfqsArray= sparkSession.sql(gdfqs).collect()
        val gdfqsMap= Map[String,Map[String,Long]]()
        DependentStatistics.updateSpeMap(gdfqsArray,gdfqsMap,"gddwbm","sl")

        val gdfqsIterable= gdfqsMap.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val gdfqsRDD= sparkSession.sparkContext.parallelize(gdfqsIterable.toSeq).map(data=>(Row(data._1.split("_")(0),data._1.split("_")(1),data._1.split("_")(2),data._2)))

        //gk_xlycfx join xt_gzdxxls -->gdfqs
        sparkSession.createDataFrame(gdfqsRDD,gdStructType).createOrReplaceTempView("gdfqs")
        sparkSession.sql("select * from gdfqs limit 10").show

        val gdgds = // TODO: 表格有问题需要改
            s"""
               |select
               |    concat(x.gddwbm,'_',x.ny,'_',g.ywlb) gddwbm,count(1) sl
               |from gk_xlycfx x
               |join xt_gzdxxls g on x.gzdbh = g.gzdbh
               |where x.gzdzt = '2' and g.ywlb in ('F-CSG-MK154-01','F-CSG-MK154-02')
               |group by x.ny,x.gddwbm,g.ywlb
             """.stripMargin
        val gdgdsArray:Array[Row] = sparkSession.sql(gdgds).collect()
        val gdgdsMap = Map[String,Map[String,Long]]()
        DependentStatistics.updateSpeMap(gdgdsArray,gdgdsMap,"gddwbm","sl")
        val gdgdsIterable= gdgdsMap.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val gdgdsRDD= sparkSession.sparkContext.parallelize(gdgdsIterable.toSeq).map(data=>(Row(data._1.split("_")(0),data._1.split("_")(1),data._1.split("_")(2),data._2)))

        // gk_xlycfx  join  xt_gzdxxls-->gdgds
        sparkSession.createDataFrame(gdgdsRDD,gdStructType).createOrReplaceTempView("gdgds")
        sparkSession.sql("select * from gdgds limit 10").show

        // TODO: 有输出表格
        sparkSession.sql(
            s"""
               |select
               |    getUUID(),${creator_id},${create_time},${create_time},${updator_id},
               |    fq.ny,${xlbz} xltqbz,z.zzbm gddwbm,l.sl,yc.sl,
               |    (case when (l.sl=0 or l.sl is null) then 0 else ifnull(yc.sl,0)/l.sl end)*100 ycl,fq.sl,
               |    (case when (yc.sl=0 or yc.sl is null) then 0 else ifnull(fq.sl,0)/yc.sl end)*100 gdfql,gd.sl,
               |    (case when (fq.sl=0 or fq.sl is null) then 0 else ifnull(gd.sl,0)/fq.sl end)*100 ycclwcl,
               |    handleNumber(null),handleNumber(null),getDsjbm(z.zzbm) dsjbm,getQxjbm(z.zzbm) qxjbm,
               |    getGdsbm(z.zzbm) gdsbm,getzzmc(getDsjbm(z.zzbm)) dsj,getzzmc(getQxjbm(z.zzbm)) qxj,
               |    getzzmc(getGdsbm(z.zzbm)) gds,fq.ywlbbh,getywlbmc(fq.ywlbbh)
               |from zz z
               |left join gdfqs fq on z.zzbm = fq.gddwbm
               |left join gdgds gd on z.zzbm = gd.gddwbm and fq.ny = gd.ny and fq.ywlbbh = gd.ywlbbh
               |left join ny_xlzs l on z.zzbm = l.gddwbm and fq.ny = l.ny
               |left join ny_xlgddwyczs yc on z.zzbm = yc.gddwbm and fq.ny = yc.ny
               |
             """.stripMargin).repartition(resultPartition).createOrReplaceTempView("xsycfxclgzdtj") //线损异常分析处理工作单统计
        sparkSession.sql(
            s"""
               |select * from xsycfxclgzdtj
               |
             """.stripMargin).write.options(Map("kudu.master"->url,"kudu.table" -> s"${writeSchema}.gk_xsycfxclgzdtj"))
          .mode(SaveMode.Append).format("org.apache.kudu.spark.kudu").save()
    }

}
