package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.Set

/**
  * hz_xsycxlhtqmx(线损异常（线路和台区）明细)
  */
object CollectXsycxlhtqmxService {

    //汇总：线损异常线路,异常台区明细     sfxf-是否下发,bz-备注 缺,异常原因编码等新增字段还没写
    /**
      * 根据线路线损统计信息，线损率、考核指标、考核指标下限等3个字段，筛选出线损率异常线路
      * @param sparkSession
      */
    def collectXsycxlhtqmxService(sparkSession: SparkSession)={
        var start = 0L
        var end = 0L
        var reason = ""
        var isSuccess = 1

        start = System.currentTimeMillis()
        try{
            val hz_xsycxlhtqmx =
                s"""
                   |select
                   |    getUUID(),${creator_id},${create_time},${create_time},${updator_id},
                   |    xl.gddwbm,xl.sjdwbm,xl.ssjdwbm,${nowMonth} tjsj,${ybs} tjzq,xl.xlxdbs,
                   |    xl.xlbh,xl.xlmc,xl.tqbs,xl.tqbh,xl.tqmc,xl.bygdl,xl.bysdl,xl.byxsl,
                   |    xl.xszrrbs,x.rymc xszrr,xl.zhxx,xl.sfxf,xl.dqbm,xl.bz,xl.khzbxx zbxx,
                   |    xl.khzb zbsx,cast(getXlxdSqXsl(xl.xlxdbs) as decimal(12,6)),
                   |    cast(getXlxdSsqXsl(xl.xlxdbs) as decimal(12,6)),xl.xltqbz,xl.bdzbs,
                   |    xl.bdzbh,xl.bdzmc,k1.bdzkhbhh,null tqkhbhh,concat_ws(',',collect_list(ycgzmc_bh)) ycgzList  --没找到这个字段含义
                   |from xsycxlhtqmx xl  --线损异常(线路和台区)明细
                   |left join ry x on x.rybs = xl.xszrrbs
                   |left join (
                   |        select
                   |            distinct yc.gddwbm,yc.xlxdbs,yc.xltqbz,yc.tjzq,yc.tjsj,
                   |            concat(xs.ycyysjfl,"_",xs.ycyysjflbm) ycgzmc_bh
                   |        from ycgddwxlgx yc  --异常供电单位线路对应关系
                   |        join xsycgz xs on yc.ycgzbh = xs.ycgzbh  --异常规则表
                   |        ) y
                   |on  xl.gddwbm = y.gddwbm and xl.xlxdbs = y.xlxdbs
                   |    and y.xltqbz = xl.xltqbz and y.tjzq=${ybs} and y.tjsj=${nowMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |where xl.xltqbz = ${xlbz} and xl.ny = ${nowMonth}
                   |group by xl.gddwbm,xl.sjdwbm,xl.ssjdwbm,xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,
                   |    xl.tqbh,xl.tqmc,xl.bygdl,xl.bysdl,xl.byxsl,xl.xszrr,xl.zhxx,xl.sfxf,xl.dqbm,
                   |    xl.bz,xl.khzbxx,xl.khzb,xl.sqxsl,xl.xltqbz,xl.bdzbh,
                   |    xl.bdzmc,xl.xszrrbs,x.rymc,xl.bdzbs,k1.bdzkhbhh
                   |
                   |union all
                   |select
                   |    getUUID(),${creator_id},${create_time},${create_time},${updator_id},
                   |    xl.gddwbm,xl.sjdwbm,xl.ssjdwbm,${nowMonth} tjsj,${ybs} tjzq,
                   |    xl.xlxdbs,xl.xlbh,xl.xlmc,xl.tqbs,xl.tqbh,xl.tqmc,xl.bygdl,xl.bysdl,
                   |    xl.byxsl,xl.xszrrbs,x.rymc xszrr,xl.zhxx,xl.sfxf,xl.dqbm,
                   |    xl.bz,xl.khzbxx zbxx,xl.khzb zbsx,cast(getTqSqXsl(xl.tqbs) as decimal(12,6)),
                   |    cast(getTqSsqXsl(xl.tqbs) as decimal(12,6)),xl.xltqbz,xl.bdzbs,xl.bdzbh,
                   |    xl.bdzmc,k1.bdzkhbhh,k2.tqkhbhh,concat_ws(',',collect_list(ycgzmc_bh)) ycgzList
                   |from xsycxlhtqmx xl
                   |left join ry x on x.rybs = xl.xszrrbs  --人员表
                   |left join (
                   |        select
                   |            distinct yc.gddwbm,yc.tqbs,yc.xltqbz,yc.tjzq,yc.tjsj,
                   |            concat(xs.ycyysjfl,"_",xs.ycyysjflbm) ycgzmc_bh
                   |        from ycgddwxlgx yc  --异常供电单位线路对应关系
                   |        join xsycgz xs on yc.ycgzbh = xs.ycgzbh) y  --异常规则表
                   |on xl.gddwbm = y.gddwbm and xl.tqbs = y.tqbs
                   |    and y.xltqbz = xl.xltqbz  and y.tjzq=${ybs} and y.tjsj=${nowMonth}
                   |lateral view outer explode(split(getbdzkhb(xl.xlxdbs),',')) k1 as bdzkhbhh
                   |lateral view outer explode(split(gettqkhb(xl.tqbs),',')) k2 as tqkhbhh
                   |where xl.xltqbz = ${tqbz} and xl.ny = ${nowMonth}
                   |group by xl.gddwbm,xl.sjdwbm,xl.ssjdwbm,xl.xlxdbs,xl.xlbh,xl.xlmc, --运行很慢
                   |    xl.tqbs,xl.tqbh,xl.tqmc,xl.bygdl,xl.bysdl,xl.byxsl,
                   |    xl.xszrr,xl.zhxx,xl.sfxf,xl.dqbm,xl.bz,xl.khzbxx,
                   |    xl.khzb,xl.sqxsl,xl.xltqbz,xl.bdzbh,xl.bdzmc,xl.xszrrbs,
                   |    x.rymc,xl.ssqxsl,xl.bdzbs,k1.bdzkhbhh,k2.tqkhbhh
                   |
                 """.stripMargin
            val xsycxlhtqmxRDD = sparkSession.sql(hz_xsycxlhtqmx).rdd.repartition(1)
              .map(row => { //不是迭代器
                val ycgzList = row.getAs[String]("ycgzList")
                val arr = ycgzList.split(",")

                val set = Set[String]()
                for(i <- 0 until arr.size){
                    set.add(arr(i))
                }
                val ycgzArr: Array[String] = set.toArray[String]

                var realRow = Row()
                for(i <- 0 to row.size-2){
                    realRow = Row.merge(realRow,Row(row.get(i)))  //row 为一行的数据集
                }
                val size = set.size
                if(size > 5){       //todo 没数据看不了，注解也没有，想要实现什么
                    var ycgzbhOther = ""
                    for(i <- 5 until size ){
                        ycgzbhOther = ycgzbhOther + "|" + ycgzArr(i).split("_")(0)
                    }
                    var ycgzmcOther = ""
                    for(i <- 5 until size ){
                        ycgzmcOther = ycgzmcOther + "|" + ycgzArr(i).split("_")(1) + "|"
                    }
                    ycgzbhOther = ycgzbhOther.substring(1)
                    ycgzmcOther = ycgzmcOther.substring(1)
                    val ycgzRow = Row(ycgzArr(0).split("_")(0), ycgzArr(1).split("_")(0),
                        ycgzArr(2).split("_")(0), ycgzArr(3).split("_")(0), ycgzArr(4).split("_")(0), ycgzbhOther,
                        ycgzArr(0).split("_")(1), ycgzArr(1).split("_")(1),
                        ycgzArr(2).split("_")(1), ycgzArr(3).split("_")(1), ycgzArr(4).split("_")(1),ycgzmcOther)
                    Row.merge(realRow, ycgzRow)
                }else if (size == 5) {
                    val ycgzRow = Row(ycgzArr(0).split("_")(0), ycgzArr(1).split("_")(0),
                        ycgzArr(2).split("_")(0), ycgzArr(3).split("_")(0), ycgzArr(4).split("_")(0), "",
                        ycgzArr(0).split("_")(1), ycgzArr(1).split("_")(1),
                        ycgzArr(2).split("_")(1), ycgzArr(3).split("_")(1), ycgzArr(4).split("_")(1),"")
                    Row.merge(realRow, ycgzRow)
                } else if (size == 4) {
                    val ycgzRow = Row(ycgzArr(0).split("_")(0), ycgzArr(1).split("_")(0),
                        ycgzArr(2).split("_")(0), ycgzArr(3).split("_")(0), "", "",
                        ycgzArr(0).split("_")(1), ycgzArr(1).split("_")(1),
                        ycgzArr(2).split("_")(1), ycgzArr(3).split("_")(1), "","")
                    Row.merge(realRow, ycgzRow)
                } else if (size == 3) {
                    val ycgzRow = Row(ycgzArr(0).split("_")(0), ycgzArr(1).split("_")(0),
                        ycgzArr(2).split("_")(0), "", "", "",
                        ycgzArr(0).split("_")(1), ycgzArr(1).split("_")(1),
                        ycgzArr(2).split("_")(1), "", "","")
                    Row.merge(realRow, ycgzRow)
                } else if (size == 2) {
                    val ycgzRow = Row(ycgzArr(0).split("_")(0), ycgzArr(1).split("_")(0),
                        "", "", "", "",
                        ycgzArr(0).split("_")(1), ycgzArr(1).split("_")(1),
                        "", "", "","")
                    Row.merge(realRow, ycgzRow)
                } else {    //size至少为1，即使是""空字符串，长度也为1。这里要判断是否是空字符串
                    if(!"".equals(ycgzArr(0))){
                        val ycgzRow = Row(ycgzArr(0).split("_")(0), "",
                            "", "", "", "",
                            ycgzArr(0).split("_")(1), "",
                            "", "", "","")
                        Row.merge(realRow, ycgzRow)
                    }else{
                        val ycgzRow = Row("原因不明", "",
                            "", "", "", "",
                            "700", "",
                            "", "", "","")
                        Row.merge(realRow, ycgzRow)
                    }

                }
            })
            val xsycxlhtqmxType = StructType(List[StructField](
                StructField("ID", StringType, true),
                StructField("CREATOR_ID", StringType, true),
                StructField("CREATE_TIME", StringType, true),
                StructField("UPDATE_TIME", StringType, true),
                StructField("UPDATOR_ID", StringType, true),
                StructField("GDDWBM", StringType, true),
                StructField("SJDWBM", StringType, true),
                StructField("SSJDWBM", StringType, true),
                StructField("TJSJ", IntegerType, true),
                StructField("TJZQ", StringType, true),
                StructField("XLXDBS", StringType, true),
                StructField("XLBH", StringType, true),
                StructField("XLMC", StringType, true),
                StructField("TQBS", StringType, true),
                StructField("TQBH", StringType, true),
                StructField("TQMC", StringType, true),
                StructField("GDL", DecimalType(14,2), true),
                StructField("SDL", DecimalType(14,2), true),
                StructField("XSL", DecimalType(12,6), true),
                StructField("XSZRRBS", StringType, true),
                StructField("XSZRR", StringType, true),
                StructField("ZHXX", StringType, true),
                StructField("SFXF", StringType, true),
                StructField("DQBM", StringType, true),
                StructField("BZ", StringType, true),
                StructField("ZBXX", DecimalType(12,6), true),
                StructField("ZBSX", DecimalType(12,6), true),
                StructField("SQXSL", DecimalType(12,6), true),
                StructField("SSQXSL", DecimalType(12,6), true),
                StructField("XLTQBZ", StringType, true),
                StructField("BDZBS", StringType, true),
                StructField("BDZBH", StringType, true),
                StructField("BDZMC", StringType, true),
                StructField("BDZKHBHH", StringType, true),
                StructField("TQKHBHH", StringType, true),
                StructField("YCYYY", StringType, true),
                StructField("YCYYE", StringType, true),
                StructField("YCYYS", StringType, true),
                StructField("YCYYSI", StringType, true),
                StructField("YCYYW", StringType, true),
                StructField("QTYCYY", StringType, true),
                StructField("YCYYYBM", StringType, true),
                StructField("YCYYEBM", StringType, true),
                StructField("YCYYSBM", StringType, true),
                StructField("YCYYSIBM", StringType, true),
                StructField("YCYYWBM", StringType, true),
                StructField("QTYCYYBM", StringType, true)
            ))
            import sparkSession.implicits._
            sparkSession.createDataFrame(xsycxlhtqmxRDD,xsycxlhtqmxType)
              .createOrReplaceTempView("hz_xsycxlhtqmx")  //线损异常线路和台区明细

            sparkSession.sql("select * from hz_xsycxlhtqmx where isFiveDsj(gddwbm) = 1")
              .repartition(resultPartition).createOrReplaceTempView("hz_xsycxlhtqmx")
            sparkSession.sql(
                s"""
                   |insert into ${writeSchema}.hz_xsycxlhtqmx partition(fqrq)
                   |select
                   |    *,
                   |    getDsjbm(gddwbm) dsjbm,getQxjbm(gddwbm) qxjbm,getGdsbm(gddwbm) gdsbm,
                   |    getzzmc(getDsjbm(gddwbm)) dsj,
                   |    getzzmc(getQxjbm(gddwbm)) QXJ,
                   |    getzzmc(getGdsbm(gddwbm)) gds,
                   |    tjsj fqrq
                   |from hz_xsycxlhtqmx
                   |
                 """.stripMargin) // TODO: 进行了具体异常原因的收集
            reason = ""
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
               |    getFormatDate(${end}) recordtime,'线损异常线路和台区异常明细',
               |    ${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime
               |
             """.stripMargin).createOrReplaceTempView("ruleState")
        println(s"线损异常线路和台区异常明细${(end-start)/1000}秒")
    }

}
