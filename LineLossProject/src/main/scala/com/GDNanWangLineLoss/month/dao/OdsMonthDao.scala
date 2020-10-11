package com.GDNanWangLineLoss.month.dao

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import com.GDNanWangLineLoss.month.bean.Variables._  //导入变量
//todo kudu库里的表名基本是这个形式  impala::csg_ods_yx.dw_bdz

/**
  * 数据源
  */
object OdsMonthDao {


    /*----------------------------------------------------------------------------------------------------------------*/
    // TODO: 1
    def getCbxxData(sparkSession:SparkSession,url:String)={  //对标数据源

        //抄表信息（过去6个月）  yhbh-用户编号,zcbh-资产编号,zhbl-综合倍率,scbss-上次表示数,bcbss-本次表示数,bqcbcs-本期抄表示数,sccbrq-上次抄表日期,cbsj-抄表时间,bjdl-报警电量,
        //          jldbh-计量点编号,dfny-电费年月,yhmc-用户名称,jldcbfsdm-计量点抄表方式
        //lixc20200506新加字段 cbsxh-抄表顺序号,sjcbfsdm-实际抄表方式代码,gzdbh-工作单编号,ywlbdm-业务类别代码,dqbm-地区编码,gddwbm-供电单位编码,cbrbs-抄表人标识,yddz-用电地址,cbqdbh-抄表区段编号,jldcbsxh-计量点抄表顺序号

        //val cbxx = spark.sql(s"""select yhbh,zcbh,zhbl,scbss,bcbss,bqcbcs,sccbrq,cbsj,bjdl,jldbh,dfny,yhmc,jldcbfsdm,yxdnbbs,sslxdm,bssce,yhlbdm,jbdl,cbsxh,sjcbfsdm,gzdbh,ywlbdm,dqbm,gddwbm,cbrbs,yddz,cbqdbh,jldcbsxh from _cbxx_all
        //                    where dfny in (${nowMonth},${lastMonth}) """)
        //cbxx.cache().createOrReplaceTempView("_cbxx")

        val table1 ="impala::csg_ods_yx.lc_cbxx"    //todo这个在南网是全部的表
                 // val table2 ="npmis_lc_cbxx_his"  南网没有这个
        val kuduMap1: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table1)

        val cbxx1=sparkSession.read.options(kuduMap1).format("org.apache.kudu.spark.kudu").load()
        cbxx1.createOrReplaceTempView("_cbxx")


        /*
        抄表信息  yhbh-用户编号,zcbh-资产编号,zhbl-综合倍率,scbss-上次表示数,bcbss-本次表示数,bqcbcs-本期抄表示数,sccbrq-上次抄表日期,cbsj-抄表时间,bjdl-报警电量,
                  jldbh-计量点编号,dfny-电费年月,yhmc-用户名称,jldcbfsdm-计量点抄表方式
        lixc20200506新加字段 cbsxh-抄表顺序号,sjcbfsdm-实际抄表方式代码,gzdbh-工作单编号,ywlbdm-业务类别代码,dqbm-地区编码,gddwbm-供电单位编码,cbrbs-抄表人标识,yddz-用电地址,cbqdbh-抄表区段编号,jldcbsxh-计量点抄表顺序号
         */
        val cbxx=sparkSession.sql(
            s"""
               |select yhbh,zcbh,zhbl,scbss,bcbss,bqcbcs,sccbrq,cbsj,bjdl,jldbh,dfny,yhmc,jldcbfsdm,yxdnbbs,sslxdm,bssce,yhlbdm,jbdl,cbsxh,sjcbfsdm,gzdbh,ywlbdm,dqbm,gddwbm,cbrbs,yddz,cbqdbh,jldcbsxh
               |from _cbxx
               |where dfny in (${nowMonth},${lastMonth})
               |
             """.stripMargin)
        cbxx.cache().createOrReplaceTempView("_cbxx")
    }



    // TODO: 2
    def getYdkhData(sparkSession: SparkSession, url:String)={
        //用电户   yhbh-用户编号,yhmc-用户名称,yhztdm-用电状态,dydjdm-电压等级代码,gddwbm-供电单位编码,lhrq-立户日期,cbqdbh-抄表区段,cbzq-抄表周期

//        val ydkh = spark.sql(
//            s"""select yhbh,yhmc,yhztdm,dydjdm,gddwbm,yhlbdm,lhrq,cbqdbh,cbzq,hyfldm
        //from npmis_kh_ydkh
        //where dqbm in (${cityCodeList})""".stripMargin)
//        ydkh.createOrReplaceTempView("_ydkh")

        val table ="impala::csg_ods_yx.kh_ydkh"  //todo 所有的 npmis的前缀全部去掉
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val ydkh1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()//.where(s"dqbm in (${cityCodeList})")
        ydkh1.createOrReplaceTempView("_ydkh1")
        val ydkh=sparkSession.sql(
            s"""
               |select yhbh,yhmc,yhztdm,dydjdm,gddwbm,yhlbdm,lhrq,cbqdbh,cbzq,hyfldm
               |from _ydkh1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_ydkh1")
        ydkh.createOrReplaceTempView("_ydkh")
    }


    // TODO: 3
    def getJldData(sparkSession: SparkSession,url:String)={
        //计量点   jldbh-计量点编号,jldmc-计量点名称,xlxdbs-线路线段标识,yhbh-用户编号,tqbs-台区标识,jlfsdm-计量方式,jxfsdm-接线方式代码
        //lixc20200506新加字段 jldydjdm-计量电压等级代码

//        val jld = spark.sql(
//            s"""select jldbh,jldmc,jldytdm,xlxdbs,yhbh,tqbs,jlfsdm,jxfsdm,jldztdm,jldlbdm,cbfsdm,gddwbm,jldydjdm
//               |from npmis_kh_jld
//               |where dqbm in (${cityCodeList})""".stripMargin)
//        jld.createOrReplaceTempView("_jld")

        val table ="impala::csg_ods_yx.kh_jld"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val jld1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        jld1.createOrReplaceTempView("_jld1")

        val jld=sparkSession.sql(
            s"""
               |select jldbh,jldmc,jldytdm,xlxdbs,yhbh,tqbs,jlfsdm,jxfsdm,jldztdm,jldlbdm,cbfsdm,gddwbm,jldydjdm
               |from _jld1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_jld1")
        jld.createOrReplaceTempView("_jld")

    }

    // TODO: 4
    def getAll_xlxdData(sparkSession: SparkSession, url:String): DataFrame ={
        //线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
        //lixc20200506只查配线

//        val all_xlxd = spark.sql(
//            s"""select l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm
//               |from npmis_dw_xlxd l
//               |where l.xllbdm = '1' and l.xlyxzt = '01' and l.dydjdm not in ('10','12','13','15') and dqbm in (${cityCodeList})""".stripMargin)
//        all_xlxd.createOrReplaceTempView("_all_xlxd")

        val table ="impala::csg_ods_yx.dw_xlxd"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val all_xlxd1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        all_xlxd1.createOrReplaceTempView("_all_xlxd1")
        val all_xlxd =sparkSession.sql(
            s"""
               |select l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm
               |from _all_xlxd1 l
               |where l.xllbdm = '1' and l.xlyxzt = '01' and l.dydjdm not in ('10','12','13','15') and dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_all_xlxd1")
        all_xlxd.createOrReplaceTempView("_all_xlxd")
        all_xlxd

    }

    // TODO: 5
    def getAll_tpData(sparkSession: SparkSession, url:String)={
        //台区  tqbs-台区标识,tqbh-台区编号,tqmc-台区名称

//        val all_tq = spark.sql(
//            s"""select tqbs,tqbh,tqmc,yxztdm,gddwbm,cbzq
//               |from npmis_dw_tq t
//               |where t.gddwbm is not null and dqbm in (${cityCodeList})""".stripMargin)
//        all_tq.createOrReplaceTempView("_all_tq")

        val table ="impala::csg_ods_yx.dw_tq"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val all_tq1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        all_tq1.createOrReplaceTempView("_all_tq1")
        val all_tq=sparkSession.sql(
            s"""
               |select tqbs,tqbh,tqmc,yxztdm,gddwbm,cbzq
               |from _all_tq1 t
               |where t.gddwbm is not null and dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_all_tq1")
        all_tq.createOrReplaceTempView("_all_tq")

    }

    // TODO: 6
    def getXltqgxData(sparkSession: SparkSession, url:String)={
        //线路台区关系  xlxdbs-线路线段标识,tqbs-台区标识

//        val xltqgx = spark.sql(s"""select xlxdbs,tqbs from npmis_dw_xltqgx where dqbm in (${cityCodeList})""")
//        xltqgx.createOrReplaceTempView("_xltqgx")

        val table ="impala::csg_ods_yx.dw_xltqgx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xltqgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xltqgx1.createOrReplaceTempView("_xltqgx1")
        val xltqgx =sparkSession.sql(
            s"""
               |select xlxdbs,tqbs from _xltqgx1 where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_xltqgx1")
        xltqgx.createOrReplaceTempView("_xltqgx")
    }


    // TODO: 7
    def getYhdyxxData(sparkSession: SparkSession, url:String)={
//        //用户电源信息  xlxdbs-线路线段标识,tqbs-台区标识,yhbh-用户编号,dybh-电源编号,dyxzdm-电源状态代码

//        val yhdyxx = spark.sql(s"""select xlxdbs,tqbs,yhbh,dybh,dyxzdm from npmis_kh_yhdyxx where dqbm in (${cityCodeList})""")
//        yhdyxx.createOrReplaceTempView("_yhdyxx")

        val table ="impala::csg_ods_yx.kh_yhdyxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val yhdyxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        yhdyxx1.createOrReplaceTempView("_yhdyxx1")
        val yhdyxx =sparkSession.sql(
            s"""
               |select xlxdbs,tqbs,yhbh,dybh,dyxzdm from _yhdyxx1 where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_yhdyxx1")
        yhdyxx.createOrReplaceTempView("_yhdyxx")

    }

    // TODO: 8
    def getYxbyqData(sparkSession: SparkSession, url:String)={
        // 变压器  sbbs-唯一标识,mc-名称,zcmrid-资产mrid ,tqbs-台区标识,yhbh-用户编号,edrl-设备铭牌上登记的容量,gbzbbz-公变专变标志,edrl-额定容量,
        // shlxdm-损耗类型代码,sbxhdm-设备型号代码,yxztdm-运行状态代码,kzsh-空载损耗,gddwbm-供电单位代码

//        val yxbyq = spark.sql(
//            s"""select sbbs,mc,zcmrid,gisid,tqbs,yhbh,gbzbbz,edrl,shlxdm,sbxhdm,yxztdm,kzsh,gddwbm
//               |from npmis_dw_yxbyq where dqbm in (${cityCodeList})""".stripMargin)
//        yxbyq.createOrReplaceTempView("_yxbyq")

        val table ="impala::csg_ods_yx.dw_yxbyq"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val yxbyq1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        yxbyq1.createOrReplaceTempView("_yxbyq1")
        val yxbyq =sparkSession.sql(
            s"""
               |select sbbs,mc,zcmrid,gisid,tqbs,yhbh,gbzbbz,edrl,shlxdm,sbxhdm,yxztdm,kzsh,gddwbm
               |from _yxbyq1 where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_yxbyq1")
        yxbyq.createOrReplaceTempView("_yxbyq")

    }


    // TODO: 9
    def getJlddnbgxData(sparkSession: SparkSession, url:String)={
        //计量点运行电能表关系  jldbh-计量点编号,yxdnbbs-运行电能表标识

//        val jlddnbgx = spark.sql(s"""select jldbh,yxdnbbs from npmis_kh_jlddnbgx where dqbm in (${cityCodeList})""")
//        jlddnbgx.createOrReplaceTempView("_jlddnbgx")

        val table ="impala::csg_ods_yx.kh_jlddnbgx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val jlddnbgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        jlddnbgx1.createOrReplaceTempView("_jlddnbgx1")
        val jlddnbgx =sparkSession.sql(
            s"""
               |select jldbh,yxdnbbs from _jlddnbgx1 where dqbm in (${cityCodeList})
             """.stripMargin)
        sparkSession.catalog.dropTempView("_jlddnbgx1")
        jlddnbgx.createOrReplaceTempView("_jlddnbgx")

    }

    // TODO: 10
    def getYxdnbData(sparkSession: SparkSession, url:String)={
//        //运行电能表 yxdnbbs-运行电能表标识,csbs-参数标识

//        val yxdnb = spark.sql(s"""select yxdnbbs,csbs,zcbh,zhbl from npmis_sb_yxdnb where dqbm in (${cityCodeList})""")
//        yxdnb.createOrReplaceTempView("_yxdnb")

        val table ="impala::csg_ods_yx.sb_yxdnb"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val yxdnb1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()

        yxdnb1.createOrReplaceTempView("_yxdnb1")
        val yxdnb =sparkSession.sql(
            s"""
               |select yxdnbbs,csbs,zcbh,zhbl from _yxdnb1 where dqbm in (${cityCodeList})
             """.stripMargin)
        sparkSession.catalog.dropTempView("_yxdnb1")
        yxdnb.createOrReplaceTempView("_yxdnb")

    }

    // TODO: 11
    def getDnbcsData(sparkSession: SparkSession, url:String)={
        //电能表参数  csbs-参数标识,eddydm-额定电压

//        val dnbcs = spark.sql(s"""select csbs,eddydm from npmis_sb_dnbcs""")
//        dnbcs.createOrReplaceTempView("_dnbcs")

        val table ="impala::csg_ods_yx.sb_dnbcs"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val dnbcs1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        dnbcs1.createOrReplaceTempView("_dnbcs")
        val dnbcs =sparkSession.sql(
            s"""
               |select csbs,eddydm from _dnbcs
             """.stripMargin)
        dnbcs.createOrReplaceTempView("_dnbcs")

    }

    // TODO: 12
    def getJldxx_allData(sparkSession: SparkSession, url:String)={
        //计量点信息(过去6个月) yhbh-用户编号,yhmc-用户名称,jldytdm-计量表分类,jfdl-计费电量,dfny-电费年月,xlxdbs-线路线段标识,tbgzdbh-退补工作单编号,tqbs-台区标识
        //           jldbh-计量点编号,gddwbm-供电单位编码,yhlbdm-用户类别代码,bqcbcs-本期抄表次数,yhztdm-用户状态代码,jlfsdm-计量方式代码,ygbsdl-有功变损电量,
        //            ygcjdl-有功抄见电量,jldydjdm-计量点电压等级代码,hyfldm-行业分类代码
        //lixc20200506新加字段 djdm-电价代码,cbjhbh-抄表计划编号,jldcbsxh-计量点抄表顺序号,yddz-用电地址,cbqdbh-抄表区段编号,mfdl-免费电量,jfrl-计费容量,ywlbdm-业务类别代码,jldxh-计量点序号,ygtbdl-有功退补电量,ygfbkjdl-有功分表扣减电量,dqbm-地区编码,cbrq-抄表日期,dwbm-单位编码

//        val jldxx_all = spark.sql(s"""select yhbh,yhmc,jldytdm,jfdl,dfny,xlxdbs,tbgzdbh,tqbs,jldbh,gddwbm,yhlbdm,bqcbcs,yhztdm,jlfsdm,ygbsdl,ygcjdl,jldydjdm,hyfldm,gzdbh,scdl,ygzdl,wgzdl,hsztdm,
//                        djdm,cbjhbh,jldcbsxh,yddz,cbqdbh,mfdl,jfrl,ywlbdm,jldxh,ygtbdl,ygfbkjdl,dqbm,cbrq,dwbm
//						from npmis_hs_jldxx where dfny in (${nowMonth},${lastMonth}) and ywlbdm = '0000' and dqbm in (${cityCodeList})
//						union all
//                        select yhbh,yhmc,jldytdm,jfdl,dfny,xlxdbs,tbgzdbh,tqbs,jldbh,gddwbm,yhlbdm,bqcbcs,yhztdm,jlfsdm,ygbsdl,ygcjdl,jldydjdm,hyfldm,gzdbh,scdl,ygzdl,wgzdl,hsztdm,
//                        djdm,cbjhbh,jldcbsxh,yddz,cbqdbh,mfdl,jfrl,ywlbdm,jldxh,ygtbdl,ygfbkjdl,dqbm,cbrq,dwbm
//						from npmis_hs_jldxx_his where dfny in (${nowMonth},${lastMonth},${lastMonth2},${lastMonth3},${lastMonth4},${lastMonth5},${tqny}) and ywlbdm = '0000' and dqbm in (${cityCodeList})
//					  """)
//        jldxx_all.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("_jldxx_all")

        val table1 ="impala::csg_ods_yx.hs_jldxx_d"  //hs_jldxx_d
                // val table2 ="npmis_hs_jldxx_his"  没这表
        val kuduMap1: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table1)
        val jldxx1=sparkSession.read.options(kuduMap1).format("org.apache.kudu.spark.kudu").load()
        jldxx1.createOrReplaceTempView("_jldxx1")

        val jldxx_all =sparkSession.sql(
            s"""
               |select yhbh,yhmc,jldytdm,jfdl,dfny,xlxdbs,tbgzdbh,tqbs,jldbh,gddwbm,yhlbdm,bqcbcs,yhztdm,jlfsdm,ygbsdl,ygcjdl,jldydjdm,hyfldm,
               |gzdbh,scdl,ygzdl,wgzdl,hsztdm,djdm,cbjhbh,jldcbsxh,yddz,cbqdbh,mfdl,jfrl,ywlbdm,jldxh,ygtbdl,ygfbkjdl,dqbm,cbrq,dwbm
               |from _jldxx1
               |where dfny in (${nowMonth},${lastMonth}) and ywlbdm = '0000' and dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("_jldxx1")
        jldxx_all.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("_jldxx_all")

    }

    // TODO: 13
    //这个表数据不是原始表
    def getJldxxData(sparkSession: SparkSession, url:String)={

        //计量点信息 yhbh-用户编号,yhmc-用户名称,jldytdm-计量表分类,jfdl-计费电量,dfny-电费年月,xlxdbs-线路线段标识,tbgzdbh-退补工作单编号,tqbs-台区标识
        //           jldbh-计量点编号,gddwbm-供电单位编码,yhlbdm-用户类别代码,bqcbcs-本期抄表次数,yhztdm-用户状态代码,jlfsdm-计量方式代码,ygbsdl-有功变损电量,
        //            ygcjdl-有功抄见电量,jldydjdm-计量点电压等级代码,hyfldm-行业分类代码
        //lixc20200506新加字段 djdm-电价代码,cbjhbh-抄表计划编号,jldcbsxh-计量点抄表顺序号,yddz-用电地址,cbqdbh-抄表区段编号,mfdl-免费电量,jfrl-计费容量,ywlbdm-业务类别代码,jldxh-计量点序号,ygtbdl-有功退补电量,ygfbkjdl-有功分表扣减电量,dqbm-地区编码,cbrq-抄表日期,dwbm-单位编码

        val jldxx = sparkSession.sql(
            s"""
               |select yhbh,yhmc,jldytdm,jfdl,dfny,xlxdbs,tbgzdbh,tqbs,jldbh,gddwbm,yhlbdm,bqcbcs,yhztdm,jlfsdm,ygbsdl,ygcjdl,
               |	jldydjdm,hyfldm,gzdbh,scdl,ygzdl,wgzdl,hsztdm,djdm,cbjhbh,jldcbsxh,yddz,cbqdbh,mfdl,jfrl,ywlbdm,jldxh,ygtbdl,ygfbkjdl,dqbm,cbrq,dwbm
               |from _jldxx_all
               |where dfny in (${nowMonth},${lastMonth}) and ywlbdm = '0000'
               |
             """.stripMargin)
        jldxx.cache().createOrReplaceTempView("_jldxx")

    }

    // TODO: 14
    def getTbdlxxData(sparkSession: SparkSession, url:String)={
        //退补电量信息 ygdl-有功电量,gzdbh-工作单编号,ccyf-差错月份

//        val tbdlxx = spark.sql(s"""select ygdl,gzdbh,ccyf,jldbh from npmis_hs_tbdlxx where dqbm in (${cityCodeList})""")
//        tbdlxx.createOrReplaceTempView("_tbdlxx")

        val table ="impala::csg_ods_yx.hs_tbdlxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val tbdlxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        tbdlxx1.createOrReplaceTempView("_tbdlxx")
        val tbdlxx =sparkSession.sql(
            s"""
               |select ygdl,gzdbh,ccyf,jldbh from _tbdlxx where dqbm in (${cityCodeList})
             """.stripMargin)
        tbdlxx.createOrReplaceTempView("_tbdlxx")

    }

    // TODO: 15
    def getCztbData(sparkSession: SparkSession, url:String)={
        //冲正退补

//        val cztb = spark.sql(s"""select tbdl,gzdbh,jldbh,dfny,tblx from npmis_hs_tbmx where dqbm in (${cityCodeList})""")
//        cztb.createOrReplaceTempView("_tbmx")

        val table ="impala::csg_ods_yx.hs_tbmx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val cztb1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        cztb1.createOrReplaceTempView("_tbmx")
        val cztb =sparkSession.sql(
            s"""
               |select tbdl,gzdbh,jldbh,dfny,tblx from _tbmx where dqbm in (${cityCodeList})
             """.stripMargin)
        cztb.createOrReplaceTempView("_tbmx")

    }


    // TODO: 16
    def getGfyhdaxxData(sparkSession: SparkSession, url:String)={
        //光伏用户档案信息 yhbh-用户编号

//        val gfyhdaxx = spark.sql(s"""select yhbh from npmis_sc_gf_gfyhdaxx where dqbm in (${cityCodeList})""")
//        gfyhdaxx.createOrReplaceTempView("_gfyhdaxx")

        val table ="impala::csg_ods_yx.sc_gf_gfyhdaxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gfyhdaxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gfyhdaxx1.createOrReplaceTempView("_gfyhdaxx")
        val gfyhdaxx =sparkSession.sql(
            s"""
               |select yhbh from _gfyhdaxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gfyhdaxx.createOrReplaceTempView("_gfyhdaxx")

    }

    // TODO: 17
    def getGfyhglxxData(sparkSession: SparkSession, url:String)={
        //光伏用户关联信息 gfhyhbh-光伏户用户编号,gdhyhbh-购电户用户编号,ydhyhbh-用电户用户编号

//        val gfyhglxx = spark.sql(s"""select gfhyhbh,gdhyhbh,ydhyhbh from npmis_sc_gf_gfyhglxx where dqbm in (${cityCodeList})""")
//        gfyhglxx.createOrReplaceTempView("_gfyhglxx")

        val table ="impala::csg_ods_yx.sc_gf_gfyhglxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gfyhglxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gfyhglxx1.createOrReplaceTempView("_gfyhglxx")
        val gfyhglxx =sparkSession.sql(
            s"""
               |select gfhyhbh,gdhyhbh,ydhyhbh from _gfyhglxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gfyhglxx.createOrReplaceTempView("_gfyhglxx")

    }


    // TODO: 18
    def getXlbzxxData(sparkSession: SparkSession, url:String)={
        //线路编组信息  bdzbh-变电站编号,bdzmc-变电站名称,bdzbs-变电站标识,xlbh-线路编号,xlmc-线路名称,xlxdbs-线路线段标识,xlbzbh-线路编组编号,create_time-创建时间,gddwbm-供电单位编码

//        val xlbzxx = spark.sql(s"""select bdzbh,bdzmc,xlbh,xlmc,xlbzbh,bdzbs,xlxdbs,cjsj,gddwbm,bzny from npmis_gk_xlbzxx""")
//        xlbzxx.createOrReplaceTempView("_xlbzxx")

        val table ="impala::csg_ods_yx.gk_xlbzxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xlbzxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xlbzxx1.createOrReplaceTempView("_xlbzxx")
        val xlbzxx =sparkSession.sql(
            s"""
               |select bdzbh,bdzmc,xlbh,xlmc,xlbzbh,bdzbs,xlxdbs,cjsj,gddwbm,bzny from _xlbzxx
             """.stripMargin)
        xlbzxx.createOrReplaceTempView("_xlbzxx")

    }


    // TODO: 19
    def getTqbzxxData(sparkSession: SparkSession, url:String)={
        //台区编组信息

//        val tqbzxx = spark.sql(s"""select tqbzbh,tqbs,bzny from npmis_gk_tqbzxx""")
//        tqbzxx.createOrReplaceTempView("_tqbzxx")

        val table ="impala::csg_ods_yx.gk_tqbzxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val tqbzxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        tqbzxx1.createOrReplaceTempView("_tqbzxx")
        val tqbzxx = sparkSession.sql(
            """
              |select tqbzbh,tqbs,bzny from _tqbzxx
            """.stripMargin)
        tqbzxx.createOrReplaceTempView("_tqbzxx")

    }


    // TODO: 20
    def getWqxxData(sparkSession: SparkSession, url:String)={
        //违窃信息   chsj-查获时间,yzbdl-应追补电量,yhbh-用户编号,gzdbh-工作单编号
//        val wqxx = spark.sql(s"""select chsj,yzbdl,yhbh,gzdbh,dfny from npmis_fw_wqxx where dqbm in (${cityCodeList})""")
//        wqxx.createOrReplaceTempView("_wqxx")

        val table ="impala::csg_ods_yx.fw_wqxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val wqxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        wqxx1.createOrReplaceTempView("_wqxx")
        val wqxx =sparkSession.sql(
            s"""
               |select chsj,yzbdl,yhbh,gzdbh,dfny from _wqxx where dqbm in (${cityCodeList})
             """.stripMargin)
        wqxx.createOrReplaceTempView("_wqxx")

    }

    // TODO: 21
    def getGzdxxData(sparkSession: SparkSession, url:String)={
        //工作单信息  gzdbh-工作单编号

//        val gzdxx = spark.sql(s"""select gzdbh,ywlb,ywfl,sqsj,wcsj from npmis_xt_gzdxx where dqbm in (${cityCodeList})""")
//        gzdxx.createOrReplaceTempView("_gzdxx")

        val table ="impala::csg_ods_yx.xt_gzdxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gzdxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gzdxx1.createOrReplaceTempView("_gzdxx")
        val gzdxx =sparkSession.sql(
            s"""
               |select gzdbh,ywlb,ywfl,sqsj,wcsj from _gzdxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gzdxx.createOrReplaceTempView("_gzdxx")

    }

    // TODO: 22
    def getXlhtqzbData(sparkSession: SparkSession, url:String)={
        //线路和台区指标
//        val xlhtqzb = spark.sql(
//            s"""select bdzbh,bdzmc,xlbh,xlmc,tqbh,tqmc,ny,bdzbs,xlxdbs,tqbs,gddwbm,xltqbz,xszrrbs,khzb,khzbxx,rxskhzb,rxskhzbxx
//               |from npmis_gk_xlhtqzb where dqbm in (${cityCodeList})""".stripMargin)
//        xlhtqzb.createOrReplaceTempView("_xlhtqzb")

        val table ="impala::csg_ods_yx.gk_xlhtqzb"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xlhtqzb1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xlhtqzb1.createOrReplaceTempView("_xlhtqzb1")
        val xlhtqzb=sparkSession.sql(
            s"""
               |select bdzbh,bdzmc,xlbh,xlmc,tqbh,tqmc,ny,bdzbs,xlxdbs,tqbs,gddwbm,xltqbz,xszrrbs,khzb,khzbxx,rxskhzb,rxskhzbxx
               |from _xlhtqzb1 where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        xlhtqzb.createOrReplaceTempView("_xlhtqzb")
        sparkSession.catalog.dropTempView("_xlhtqzb1")


    }

    // TODO: 23
    def getXszrrData(sparkSession: SparkSession, url:String)={

        //线损责任人  rybh-人员编号,rymc-人员名称,rybs-人员标识,xszrrbs-线损责任人标识

//        val xszrr = spark.sql(s"""select rybh,rymc,rybs,xszrrbs from npmis_gk_xszrr where dqbm in (${cityCodeList})""")
//        xszrr.createOrReplaceTempView("_xszrr")

        val table ="impala::csg_ods_yx.gk_xszrr"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xszrr1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xszrr1.createOrReplaceTempView("_xszrr")
        val xszrr =sparkSession.sql(
            s"""
               |select rybh,rymc,rybs,xszrrbs from _xszrr where dqbm in (${cityCodeList})
             """.stripMargin)
        xszrr.createOrReplaceTempView("_xszrr")

    }


    // TODO: 24
    def getYsdfjl(sparkSession: SparkSession, url:String)={
        //应收电费记录  yhbh-用户编号,yhmc-用户名称,ysdf-应收电费,jldbh-计量点编号,gddwbm-供电单位编码,yhlbdm-用户类别代码
        //lixc20200506新增字段 ywlbdm-业务类别代码,djdm-电价代码

//        val ysdfjl = spark.sql(
//            s"""select yhbh,yhmc,ysdf,jldbh,gddwbm,yhlbdm,dfny,bqcbcs,xlxdbs,yddz,tqbs,
//               |cbqdbh,jfdl,czsj,ywlbdm,djdm from npmis_zw_ysdfjl where dqbm in (${cityCodeList})""".stripMargin)
//        ysdfjl.createOrReplaceTempView("_ysdfjl")

        val table ="impala::csg_ods_yx.zw_ysdfjl_d"  //zw_ysdfjl_d
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val ysdfjl1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        ysdfjl1.createOrReplaceTempView("_ysdfjl")
        val ysdfjl =sparkSession.sql(
            s"""
               |select yhbh,yhmc,ysdf,jldbh,gddwbm,yhlbdm,dfny,bqcbcs,xlxdbs,yddz,tqbs,cbqdbh,jfdl,czsj,ywlbdm,djdm
               |from _ysdfjl
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        ysdfjl.createOrReplaceTempView("_ysdfjl")

    }


    // TODO: 25
    def getCbqdxxData(sparkSession: SparkSession, url:String)={
        //抄表区段信息  cbqdbh-抄表区段编号,cbqdmc-抄表区段名称

//        val cbqdxx = spark.sql(s"""select cbqdbh,cbqdmc from npmis_lc_cbqdxx where dqbm in (${cityCodeList})""")
//        cbqdxx.createOrReplaceTempView("_cbqdxx")

        val table ="impala::csg_ods_yx.lc_cbqdxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val cbqdxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        cbqdxx1.createOrReplaceTempView("_cbqdxx")
        val cbqdxx =sparkSession.sql(
            s"""
               |select cbqdbh,cbqdmc from _cbqdxx where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        cbqdxx.createOrReplaceTempView("_cbqdxx")

    }


    // TODO: 26
    def getYxjlzdhzdData(sparkSession: SparkSession, url:String)={
        //运行计量自动化终端   yxjlzdhzdbs-运行计量自动化标识,yhbh-用户编号,sblbdm-设备类别代码

//        val yxjlzdhzd = spark.sql(s"""select yxjlzdhzdbs,yhbh,sblbdm from npmis_sb_yxjlzdhzd where dqbm in (${cityCodeList})""")
//        yxjlzdhzd.createOrReplaceTempView("_yxjlzdhzd")

        val table ="impala::csg_ods_yx.sb_yxjlzdhzd"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val yxjlzdhzd1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        yxjlzdhzd1.createOrReplaceTempView("_yxjlzdhzd")
        val yxjlzdhzd =sparkSession.sql(
            s"""
               |select yxjlzdhzdbs,yhbh,sblbdm from _yxjlzdhzd where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        yxjlzdhzd.createOrReplaceTempView("_yxjlzdhzd")

    }


    // TODO: 27
    def getJlzdhzdcjgxjlData(sparkSession: SparkSession, url:String)={
        //计量自动化终端采集关系记录   yxjlzdhzdbs-运行计量自动化标识,cjdxyhbh-采集对象用户编号

//        val jlzdhzdcjgxjl = spark.sql(s"""select yxjlzdhzdbs,cjdxyhbh from npmis_sb_jlzdhzdcjgxjl where dqbm in (${cityCodeList})""")
//        jlzdhzdcjgxjl.createOrReplaceTempView("_jlzdhzdcjgxjl")

        val table ="impala::csg_ods_yx.sb_jlzdhzdcjgxjl"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val jlzdhzdcjgxjl1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        jlzdhzdcjgxjl1.createOrReplaceTempView("_jlzdhzdcjgxjl")
        val jlzdhzdcjgxjl =sparkSession.sql(
            s"""
               |select yxjlzdhzdbs,cjdxyhbh from _jlzdhzdcjgxjl where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        jlzdhzdcjgxjl.createOrReplaceTempView("_jlzdhzdcjgxjl")

    }

    // TODO: 28
    def getBdzxlgxData(sparkSession: SparkSession, url:String)={
        //变电站线路关系
//        val bdzxlgx = spark.sql(s"""select xlxdbs,bdzbs from npmis_dw_bdzxlgx where dqbm in (${cityCodeList})""")
//        bdzxlgx.createOrReplaceTempView("_bdzxlgx")

        val table ="impala::csg_ods_yx.dw_bdzxlgx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val bdzxlgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        bdzxlgx1.createOrReplaceTempView("_bdzxlgx")
        val bdzxlgx=sparkSession.sql(
            s"""
               |select xlxdbs,bdzbs from _bdzxlgx where dqbm in (${cityCodeList})
             """.stripMargin)
        bdzxlgx.createOrReplaceTempView("_bdzxlgx")
    }


    // TODO: 29
    def getBdzData(sparkSession: SparkSession, url:String)={
        //变电站

//        val bdz = spark.sql(s"""select bdzbs,bdzbh,bdzmc from npmis_dw_bdz where dqbm in (${cityCodeList})""")
//        bdz.createOrReplaceTempView("_bdz")

        val table ="impala::csg_ods_yx.dw_bdz"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val bdz1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()

        bdz1.createOrReplaceTempView("_bdz")
        val bdz =sparkSession.sql(
            s"""
               |select bdzbs,bdzbh,bdzmc from _bdz where dqbm in (${cityCodeList})
             """.stripMargin)
        bdz.createOrReplaceTempView("_bdz")

    }

    // TODO: 30
    def getYwjlData(sparkSession: SparkSession, url:String)={
        //业务级联
//        val ywjl = spark.sql("select ywbmmc,ywbm,sjywbm from npmis_xt_ywjl ")
//        ywjl.createOrReplaceTempView("_ywjl")

        val table ="impala::csg_ods_yx.xt_ywjl"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val ywjl1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        ywjl1.createOrReplaceTempView("_ywjl")
        val ywjl =sparkSession.sql(
            s"""
               |select ywbmmc,ywbm,sjywbm from _ywjl
             """.stripMargin)
        ywjl.createOrReplaceTempView("_ywjl")

    }


    // TODO: 31
    def getGzdxxlsData(sparkSession: SparkSession, url:String)={
        //工作单信息历史
//        val gzdxxls = spark.sql(s"""select gzdbh,ywlb,ywfl,sqsj,wcsj,wcsj,gzdzt,slbs,gddwbm from npmis_xt_gzdxxls where dqbm in (${cityCodeList})""")
//        gzdxxls.createOrReplaceTempView("_gzdxxls")

        val table ="impala::csg_ods_yx.xt_gzdxxls"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gzdxxls1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gzdxxls1.createOrReplaceTempView("_gzdxxls")
        val gzdxxls =sparkSession.sql(
            s"""
               |select gzdbh,ywlb,ywfl,sqsj,wcsj,wcsj,gzdzt,slbs,gddwbm from _gzdxxls where dqbm in (${cityCodeList})
             """.stripMargin)
        gzdxxls.createOrReplaceTempView("_gzdxxls")

    }

    // TODO: 32
    def getZzData(sparkSession: SparkSession, url:String)={
        //组织  zzbm-组织编号,zzmc-组织名称

//        val zz = spark.sql("select zzbm,zzmc,zzlxdm,dqbm from npmis_xt_zz")
//        zz.createOrReplaceTempView("_zz")

        val table ="impala::csg_ods_yx.xt_zz"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val zz1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        zz1.createOrReplaceTempView("_zz")
        val zz =sparkSession.sql(
            s"""
               |select zzbm,zzmc,zzlxdm,dqbm from _zz
             """.stripMargin)
        zz.createOrReplaceTempView("_zz")

    }

    // TODO: 33
    def getDmbmData(sparkSession: SparkSession, url:String)={
        //代码编码
//        val dmbm = spark.sql("select dmbm,dmbmmc,dmfl from npmis_xt_dmbm")
//        dmbm.createOrReplaceTempView("_dmbm")

        val table ="impala::csg_ods_yx.xt_dmbm"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val dmbm1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()

        dmbm1.createOrReplaceTempView("_dmbm")
        val dmbm=sparkSession.sql(
            s"""
               |select dmbm,dmbmmc,dmfl from _dmbm
               |
             """.stripMargin)
        dmbm.createOrReplaceTempView("_dmbm")

    }

    // TODO: 34
    def getXlgldw(sparkSession: SparkSession, url:String)={
        //线路管理单位
//        val xlgldw = spark.sql(s"""select * from npmis_dw_xlgldw where dqbm in (${cityCodeList})""")
//        xlgldw.createOrReplaceTempView("_xlgldw")

        val table ="impala::csg_ods_yx.dw_xlgldw"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xlgldw1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xlgldw1.createOrReplaceTempView("_xlgldw")
        val xlgldw =sparkSession.sql(
            s"""
               |select * from _xlgldw where dqbm in (${cityCodeList})
             """.stripMargin)
        xlgldw.createOrReplaceTempView("_xlgldw")

    }



    // TODO: 35
    def getYkgzdjbxxData(sparkSession: SparkSession, url:String)={
        //业扩单
//        val ykgzdjbxx = spark.sql(s"""select gzdbh,yhbh,slbs,ywlbbh,zsslrq,gzdztdm,gdsj from npmis_fw_ykgzdjbxx where dqbm in (${cityCodeList})""")
//        ykgzdjbxx.createOrReplaceTempView("_ykgzdjbxx")

        val table ="impala::csg_ods_yx.fw_ykgzdjbxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val ykgzdjbxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        ykgzdjbxx1.createOrReplaceTempView("_ykgzdjbxx")
        val ykgzdjbxx =sparkSession.sql(
            s"""
               |select gzdbh,yhbh,slbs,ywlbbh,zsslrq,gzdztdm,gdsj from _ykgzdjbxx where dqbm in (${cityCodeList})
             """.stripMargin)
        ykgzdjbxx.createOrReplaceTempView("_ykgzdjbxx")

    }


    // TODO: 36
    def getWfrt_task_exec_info(sparkSession: SparkSession, url:String)={
        //
//        val wfrt_task_exec_info = spark.sql(s"""select instance_id,tache_name,task_commit_time from npmis_wfrt_task_exec_info""")
//        wfrt_task_exec_info.createOrReplaceTempView("_wfrt_task_exec_info")

        val table ="impala::csg_ods_yx.wfrt_task_exec_info"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val wfrt_task_exec_info1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        wfrt_task_exec_info1.createOrReplaceTempView("_wfrt_task_exec_info")
        val wfrt_task_exec_info =sparkSession.sql(
            s"""
               |select instance_id,tache_name,task_commit_time from _wfrt_task_exec_info
               |
             """.stripMargin)
        wfrt_task_exec_info.createOrReplaceTempView("_wfrt_task_exec_info")

    }


    // TODO: 37
    def getXsbbfqxstjxxData(sparkSession: SparkSession, url:String)={
        //分区线损统计信息

//        val xsbbfqxstjxx = spark.sql(s"""select gddwbm,byxsl,bygdl,bysdl,bbny ny,khzb,khzbxx from NPMIS_GK_XSBBFQXSTJXX""")
//        xsbbfqxstjxx.createOrReplaceTempView("_fqxstjxx")

        val table ="impala::csg_ods_yx.GK_XSBBFQXSTJXX"  //换小写把
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xsbbfqxstjxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xsbbfqxstjxx1.createOrReplaceTempView("_fqxstjxx")
        val xsbbfqxstjxx =sparkSession.sql(
            s"""
               |select gddwbm,byxsl,bygdl,bysdl,bbny ny,khzb,khzbxx from _fqxstjxx
             """.stripMargin)
        xsbbfqxstjxx.createOrReplaceTempView("_fqxstjxx")


    }


    // TODO: 38
    def getNzwot_sp_so_runmode_deviceData(sparkSession: SparkSession, url:String)={
        //2020-01-13

//        val nzwot_sp_so_runmode_device = spark.sql(
//            s"""select execute_begin_time,execute_end_time,rolloff_line_id,rollon_line_id,mode_id
//               |from nzwot_sp_so_runmode_device""".stripMargin)
//        nzwot_sp_so_runmode_device.createOrReplaceTempView("_nzwot_sp_so_runmode_device")

        val table ="nzwot_sp_so_runmode_device"  这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val nzwot_sp_so_runmode_device1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        nzwot_sp_so_runmode_device1.createOrReplaceTempView("_nzwot_sp_so_runmode_device")
        val nzwot_sp_so_runmode_device =sparkSession.sql(
            s"""
               |select execute_begin_time,execute_end_time,rolloff_line_id,rollon_line_id,mode_id
               |from _nzwot_sp_so_runmode_device
               |
             """.stripMargin)
        nzwot_sp_so_runmode_device.createOrReplaceTempView("_nzwot_sp_so_runmode_device")

    }


    // TODO: 39
    def getNzwot_sp_so_runmode_manageData(sparkSession: SparkSession, url:String)={
//        val nzwot_sp_so_runmode_manage = spark.sql(
//            s"""select code,id,mode_type,mode_state,apply_time,turn_power_reason,turn_power_reason_type
//               |from nzwot_sp_so_runmode_manage""".stripMargin)
//        nzwot_sp_so_runmode_manage.createOrReplaceTempView("_nzwot_sp_so_runmode_manage")

        val table ="nzwot_sp_so_runmode_manage"  这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val nzwot_sp_so_runmode_manage1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        nzwot_sp_so_runmode_manage1.createOrReplaceTempView("_nzwot_sp_so_runmode_manage")
        val nzwot_sp_so_runmode_manage =sparkSession.sql(
            s"""
               |select code,id,mode_type,mode_state,apply_time,turn_power_reason,turn_power_reason_type
               |from _nzwot_sp_so_runmode_manage
             """.stripMargin)
        nzwot_sp_so_runmode_manage.createOrReplaceTempView("_nzwot_sp_so_runmode_manage")


    }

    // TODO: 40
    def getHwczxxwhData(sparkSession: SparkSession, url:String)={
//        val hwczxxwh = spark.sql(s"""select xlxdbs,xsny from npmis_gk_hwczxxwh where dqbm in (${cityCodeList})""")
//        hwczxxwh.createOrReplaceTempView("_hwczxxwh")

        val table ="impala::csg_ods_yx.gk_hwczxxwh"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val hwczxxwh1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        hwczxxwh1.createOrReplaceTempView("_hwczxxwh")
        val hwczxxwh =sparkSession.sql(
            s"""
               |select xlxdbs,xsny from _hwczxxwh where dqbm in (${cityCodeList})
             """.stripMargin)
        hwczxxwh.createOrReplaceTempView("_hwczxxwh")
    }


    // TODO: 41
    def getHbxxjlData(sparkSession: SparkSession, url:String)={
        //11月18号新增
        //换表信息记录
//        val hbxxjl = spark.sql(s"""select yxdnbbs,dfny from npmis_lc_hbxxjl where dqbm in (${cityCodeList})""")
//        hbxxjl.createOrReplaceTempView("_hbxxjl")

        val table ="impala::csg_ods_yx.lc_hbxxjl"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val hbxxjl1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        hbxxjl1.createOrReplaceTempView("_hbxxjl")
        val hbxxjl =sparkSession.sql(
            s"""
               |select yxdnbbs,dfny from _hbxxjl where dqbm in (${cityCodeList})
             """.stripMargin)
        hbxxjl.createOrReplaceTempView("_hbxxjl")
    }



    // TODO: 42
    /*------这个是从已有的临时表处理数据-----------------------------------------------------------------------*/
    //变电站考核表
    def getBdzkhbData(sparkSession: SparkSession, url:String)={
          val bdzkhb =
              s"""
                 |select distinct l.xlxdbs,y.yhbh
                 |from _all_xlxd l
                 |inner join _jld j on j.xlxdbs = l.xlxdbs
                 |inner join _ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '80'
                 |where l.xlyxzt = '01' and l.dydjdm not in ('10','12','13','15') and l.xllbdm = '1'
                 |
               """.stripMargin

        sparkSession.sql(bdzkhb).createOrReplaceTempView ("_bdzkhb")

          }


    //台区考核表
    def getTqkhbData(sparkSession: SparkSession, url:String)={
        val tqkhb =
            s"""
               |select distinct t.tqbs,y.yhbh
               |from _all_tq t
               |inner join _jld j on j.tqbs = t.tqbs
               |inner join _ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '60'
               |where t.yxztdm = '01'
               |
             """.stripMargin

        sparkSession.sql(tqkhb).createOrReplaceTempView("_tqkhb")

    }


    //变电站关联表
    def getBdzglb(sparkSession: SparkSession, url:String)={
        val bdzglb = sparkSession.sql(
            s"""
               |select bx.xlxdbs,bd.bdzbs,bd.bdzbh,bd.bdzmc
               |from _bdzxlgx bx
               |join _bdz bd on bd.bdzbs = bx.bdzbs
               |
             """.stripMargin)
        bdzglb.createOrReplaceTempView("_bdzglb")
    }


    /*--------------------------------------------------------------------------------------------------------*/




    // TODO: 43
    def getGdljfhzxx(sparkSession: SparkSession, url:String)={
        //购电量价费汇总信息表 2019年11月19日11:23:10 lixc

//        val gdljfhzxx = spark.sql(s"""select dcbh,gdyf from NPMIS_SC_GDLJFHZXX where dqbm in (${cityCodeList})""")
//        gdljfhzxx.createOrReplaceTempView("_gdljfhzxx")

        val table ="impala::csg_ods_yx.SC_GDLJFHZXX"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gdljfhzxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gdljfhzxx1.createOrReplaceTempView("_gdljfhzxx")
        val gdljfhzxx =sparkSession.sql(
            s"""
               |select dcbh,gdyf from _gdljfhzxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gdljfhzxx.createOrReplaceTempView("_gdljfhzxx")

    }


    // TODO: 44
    def getDcfzxx(sparkSession: SparkSession, url:String)={
        //电厂辅助信息表 2019年11月19日11:23:14 lixc

//        val dcfzxx = spark.sql(s"""select dcbh,dydj,gdldz from NPMIS_SC_DCFZXX where dqbm in (${cityCodeList})""")
//        dcfzxx.createOrReplaceTempView("_dcfzxx")

        val table ="impala::csg_ods_yx.SC_DCFZXX"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val dcfzxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        dcfzxx1.createOrReplaceTempView("_dcfzxx")
        val dcfzxx =sparkSession.sql(
            s"""
               |select dcbh,dydj,gdldz from _dcfzxx where dqbm in (${cityCodeList})
             """.stripMargin)
        dcfzxx.createOrReplaceTempView("_dcfzxx")

    }


    // TODO: 45
    def getJsdy(sparkSession: SparkSession, url:String)={
        //结算单元表 2019年11月19日11:23:14 lixc

//        val jsdy = spark.sql(s"""select dcbh,fgbs,gsgx from npmis_sc_jsdy""")
//        jsdy.createOrReplaceTempView("_jsdy")

        val table ="impala::csg_ods_yx.sc_jsdy"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val jsdy1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        jsdy1.createOrReplaceTempView("_jsdy")
        val jsdy =sparkSession.sql(
            s"""
               |select dcbh,fgbs,gsgx from _jsdy
             """.stripMargin)
        jsdy.createOrReplaceTempView("_jsdy")
    }


    // TODO: 46
    /*---------------------这个是要从已形成的临时表处理数据----------------------------------------------*/
    def getXldfdcswdlData(sparkSession: SparkSession, url:String)={
        //线路地方电厂上网电量 2019年11月19日11:14:42 lixc

          val xldfdcswdl =
              s"""
                 |select a.xlxdbs,sum(coalesce(a.swdl,0)) swdl
                 |from (
                 |	select dy.xlxdbs,c.jldbh,c.yhbh,sum(coalesce(c.bjdl,0)+coalesce(c.jbdl,0)) swdl,'BFFG' fgbz
                 |	from _cbxx c
                 |	join _yhdyxx dy on dy.yhbh = c.yhbh
                 |	join _ydkh y on y.yhbh = c.yhbh and y.yhztdm <> '2'
                 |	where c.dfny = ${nowMonth} and c.sslxdm = '221' and c.yhlbdm = '40'
                 |	and exists (select 1 from _gdljfhzxx a,_dcfzxx b
                 |		where c.yhbh = a.dcbh and c.dfny = a.gdyf
                 |		and b.dcbh = a.dcbh and b.dydj = '08' and b.gdldz <> '2')
                 |	group by dy.xlxdbs,c.jldbh,c.yhbh
                 |
                 |  union all
                 |	select dy.xlxdbs,c.jldbh,c.yhbh,sum(coalesce(c.bjdl,0)+coalesce(c.jbdl,0)) swdl,'FFG' fgbz
                 |	from _cbxx c
                 |	join _yhdyxx dy on dy.yhbh = c.yhbh
                 |	join _ydkh y on y.yhbh = c.yhbh and y.yhztdm <> '2'
                 |	where c.dfny = ${nowMonth} and c.sslxdm in ('222','223','224') and c.yhlbdm = '40'
                 |	and exists (select 1 from _gdljfhzxx a,_dcfzxx b
                 |		where c.yhbh = a.dcbh and c.dfny = a.gdyf
                 |		and b.dcbh = a.dcbh and b.dydj = '08' and b.gdldz <> '2')
                 |	group by dy.xlxdbs,c.jldbh,c.yhbh
                 |) a
                 |where a.fgbz = (case when (select max(e.fgbs) from _jsdy e where e.gsgx = '2' and e.dcbh = a.yhbh) = '0' then 'BFFG' else 'FFG' end)
                 |group by a.xlxdbs
                 |
                 |
             """.stripMargin
        sparkSession.sql(xldfdcswdl).createOrReplaceTempView("_xldfdcswdl")

          }


    def getTqdfdcswdlData(sparkSession: SparkSession, url:String)={
        //台区地方电厂上网电量 2019年11月19日11:14:42 lixc
        val tqdfdcswdl =
            s"""
               |select dy.tqbs,sum(coalesce(c.bjdl,0)+coalesce(c.jbdl,0)) swdl
               |from _cbxx c
               |join _yhdyxx dy on dy.yhbh = c.yhbh
               |join _ydkh y on y.yhbh = c.yhbh and y.yhztdm <> '2'
               |where c.dfny = ${nowMonth} and c.sslxdm = '221' and c.yhlbdm = '40'
               |and exists (select 1 from _gdljfhzxx a,_dcfzxx b
               |		where c.yhbh = a.dcbh and c.dfny = a.gdyf
               |		and b.dcbh = a.dcbh and b.dydj in ('02','03'))
               |group by dy.tqbs
               |
             """.stripMargin
        sparkSession.sql(tqdfdcswdl).createOrReplaceTempView("_tqdfdcswdl")

    }


    // TODO: 47
    def getBsbzData(sparkSession: SparkSession, url:String)={

        //变损标准表 2019年11月20日17:04:22 lixc

//        val bsbz = spark.sql(s"""select byqrl,shlxdm,ygbs from npmis_hs_bsbz""")
//        bsbz.createOrReplaceTempView("_bsbz")

        val table ="impala::csg_ods_yx.hs_bsbz"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val bsbz1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        bsbz1.createOrReplaceTempView("_bsbz")
        val bsbz =sparkSession.sql(
            s"""
               |select byqrl,shlxdm,ygbs from _bsbz
             """.stripMargin)
        bsbz.createOrReplaceTempView("_bsbz")

    }




    // TODO: 48
    /*-----------这个是要从已形成的临时表处理数据---------------------------------------------------------*/
    def getByqygdshData(sparkSession: SparkSession, url:String)={
        //变压器月固定损耗 2019年11月20日17:04:28 lixc
           val byqygdsh =
               s"""
                  |select t.sbbs,min(b.ygbs) zxygbs
                  |from _yxbyq t
                  |join _bsbz b on b.byqrl = t.edrl and b.shlxdm = t.shlxdm
                  |where t.gbzbbz = 1 and t.yxztdm = '01'
                  |group by t.sbbs
                  |
             """.stripMargin
        sparkSession.sql(byqygdsh).createOrReplaceTempView("_byqygdsh")

    }

    /*-------------------------------------------------------------------*/




    // TODO: 49
    def getDzhyjgzdxxData(sparkSession: SparkSession, url:String)={
        //电子化移交工作单信息表 2019年11月21日19:40:58 lixc

//        val dzhyjgzdxx = spark.sql(s"""select dzhyjgdh,yhbh,ykgdbh from npmis_yp_dzhyjgzdxx""")
//        dzhyjgzdxx.createOrReplaceTempView("_dzhyjgzdxx")

        val table ="impala::csg_ods_yx.yp_dzhyjgzdxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val dzhyjgzdxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()

        dzhyjgzdxx1.createOrReplaceTempView("_dzhyjgzdxx")
        val dzhyjgzdxx =sparkSession.sql(
            s"""
               |select dzhyjgdh,yhbh,ykgdbh from _dzhyjgzdxx
             """.stripMargin)
        dzhyjgzdxx.createOrReplaceTempView("_dzhyjgzdxx")
    }


    // TODO: 50
    def getRyData(sparkSession: SparkSession, url:String)={
        //20200108
//        val ry = spark.sql("select rybs,rymc from npmis_xt_ry")
//        ry.createOrReplaceTempView("_ry")

        val table ="impala::csg_ods_yx.xt_ry"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val ry1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        ry1.createOrReplaceTempView("_ry")
        val ry =sparkSession.sql(
            s"""
               |select rybs,rymc from _ry
               |
             """.stripMargin)
        ry.createOrReplaceTempView("_ry")

    }

    // TODO: 51
    def getTranscircuitsupplyData(sparkSession: SparkSession, url:String)={
//        spark.sql("use gis")
        // gis系统  trans_id-跟变压器的gisid对应, circuit_id-跟线路线段的gisid对应
//        val transcircuitsupply = spark.sql("select trans_id,circuit_id,main_supply from gis_transcircuitsupply")
//        transcircuitsupply.createOrReplaceTempView("_transcircuitsupply")

        val table ="gis_transcircuitsupply"  这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val transcircuitsupply1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()

        transcircuitsupply1.createOrReplaceTempView("_transcircuitsupply")
        val transcircuitsupply =sparkSession.sql(
            s"""
               |select trans_id,circuit_id,main_supply from _transcircuitsupply
             """.stripMargin)
        transcircuitsupply.createOrReplaceTempView("_transcircuitsupply")

    }


    // TODO: 52
    def getGis_lvcustomersupply(sparkSession: SparkSession, url:String)={
//        val gis_lvcustomersupply = spark.sql("select trans_id,customer_mrid from gis_lvcustomersupply")
//        gis_lvcustomersupply.createOrReplaceTempView("_gis_lvcustomersupply")

        val table ="gis_lvcustomersupply"  这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gis_lvcustomersupply1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gis_lvcustomersupply1.createOrReplaceTempView("_gis_lvcustomersupply")
        val gis_lvcustomersupply =sparkSession.sql(
            s"""
               |select trans_id,customer_mrid from _gis_lvcustomersupply
             """.stripMargin)
        gis_lvcustomersupply.createOrReplaceTempView("_gis_lvcustomersupply")

    }


    // TODO: 53
    def getGis_g_dm_function_locationData(sparkSession: SparkSession, url:String)={
//        val gis_g_dm_function_location = spark.sql("select id,classify_id,xfmr_type,fl_name from gis_g_dm_function_location")
//        gis_g_dm_function_location.createOrReplaceTempView("_gis_g_dm_function_location")

        val table ="gis_g_dm_function_location"这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gis_g_dm_function_location1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gis_g_dm_function_location1.createOrReplaceTempView("_gis_g_dm_function_location")
        val gis_g_dm_function_location =sparkSession.sql(
            s"""
               |select id,classify_id,xfmr_type,fl_name from _gis_g_dm_function_location1
             """.stripMargin)
        gis_g_dm_function_location.createOrReplaceTempView("_gis_g_dm_function_location")

    }



    // TODO: 54   有自定义udf
    /*--------------这个是要从已形成的临时表处理数据-----------------------------------------------------*/
    def getyYwlbBroadcastData(sparkSession: SparkSession, url:String)={
        //    spark.sql("use yxxt")

        //2020.02.21  获取业务类别代码
        val ywlbList= sparkSession.sql("select ywbm,ywbmmc from _ywjl").collect()
        val ywlbMap = scala.collection.mutable.Map[String, String]()
        for (ywlb <- ywlbList) {
            ywlbMap.put(ywlb.getAs[String]("ywbm"), ywlb.getAs[String]("ywbmmc"))
        }
        val ywlbBroadcast= sparkSession.sparkContext.broadcast(ywlbMap)

        sparkSession.udf.register("getywlbmc", (ywlb: String) => {
            ywlbBroadcast.value.get(ywlb).getOrElse(null)

        })
    }


    /*-------------------------------------------------------------------*/



    // TODO: 55
    def getYcgddwxlgxData(sparkSession: SparkSession, url:String)={
        //异常供电单位线路对应关系

//        val ycgddwxlgx = spark.sql(s"select * from ${writeSchema}.ycgddwxlgx")
//        ycgddwxlgx.createOrReplaceTempView("_ycgddwxlgx")

        val table ="ycgddwxlgx"这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val ycgddwxlgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        ycgddwxlgx1.createOrReplaceTempView("_ycgddwxlgx")
        val ycgddwxlgx =sparkSession.sql(
            s"""
               |select * from _ycgddwxlgx
             """.stripMargin)
        ycgddwxlgx.createOrReplaceTempView("_ycgddwxlgx")
    }


    // TODO: 56
    def getXsycgzData(sparkSession: SparkSession, url:String)={
        //异常规则表
//        val xsycgz = spark.sql("select * from gpsxzq.GK_XSYCGZ")
//        xsycgz.createOrReplaceTempView("_xsycgz")

        val table ="GK_XSYCGZ"这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xsycgz1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xsycgz1.createOrReplaceTempView("_xsycgz")
        val xsycgz =sparkSession.sql(
            s"""
               |select * from _xsycgz
             """.stripMargin)
        xsycgz.createOrReplaceTempView("_xsycgz")
        xsycgz

    }


    // TODO: 57
    def getGdjycctbxxData(sparkSession: SparkSession, url:String)={
        //lixc20200506新加 购电交易差错退补信息
//        val gdjycctbxx = spark.sql(s"""select gddwbm,ccyf,dcbh,dcmc,tbdl,jsdybh,tbzt,dqbm from npmis_sc_gdjycctbxx where dqbm in (${cityCodeList})""")
//        gdjycctbxx.createOrReplaceTempView("_gdjycctbxx")

        val table ="impala::csg_ods_yx.sc_gdjycctbxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val gdjycctbxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        gdjycctbxx1.createOrReplaceTempView("_gdjycctbxx")
        val gdjycctbxx =sparkSession.sql(
            s"""
               |select
               |    gddwbm,ccyf,dcbh,dcmc,tbdl,jsdybh,tbzt,dqbm
               |from _gdjycctbxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        gdjycctbxx.createOrReplaceTempView("_gdjycctbxx")
    }


    // TODO: 58
    def getDcjzxxData(sparkSession: SparkSession, url:String)={
        //lixc20200506新加 电厂机组信息
//        val dcjzxx = spark.sql(s"""select jsdybh,jldbh,dqbm from npmis_sc_dcjzxx where dqbm in (${cityCodeList})""")
//        dcjzxx.createOrReplaceTempView("_dcjzxx")

        val table ="npmis_sc_dcjzxx"这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val dcjzxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        dcjzxx1.createOrReplaceTempView("_dcjzxx")
        val dcjzxx =sparkSession.sql(
            s"""
               |select jsdybh,jldbh,dqbm from _dcjzxx where dqbm in (${cityCodeList})
             """.stripMargin)
        dcjzxx.createOrReplaceTempView("_dcjzxx")

    }

    // TODO: 59
    def getCbqdfzData(sparkSession: SparkSession, url:String)={
        //lixc20200506新加 电厂机组信息

//        val cbqdfz = spark.sql(s"""select cbqdbh,cblr,dqbm from npmis_lc_cbqdfz where dqbm in (${cityCodeList})""")
//        cbqdfz.createOrReplaceTempView("_cbqdfz")

        val table ="impala::csg_ods_yx.lc_cbqdfz"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val cbqdfz1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        cbqdfz1.createOrReplaceTempView("_cbqdfz")
        val cbqdfz =sparkSession.sql(
            s"""
               |select cbqdbh,cblr,dqbm from _cbqdfz where dqbm in (${cityCodeList})
             """.stripMargin)
        cbqdfz.createOrReplaceTempView("_cbqdfz")

    }

    // TODO: 60
    def getXszbgyyhData(sparkSession: SparkSession, url:String)={
        //lixc20200506新加 线损专变公用用户
//        val xszbgyyh = spark.sql(s"""select xlxdbs,yhbh from npmis_gk_xszbgyyh where dqbm in (${cityCodeList})""")
//        xszbgyyh.createOrReplaceTempView("_xszbgyyh")

        val table ="impala::csg_ods_yx.gk_xszbgyyh"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xszbgyyh1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        xszbgyyh1.createOrReplaceTempView("_xszbgyyh")
        val xszbgyyh =sparkSession.sql(
            s"""
               |select xlxdbs,yhbh from _xszbgyyh where dqbm in (${cityCodeList})
             """.stripMargin)
        xszbgyyh.createOrReplaceTempView("_xszbgyyh")

    }

    // TODO: 61
    def getHs_jldgxData(sparkSession: SparkSession, url:String)={
        //lixc20200506新加 核算计量点关系
//        val hs_jldgx = spark.sql(s"""select yhbh,glyhbh,dfny from npmis_hs_jldgx where dqbm in (${cityCodeList})""")
//        hs_jldgx.createOrReplaceTempView("_hs_jldgx")

        val table ="impala::csg_ods_yx.hs_jldgx_d"  //hs_jldgx_d
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val hs_jldgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        hs_jldgx1.createOrReplaceTempView("_hs_jldgx")
        val hs_jldgx =sparkSession.sql(
            s"""
               |select yhbh,glyhbh,dfny from _hs_jldgx where dqbm in (${cityCodeList})
             """.stripMargin)
        hs_jldgx.createOrReplaceTempView("_hs_jldgx")

    }


    // TODO: 62
    def getFzqjfsqData(sparkSession: SparkSession, url:String)={
        //lixc20200506新加 非周期计费申请
//        val fzqjfsq = spark.sql(s"""select gzdbh,yhbh,ywlbdm,ywzlbh,cldfny,clgzdbh from npmis_hs_fzqjfsq where dqbm in (${cityCodeList})""")
//        fzqjfsq.createOrReplaceTempView("_fzqjfsq")

        val table ="impala::csg_ods_yx.hs_fzqjfsq"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val fzqjfsq1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        fzqjfsq1.createOrReplaceTempView("_fzqjfsq")
        val fzqjfsq =sparkSession.sql(
            s"""
               |select gzdbh,yhbh,ywlbdm,ywzlbh,cldfny,clgzdbh from _fzqjfsq where dqbm in (${cityCodeList})
             """.stripMargin)
        fzqjfsq.createOrReplaceTempView("_fzqjfsq")
    }


    // TODO: 63
    def get_jldgxData(sparkSession: SparkSession, url:String)={
        //lixc20200604新加 计量点关系

//        val _jldgx = spark.sql(s"""select jldbh,gxlxdm,gljldbh,yhbh,glyhbh,dqbm from npmis_kh_jldgx where dqbm in (${cityCodeList})""")
//        _jldgx.createOrReplaceTempView("_jldgx")

        val table ="impala::csg_ods_yx.kh_jldgx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val _jldgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        _jldgx1.createOrReplaceTempView("_jldgx")
        val _jldgx =sparkSession.sql(
            s"""
               |select jldbh,gxlxdm,gljldbh,yhbh,glyhbh,dqbm from _jldgx where dqbm in (${cityCodeList})
             """.stripMargin)
        _jldgx.createOrReplaceTempView("_jldgx")
    }


    // TODO: 64
    def getJlzdhgzrwxxData(sparkSession: SparkSession, url:String)={
        //lixc20200520新加 计量自动化故障任务信息

//        val jlzdhgzrwxx = spark.sql(s"""select rwbs,gzdbh,yhbh,gzqkjs,gzfxf,xxlydm,gzfxrq,pgrq from npmis_sb_jlzdhgzrwxx where dqbm in (${cityCodeList})""")
//        jlzdhgzrwxx.createOrReplaceTempView("_jlzdhgzrwxx")

        val table ="impala::csg_ods_yx.sb_jlzdhgzrwxx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val jlzdhgzrwxx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        jlzdhgzrwxx1.createOrReplaceTempView("_jlzdhgzrwxx")
        val jlzdhgzrwxx =sparkSession.sql(
            s"""
               |select rwbs,gzdbh,yhbh,gzqkjs,gzfxf,xxlydm,gzfxrq,pgrq from _jlzdhgzrwxx where dqbm in (${cityCodeList})
             """.stripMargin)
        jlzdhgzrwxx.createOrReplaceTempView("_jlzdhgzrwxx")
    }


    // TODO: 65
    def getJlzdhgzrwmxData(sparkSession: SparkSession, url:String)={
        //lixc20200520新加 计量自动化故障任务明细

//        val jlzdhgzrwmx = spark.sql(s"""select rwmxbs,rwbs,gzdbh,yhbh,jldbh,zcbh,ccbh,sblbdm,sblx,zdgzlx,zdgzxx,zdgzxldm from npmis_sb_jlzdhgzrwmx where dqbm in (${cityCodeList})""")
//        jlzdhgzrwmx.createOrReplaceTempView("_jlzdhgzrwmx")

        val table ="impala::csg_ods_yx.sb_jlzdhgzrwmx"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val jlzdhgzrwmx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        jlzdhgzrwmx1.createOrReplaceTempView("_jlzdhgzrwmx")
        val jlzdhgzrwmx =sparkSession.sql(
            s"""
               |select rwmxbs,rwbs,gzdbh,yhbh,jldbh,zcbh,ccbh,sblbdm,sblx,zdgzlx,zdgzxx,zdgzxldm from _jlzdhgzrwmx where dqbm in (${cityCodeList})
             """.stripMargin)
        jlzdhgzrwmx.createOrReplaceTempView("_jlzdhgzrwmx")

    }


    // TODO: 66
    def getHbzdgxData(sparkSession: SparkSession, url:String)={
        //lixc20200602三相负荷不平衡率tq_41 start
        //户表终端关系

//        val hbzdgx = spark.sql(s"""
//                                  |select
//                                  |  a.cldbs,
//                                  |  a.bjlx,
//                                  |  a.yhbh,
//                                  |  a.jldbh,
//                                  |  a.bjzcbh,
//                                  |  a.zhbl,
//                                  |  a.cldlxdm,
//                                  |  a.jldbs,
//                                  |  min(a.etl_time) etl_time
//                                  |from tmr.hbzdgx a
//                                  |where a.etl_city in (${cityNameList})
//                                  |group by a.cldbs,
//                                  |           a.bjlx,
//                                  |           a.yhbh,
//                                  |           a.jldbh,
//                                  |           a.bjzcbh,
//                                  |           a.zhbl,
//                                  |           a.cldlxdm,
//                                  |           a.jldbs""".stripMargin)
//        hbzdgx.createOrReplaceTempView("_hbzdgx")



        val table ="tmr.hbzdgx" 这个表没有
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val hbzdgx1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        hbzdgx1.createOrReplaceTempView("_hbzdgx")
        val hbzdgx =sparkSession.sql(
            s"""
               |select
               |  a.cldbs,
               |  a.bjlx,
               |  a.yhbh,
               |  a.jldbh,
               |  a.bjzcbh,
               |  a.zhbl,
               |  a.cldlxdm,
               |  a.jldbs,
               |  min(a.etl_time) etl_time
               |from _hbzdgx1 a
               |where a.etl_city in (${cityNameList})
               |group by a.cldbs,
               |           a.bjlx,
               |           a.yhbh,
               |           a.jldbh,
               |           a.bjzcbh,
               |           a.zhbl,
               |           a.cldlxdm,
               |           a.jldbs
               |
             """.stripMargin)
        hbzdgx.createOrReplaceTempView("_hbzdgx")


    }


    // TODO: 67    这个有疑问！
        //表格作变量传入
    def getClddldyData(sparkSession: SparkSession, url:String)={
        //测量点电流电压

//        val clddldy = spark.sql(s"""
//                    select * from (
//                    select d.*,row_number() over(partition by d.cldbs,d.sjsj order by d.etl_file_timtstamp desc) as num
//                    from ${tableNameC} d where d.sjsj like '${_nowMonth}%' and d.etl_city in (${cityNameList})
//                    order by d.sjsj
//                    ) t where t.num = 1
//                    """)
//        clddldy.createOrReplaceTempView("_clddldy")


        val table =tableNameC                              //这个怎么用
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val clddldy1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        clddldy1.createOrReplaceTempView("_clddldy")
        val clddldy =sparkSession.sql(
            s"""
               |select *
               |from (
               |	select d.*,
               |		row_number() over(partition by d.cldbs,d.sjsj order by d.etl_file_timtstamp desc) as num
               |	from _clddldy d
               |	where d.sjsj like '${_nowMonth}%' and d.etl_city in (${cityNameList})
               |	order by d.sjsj
               |	) t
               |where t.num = 1
               |
             """.stripMargin)
            clddldy.createOrReplaceTempView("_clddldy")

    }



    // TODO: 68
    /*--------------这个是要从已形成的临时表处理数据-----------------------------------------------------*/
    def getSxfhbphlData(sparkSession: SparkSession, url:String)={
        //三相负荷不平衡率   zddl 是什么

        val sxfhbphl = sparkSession.sql(
            s"""
               |select a.yhbh,sum(case when a.bphl > 30 then 1 else 0 end) bphsl,count(a.bphl) zsl,
               |	round(sum(case when a.bphl > 30 then 1 else 0 end)/count(a.bphl)*100,2) sxfhbphl
               |from (
               |	select h.yhbh,h.jldbh,h.bjzcbh,d.cldbs,d.sjsj,d.adl,d.bdl,d.cdl,
               |		zddl(d.adl,d.bdl,d.cdl) zddl,zxdl(d.adl,d.bdl,d.cdl) zxdl,round((zddl(d.adl,d.bdl,d.cdl)-zxdl(d.adl,d.bdl,d.cdl))/zddl(d.adl,d.bdl,d.cdl)*100,2) bphl
               |	from _ydkh y
               |	join _hbzdgx h on h.yhbh = y.yhbh
               |	join _clddldy d on d.cldbs = h.cldbs
               |	where y.yhlbdm = '60'
               |	) a
               |group by a.yhbh
               |
             """.stripMargin)

          sxfhbphl.createOrReplaceTempView("_sxfhbphl")
        //三相负荷不平衡率tq_41 end

    }

}
