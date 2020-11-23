package com.GDNanWangLineLoss.month.dao

import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/**
  * 数据源
  */
object OdsGetBaseDatas {

    def odsGetBaseDatas(sparkSession:SparkSession,url:String)={

        /*-----1 抄表信息--------------------------------------------------------------*/
        /*
        抄表信息（过去6个月）  yhbh-用户编号,zcbh-资产编号,zhbl-综合倍率,scbss-上次表示数,bcbss-本次表示数,
                bqcbcs-本期抄表示数,sccbrq-上次抄表日期,cbsj-抄表时间,bjdl-报警电量,jldbh-计量点编号,
                dfny-电费年月,yhmc-用户名称,jldcbfsdm-计量点抄表方式

        lixc20200506新加字段 cbsxh-抄表顺序号,sjcbfsdm-实际抄表方式代码,gzdbh-工作单编号,ywlbdm-业务类别代码,
                dqbm-地区编码,gddwbm-供电单位编码,cbrbs-抄表人标识,yddz-用电地址,cbqdbh-抄表区段编号,jldcbsxh-计量点抄表顺序号
         */

        val table1 ="impala::csg_ods_yx.lc_cbxx"    //todo 这个在南网是全部的表
        // val table2 ="npmis_lc_cbxx_his"  南网没有这个
        val kuduMap1: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table1)

        val cbxx1=sparkSession.read
          .options(kuduMap1)
          .format("org.apache.kudu.spark.kudu")
          .load()

        cbxx1.createOrReplaceTempView("cbxx")

        /*
        抄表信息  yhbh-用户编号,zcbh-资产编号,zhbl-综合倍率,scbss-上次表示数,bcbss-本次表示数,
                bqcbcs-本期抄表示数,sccbrq-上次抄表日期,cbsj-抄表时间,bjdl-报警电量,jldbh-计量点编号,
                dfny-电费年月,yhmc-用户名称,jldcbfsdm-计量点抄表方式
        lixc20200506新加字段 cbsxh-抄表顺序号,sjcbfsdm-实际抄表方式代码,gzdbh-工作单编号,ywlbdm-业务类别代码,dqbm-地区编码,gddwbm-供电单位编码,cbrbs-抄表人标识,yddz-用电地址,cbqdbh-抄表区段编号,jldcbsxh-计量点抄表顺序号
         */
        val cbxx=sparkSession.sql(
            s"""
               |select
               |    yhbh,zcbh,zhbl,scbss,bcbss,bqcbcs,sccbrq,cbsj,bjdl,jldbh,dfny,yhmc,jldcbfsdm,yxdnbbs,sslxdm,
               |    bssce,yhlbdm,jbdl,cbsxh,sjcbfsdm,gzdbh,ywlbdm,dqbm,gddwbm,cbrbs,yddz,cbqdbh,jldcbsxh
               |from cbxx
               |where dfny in (${nowMonth},${lastMonth})
               |
             """.stripMargin)
        cbxx.cache().createOrReplaceTempView("cbxx")


        /*-----2 用电客户--------------------------------------------------------------*/
        //用电客户   yhbh-用户编号,yhmc-用户名称,yhztdm-用电状态,dydjdm-电压等级代码,gddwbm-供电单位编码,
        // lhrq-立户日期,cbqdbh-抄表区段,cbzq-抄表周期
        val table2 ="impala::csg_ods_yx.kh_ydkh"  //todo 所有的 npmis的前缀全部去掉
        val kuduMap2: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table2)
        val ydkh1=sparkSession.read
          .options(kuduMap2)
          .format("org.apache.kudu.spark.kudu")
          .load()//.where(s"dqbm in (${cityCodeList})")

        ydkh1.createOrReplaceTempView("ydkh1")
        val ydkh=sparkSession.sql(
            s"""
               |select yhbh,yhmc,yhztdm,dydjdm,gddwbm,yhlbdm,lhrq,cbqdbh,cbzq,hyfldm
               |from ydkh1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        ydkh.createOrReplaceTempView("ydkh")
        sparkSession.catalog.dropTempView("ydkh1")



        /*-----3 计量点--------------------------------------------------------------*/
        //计量点   jldbh-计量点编号,jldmc-计量点名称,xlxdbs-线路线段标识,yhbh-用户编号,tqbs-台区标识,jlfsdm-计量方式,jxfsdm-接线方式代码
        //lixc20200506新加字段 jldydjdm-计量电压等级代码
        val table3 ="impala::csg_ods_yx.kh_jld"
        val kuduMap3: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table3)
        val jld1=sparkSession.read
          .options(kuduMap3)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jld1.createOrReplaceTempView("jld1")

        val jld=sparkSession.sql(
            s"""
               |select jldbh,jldmc,jldytdm,xlxdbs,yhbh,tqbs,jlfsdm,jxfsdm,jldztdm,jldlbdm,cbfsdm,gddwbm,jldydjdm
               |from jld1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        jld.createOrReplaceTempView("jld")
        sparkSession.catalog.dropTempView("jld1")


        /*----4 线路线段---------------------------------------------------------------*/
        //线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
        //lixc20200506只查配线
        val table4 ="impala::csg_ods_yx.dw_xlxd"
        val kuduMap4: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table4)
        val all_xlxd1=sparkSession.read
          .options(kuduMap4)
          .format("org.apache.kudu.spark.kudu")
          .load()

        all_xlxd1.createOrReplaceTempView("all_xlxd1")
        val all_xlxd =sparkSession.sql(
            s"""
               |select
               |    l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm
               |from all_xlxd1 l
               |where l.xllbdm = '1' and l.xlyxzt = '01' and l.dydjdm not in ('10','12','13','15') and dqbm in (${cityCodeList})
               |
             """.stripMargin)
        all_xlxd.createOrReplaceTempView("all_xlxd")  //线路线段
        sparkSession.catalog.dropTempView("all_xlxd1")
        all_xlxd


        /*----5 台区---------------------------------------------------------------*/
        //台区  tqbs-台区标识,tqbh-台区编号,tqmc-台区名称
        val table5 ="impala::csg_ods_yx.dw_tq"
        val kuduMap5: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table5)
        val all_tq1=sparkSession.read
          .options(kuduMap5)
          .format("org.apache.kudu.spark.kudu")
          .load()

        all_tq1.createOrReplaceTempView("all_tq1")
        val all_tq=sparkSession.sql(
            s"""
               |select tqbs,tqbh,tqmc,yxztdm,gddwbm,cbzq
               |from all_tq1 t
               |where t.gddwbm is not null and dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("all_tq1")
        all_tq.createOrReplaceTempView("all_tq")


        /*----6 线路台区关系---------------------------------------------------------------*/
        //线路台区关系  xlxdbs-线路线段标识,tqbs-台区标识
        val table6 ="impala::csg_ods_yx.dw_xltqgx"
        val kuduMap6: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table6)
        val xltqgx1=sparkSession.read
          .options(kuduMap6)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xltqgx1.createOrReplaceTempView("xltqgx1")
        val xltqgx =sparkSession.sql(
            s"""
               |select xlxdbs,tqbs
               |from xltqgx1 where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        xltqgx.createOrReplaceTempView("xltqgx")
        sparkSession.catalog.dropTempView("xltqgx1")



        /*----7 用户电源信息---------------------------------------------------------------*/
        //用户电源信息  xlxdbs-线路线段标识,tqbs-台区标识,yhbh-用户编号,dybh-电源编号,dyxzdm-电源状态代码
        val table7 ="impala::csg_ods_yx.kh_yhdyxx"
        val kuduMap7: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table7)
        val yhdyxx1=sparkSession.read.options(kuduMap7).format("org.apache.kudu.spark.kudu").load()
        yhdyxx1.createOrReplaceTempView("yhdyxx1")
        val yhdyxx =sparkSession.sql(
            s"""
               |select
               |    xlxdbs,tqbs,yhbh,dybh,dyxzdm
               |from yhdyxx1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("yhdyxx1")
        yhdyxx.createOrReplaceTempView("yhdyxx") //用户电源信息


        /*----8 变压器---------------------------------------------------------------*/
        // 运行变压器  sbbs-唯一标识,mc-名称,zcmrid-资产mrid ,tqbs-台区标识,yhbh-用户编号,
        // edrl-设备铭牌上登记的容量,gbzbbz-公变专变标志,edrl-额定容量,
        // shlxdm-损耗类型代码,sbxhdm-设备型号代码,yxztdm-运行状态代码,kzsh-空载损耗,gddwbm-供电单位代码

        val table8 ="impala::csg_ods_yx.dw_yxbyq"
        val kuduMap8: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table8)
        val yxbyq1=sparkSession.read
          .options(kuduMap8)
          .format("org.apache.kudu.spark.kudu")
          .load()

        yxbyq1.createOrReplaceTempView("yxbyq1")
        val yxbyq =sparkSession.sql(
            s"""
               |select
               |    sbbs,mc,zcmrid,gisid,tqbs,yhbh,gbzbbz,edrl,shlxdm,sbxhdm,yxztdm,kzsh,gddwbm
               |from yxbyq1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        sparkSession.catalog.dropTempView("yxbyq1")
        yxbyq.createOrReplaceTempView("yxbyq") //运行变压器



        /*----9 计量点运行电能表关系---------------------------------------------------------------*/
        //计量点运行电能表关系  jldbh-计量点编号,yxdnbbs-运行电能表标识
        val table9 ="impala::csg_ods_yx.kh_jlddnbgx"
        val kuduMap9: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table9)
        val jlddnbgx1=sparkSession.read
          .options(kuduMap9)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jlddnbgx1.createOrReplaceTempView("jlddnbgx1")
        val jlddnbgx =sparkSession.sql(
            s"""
               |select
               |    jldbh,yxdnbbs
               |from jlddnbgx1
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        sparkSession.catalog.dropTempView("jlddnbgx1")
        jlddnbgx.createOrReplaceTempView("jlddnbgx")  //计量点运行电能表关系



        /*----10 运行电能表---------------------------------------------------------------*/
        //运行电能表 yxdnbbs-运行电能表标识,csbs-参数标识
        val table10 ="impala::csg_ods_yx.sb_yxdnb"
        val kuduMap10: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table10)
        val yxdnb1=sparkSession.read
          .options(kuduMap10)
          .format("org.apache.kudu.spark.kudu")
          .load()

        yxdnb1.createOrReplaceTempView("yxdnb1")
        val yxdnb =sparkSession.sql(
            s"""
               |select
               |    yxdnbbs,csbs,zcbh,zhbl
               |from yxdnb1 where dqbm in (${cityCodeList})
             """.stripMargin)
        sparkSession.catalog.dropTempView("yxdnb1")
        yxdnb.createOrReplaceTempView("yxdnb") //运行电能表



        /*----11 电能表参数---------------------------------------------------------------*/
        //电能表参数  csbs-参数标识,eddydm-额定电压
        val table11 ="impala::csg_ods_yx.sb_dnbcs"
        val kuduMap11: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table11)
        val dnbcs1=sparkSession.read
          .options(kuduMap11)
          .format("org.apache.kudu.spark.kudu")
          .load()

        dnbcs1.createOrReplaceTempView("dnbcs")
        val dnbcs =sparkSession.sql(
            s"""
               |select csbs,eddydm
               |from dnbcs
             """.stripMargin)
        dnbcs.createOrReplaceTempView("dnbcs")




        /*----12 计量点信息---------------------------------------------------------------*/
        //计量点信息(过去6个月) yhbh-用户编号,yhmc-用户名称,jldytdm-计量表分类,jfdl-计费电量,dfny-电费年月,xlxdbs-线路线段标识,
        //      tbgzdbh-退补工作单编号,tqbs-台区标识，jldbh-计量点编号,gddwbm-供电单位编码,
        //      yhlbdm-用户类别代码,bqcbcs-本期抄表次数,yhztdm-用户状态代码,jlfsdm-计量方式代码,ygbsdl-有功变损电量,
        //      ygcjdl-有功抄见电量,jldydjdm-计量点电压等级代码,hyfldm-行业分类代码
        //
        //lixc20200506新加字段 djdm-电价代码,cbjhbh-抄表计划编号,jldcbsxh-计量点抄表顺序号,yddz-用电地址,
        //      cbqdbh-抄表区段编号,mfdl-免费电量,jfrl-计费容量,ywlbdm-业务类别代码,jldxh-计量点序号,ygtbdl-有功退补电量,
        //      ygfbkjdl-有功分表扣减电量,dqbm-地区编码,cbrq-抄表日期,dwbm-单位编码
        val table12 ="impala::csg_ods_yx.hs_jldxx_d"  //hs_jldxx_d
        // val table2 ="npmis_hs_jldxx_his"  没这表
        val kuduMap12: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table12)
        val jldxx1=sparkSession.read
          .options(kuduMap12)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jldxx1.createOrReplaceTempView("jldxx1")

        val jldxx_all =sparkSession.sql(
            s"""
               |select
               |    yhbh,yhmc,jldytdm,jfdl,dfny,xlxdbs,tbgzdbh,tqbs,jldbh,gddwbm,yhlbdm,bqcbcs,
               |    yhztdm,jlfsdm,ygbsdl,ygcjdl,jldydjdm,hyfldm,gzdbh,scdl,ygzdl,wgzdl,hsztdm,
               |    djdm,cbjhbh,jldcbsxh,yddz,cbqdbh,mfdl,jfrl,ywlbdm,jldxh,ygtbdl,ygfbkjdl,dqbm,cbrq,dwbm
               |from jldxx1
               |where dfny in (${nowMonth},${lastMonth}) and ywlbdm = '0000' and dqbm in (${cityCodeList})
               |
             """.stripMargin)
        jldxx_all.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("jldxx_all")
        sparkSession.catalog.dropTempView("jldxx1")





        /*----13 计量点信息---------------------------------------------------------------*/
        // TODO: 基于临时表操作
        //计量点信息 yhbh-用户编号,yhmc-用户名称,jldytdm-计量表分类,jfdl-计费电量,dfny-电费年月,xlxdbs-线路线段标识,
        //      tbgzdbh-退补工作单编号,tqbs-台区标识，jldbh-计量点编号,gddwbm-供电单位编码,yhlbdm-用户类别代码,
        //      bqcbcs-本期抄表次数,yhztdm-用户状态代码,jlfsdm-计量方式代码,ygbsdl-有功变损电量,
        //      ygcjdl-有功抄见电量,jldydjdm-计量点电压等级代码,hyfldm-行业分类代码
        //
        //lixc20200506新加字段 djdm-电价代码,cbjhbh-抄表计划编号,jldcbsxh-计量点抄表顺序号,yddz-用电地址,
        //      cbqdbh-抄表区段编号,mfdl-免费电量,jfrl-计费容量,ywlbdm-业务类别代码,jldxh-计量点序号,
        //      ygtbdl-有功退补电量,ygfbkjdl-有功分表扣减电量,dqbm-地区编码,cbrq-抄表日期,dwbm-单位编码
        val jldxx = sparkSession.sql(
            s"""
               |select
               |    yhbh,yhmc,jldytdm,jfdl,dfny,xlxdbs,tbgzdbh,tqbs,jldbh,gddwbm,yhlbdm,bqcbcs,
               |    yhztdm,jlfsdm,ygbsdl,ygcjdl,jldydjdm,hyfldm,gzdbh,scdl,ygzdl,wgzdl,hsztdm,
               |    djdm,cbjhbh,jldcbsxh,yddz,cbqdbh,mfdl,jfrl,ywlbdm,jldxh,ygtbdl,ygfbkjdl,dqbm,cbrq,dwbm
               |from jldxx_all
               |where dfny in (${nowMonth},${lastMonth}) and ywlbdm = '0000'
               |
             """.stripMargin)
        jldxx.cache().createOrReplaceTempView("jldxx")


        /*----14 退补电量信息---------------------------------------------------------------*/
        //退补电量信息 ygdl-有功电量,gzdbh-工作单编号,ccyf-差错月份
        val table14 ="impala::csg_ods_yx.hs_tbdlxx"
        val kuduMap14: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table14)
        val tbdlxx1=sparkSession.read
          .options(kuduMap14)
          .format("org.apache.kudu.spark.kudu")
          .load()

        tbdlxx1.createOrReplaceTempView("tbdlxx")
        val tbdlxx =sparkSession.sql(
            s"""
               |select
               |    ygdl,gzdbh,ccyf,jldbh
               |from tbdlxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        tbdlxx.createOrReplaceTempView("tbdlxx") //退补电量信息



        /*----15 退补明细---------------------------------------------------------------*/
        //退补明细
        val table15 ="impala::csg_ods_yx.hs_tbmx"
        val kuduMap15: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table15)
        val cztb1=sparkSession.read
          .options(kuduMap15)
          .format("org.apache.kudu.spark.kudu")
          .load()

        cztb1.createOrReplaceTempView("tbmx")
        val cztb =sparkSession.sql(
            s"""
               |select
               |    tbdl,gzdbh,jldbh,dfny,tblx
               |from tbmx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        cztb.createOrReplaceTempView("tbmx") //退补明细






        /*----16---------------------------------------------------------------*/
        //光伏用户档案信息 yhbh-用户编号
        val table16 ="impala::csg_ods_yx.sc_gf_gfyhdaxx"
        val kuduMap16: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table16)
        val gfyhdaxx1=sparkSession.read
          .options(kuduMap16)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gfyhdaxx1.createOrReplaceTempView("gfyhdaxx")
        val gfyhdaxx =sparkSession.sql(
            s"""
               |select yhbh
               |from gfyhdaxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gfyhdaxx.createOrReplaceTempView("gfyhdaxx") //光伏用户档案信息



        /*----17 光伏用户关联信息---------------------------------------------------------------*/
        //光伏用户关联信息 gfhyhbh-光伏户用户编号,gdhyhbh-购电户用户编号,ydhyhbh-用电户用户编号
        val table17 ="impala::csg_ods_yx.sc_gf_gfyhglxx"
        val kuduMap17: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table17)
        val gfyhglxx1=sparkSession.read
          .options(kuduMap17)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gfyhglxx1.createOrReplaceTempView("gfyhglxx")
        val gfyhglxx =sparkSession.sql(
            s"""
               |select
               |    gfhyhbh,gdhyhbh,ydhyhbh
               |from gfyhglxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        gfyhglxx.createOrReplaceTempView("gfyhglxx")




        /*----18---------------------------------------------------------------*/
        //线路编组信息  bdzbh-变电站编号,bdzmc-变电站名称,bdzbs-变电站标识,xlbh-线路编号,xlmc-线路名称,
        //      xlxdbs-线路线段标识,xlbzbh-线路编组编号,create_time-创建时间,gddwbm-供电单位编码
        val table18 ="impala::csg_ods_yx.gk_xlbzxx"
        val kuduMap18: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table18)
        val xlbzxx1=sparkSession.read
          .options(kuduMap18)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xlbzxx1.createOrReplaceTempView("xlbzxx")
        val xlbzxx =sparkSession.sql(
            s"""
               |select
               |    bdzbh,bdzmc,xlbh,xlmc,xlbzbh,bdzbs,xlxdbs,cjsj,gddwbm,bzny
               |from xlbzxx
             """.stripMargin)
        xlbzxx.createOrReplaceTempView("xlbzxx") //线路编组信息




        /*----19---------------------------------------------------------------*/
        //台区编组信息
        val table19 ="impala::csg_ods_yx.gk_tqbzxx"
        val kuduMap19: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table19)
        val tqbzxx1=sparkSession.read
          .options(kuduMap19)
          .format("org.apache.kudu.spark.kudu")
          .load()

        tqbzxx1.createOrReplaceTempView("tqbzxx")
        val tqbzxx = sparkSession.sql(
            """
              |select tqbzbh,tqbs,bzny
              |from tqbzxx
            """.stripMargin)
        tqbzxx.createOrReplaceTempView("tqbzxx")



        /*----20---------------------------------------------------------------*/
        //违窃信息   chsj-查获时间,yzbdl-应追补电量,yhbh-用户编号,gzdbh-工作单编号
        val table20 ="impala::csg_ods_yx.fw_wqxx"
        val kuduMap20: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table20)
        val wqxx1=sparkSession.read
          .options(kuduMap20)
          .format("org.apache.kudu.spark.kudu")
          .load()

        wqxx1.createOrReplaceTempView("wqxx")
        val wqxx =sparkSession.sql(
            s"""
               |select
               |    chsj,yzbdl,yhbh,gzdbh,dfny
               |from wqxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        wqxx.createOrReplaceTempView("wqxx")




        /*----21 工作单信息---------------------------------------------------------------*/
        //工作单信息  gzdbh-工作单编号
        val table21 ="impala::csg_ods_yx.xt_gzdxx"
        val kuduMap21: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table21)
        val gzdxx1=sparkSession.read
          .options(kuduMap21)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gzdxx1.createOrReplaceTempView("gzdxx")
        val gzdxx =sparkSession.sql(
            s"""
               |select
               |    gzdbh,ywlb,ywfl,sqsj,wcsj
               |from gzdxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gzdxx.createOrReplaceTempView("gzdxx")





        /*----22 线路和台区指标---------------------------------------------------------------*/
        //线路和台区指标
        val table22 ="impala::csg_ods_yx.gk_xlhtqzb"
        val kuduMap22: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table22)
        val xlhtqzb1=sparkSession.read
          .options(kuduMap22)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xlhtqzb1.createOrReplaceTempView("xlhtqzb1")
        val xlhtqzb =sparkSession.sql(
            s"""
               |select
               |    bdzbh,bdzmc,xlbh,xlmc,tqbh,tqmc,ny,bdzbs,xlxdbs,tqbs,gddwbm,
               |    xltqbz,xszrrbs,khzb,khzbxx,rxskhzb,rxskhzbxx
               |from xlhtqzb1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        xlhtqzb.createOrReplaceTempView("xlhtqzb") //线路和台区指标
        sparkSession.catalog.dropTempView("xlhtqzb1")



        /*----23---------------------------------------------------------------*/
        //线损责任人  rybh-人员编号,rymc-人员名称,rybs-人员标识,xszrrbs-线损责任人标识
        val table23 ="impala::csg_ods_yx.gk_xszrr"
        val kuduMap23: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table23)
        val xszrr1=sparkSession.read
          .options(kuduMap23)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xszrr1.createOrReplaceTempView("xszrr")
        val xszrr =sparkSession.sql(
            s"""
               |select
               |    rybh,rymc,rybs,xszrrbs
               |from xszrr
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        xszrr.createOrReplaceTempView("xszrr")



        /*----24---------------------------------------------------------------*/
        //应收电费记录  yhbh-用户编号,yhmc-用户名称,ysdf-应收电费,jldbh-计量点编号,gddwbm-供电单位编码,yhlbdm-用户类别代码
        //lixc20200506新增字段 ywlbdm-业务类别代码,djdm-电价代码
        val table24 ="impala::csg_ods_yx.zw_ysdfjl_d"  //zw-账务域
        val kuduMap24: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table24)
        val ysdfjl1=sparkSession.read
          .options(kuduMap24)
          .format("org.apache.kudu.spark.kudu")
          .load()

        ysdfjl1.createOrReplaceTempView("ysdfjl")
        val ysdfjl =sparkSession.sql(
            s"""
               |select
               |    yhbh,yhmc,ysdf,jldbh,gddwbm,yhlbdm,dfny,bqcbcs,
               |    xlxdbs,yddz,tqbs,cbqdbh,jfdl,czsj,ywlbdm,djdm
               |from ysdfjl
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        ysdfjl.createOrReplaceTempView("ysdfjl") //应收电费记录




        /*----25抄表区段信息---------------------------------------------------------------*/
        //抄表区段信息  cbqdbh-抄表区段编号,cbqdmc-抄表区段名称
        val table25 ="impala::csg_ods_yx.lc_cbqdxx"
        val kuduMap25: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table25)
        val cbqdxx1=sparkSession.read
          .options(kuduMap25)
          .format("org.apache.kudu.spark.kudu")
          .load()

        cbqdxx1.createOrReplaceTempView("cbqdxx")
        val cbqdxx =sparkSession.sql(
            s"""
               |select cbqdbh,cbqdmc
               |from cbqdxx
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        cbqdxx.createOrReplaceTempView("cbqdxx")





        /*----26 运行计量自动化终端---------------------------------------------------------------*/
        //运行计量自动化终端   yxjlzdhzdbs-运行计量自动化标识,yhbh-用户编号,sblbdm-设备类别代码
        val table26 ="impala::csg_ods_yx.sb_yxjlzdhzd"
        val kuduMap26: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table26)
        val yxjlzdhzd1=sparkSession.read
          .options(kuduMap26)
          .format("org.apache.kudu.spark.kudu")
          .load()

        yxjlzdhzd1.createOrReplaceTempView("yxjlzdhzd")
        val yxjlzdhzd =sparkSession.sql(
            s"""
               |select
               |    yxjlzdhzdbs,yhbh,sblbdm
               |from yxjlzdhzd
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        yxjlzdhzd.createOrReplaceTempView("yxjlzdhzd")





        /*----27---------------------------------------------------------------*/
        //计量自动化终端采集关系记录   yxjlzdhzdbs-运行计量自动化标识,cjdxyhbh-采集对象用户编号
        val table27 ="impala::csg_ods_yx.sb_jlzdhzdcjgxjl"
        val kuduMap27: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table27)
        val jlzdhzdcjgxjl1=sparkSession.read
          .options(kuduMap27)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jlzdhzdcjgxjl1.createOrReplaceTempView("jlzdhzdcjgxjl")
        val jlzdhzdcjgxjl =sparkSession.sql(
            s"""
               |select
               |    yxjlzdhzdbs,cjdxyhbh
               |from jlzdhzdcjgxjl
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        jlzdhzdcjgxjl.createOrReplaceTempView("jlzdhzdcjgxjl")//计量自动化终端采集关系记录





        /*----28---------------------------------------------------------------*/
        //变电站线路关系
        val table28 ="impala::csg_ods_yx.dw_bdzxlgx"
        val kuduMap28: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table28)
        val bdzxlgx1=sparkSession.read
          .options(kuduMap28)
          .format("org.apache.kudu.spark.kudu")
          .load()

        bdzxlgx1.createOrReplaceTempView("bdzxlgx")
        val bdzxlgx=sparkSession.sql(
            s"""
               |select
               |    xlxdbs,bdzbs
               |from bdzxlgx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        bdzxlgx.createOrReplaceTempView("bdzxlgx")



        /*----29---------------------------------------------------------------*/
        //变电站
        val table29 ="impala::csg_ods_yx.dw_bdz"
        val kuduMap29: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table29)
        val bdz1=sparkSession.read
          .options(kuduMap29)
          .format("org.apache.kudu.spark.kudu")
          .load()

        bdz1.createOrReplaceTempView("bdz")
        val bdz =sparkSession.sql(
            s"""
               |select
               |    bdzbs,--变电站标识
               |    bdzbh,--变电站标号
               |    bdzmc  --变电站名称
               |from bdz
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        bdz.createOrReplaceTempView("bdz")




        /*----30---------------------------------------------------------------*/
        //业务级联
        val table30 ="impala::csg_ods_yx.xt_ywjl"
        val kuduMap30: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table30)
        val ywjl1=sparkSession.read
          .options(kuduMap30)
          .format("org.apache.kudu.spark.kudu")
          .load()

        ywjl1.createOrReplaceTempView("ywjl")
        val ywjl =sparkSession.sql(
            s"""
               |select
               |    ywbmmc,ywbm,sjywbm
               |from ywjl
             """.stripMargin)
        ywjl.createOrReplaceTempView("ywjl")






        /*----31---------------------------------------------------------------*/
        //工作单信息历史
        val table31 ="impala::csg_ods_yx.xt_gzdxxls"
        val kuduMap31: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table31)
        val gzdxxls1=sparkSession.read
          .options(kuduMap31)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gzdxxls1.createOrReplaceTempView("gzdxxls")
        val gzdxxls =sparkSession.sql(
            s"""
               |select
               |    gzdbh,ywlb,ywfl,sqsj,wcsj,wcsj,gzdzt,slbs,gddwbm
               |from gzdxxls
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        gzdxxls.createOrReplaceTempView("gzdxxls")






        /*----32---------------------------------------------------------------*/
        //组织  zzbm-组织编号,zzmc-组织名称
        val table32 ="impala::csg_ods_yx.xt_zz"
        val kuduMap32: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table32)
        val zz1=sparkSession.read
          .options(kuduMap32)
          .format("org.apache.kudu.spark.kudu")
          .load()

        zz1.createOrReplaceTempView("zz")
        val zz =sparkSession.sql(
            s"""
               |select
               |    zzbm,zzmc,zzlxdm,dqbm
               |from zz
             """.stripMargin)
        zz.createOrReplaceTempView("zz") //各组织关系





        /*----33 代码编码---------------------------------------------------------------*/
        //代码编码
        val table33 ="impala::csg_ods_yx.xt_dmbm"
        val kuduMap33: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table33)
        val dmbm1=sparkSession.read
          .options(kuduMap33)
          .format("org.apache.kudu.spark.kudu")
          .load()

        dmbm1.createOrReplaceTempView("dmbm")
        val dmbm=sparkSession.sql(
            s"""
               |select
               |    dmbm,dmbmmc,dmfl
               |from dmbm
               |
             """.stripMargin)
        dmbm.createOrReplaceTempView("dmbm")




        /*----34---------------------------------------------------------------*/
        //线路管理单位
        val table ="impala::csg_ods_yx.dw_xlgldw"
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        val xlgldw1=sparkSession.read
          .options(kuduMap)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xlgldw1.createOrReplaceTempView("xlgldw")
        val xlgldw =sparkSession.sql(
            s"""
               |select *
               |from xlgldw
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        xlgldw.createOrReplaceTempView("xlgldw")




        /*----35---------------------------------------------------------------*/
        //业扩工作单基本信息
        val table35 ="impala::csg_ods_yx.fw_ykgzdjbxx"
        val kuduMap35: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table35)
        val ykgzdjbxx1=sparkSession.read
          .options(kuduMap35)
          .format("org.apache.kudu.spark.kudu")
          .load()

        ykgzdjbxx1.createOrReplaceTempView("ykgzdjbxx")
        val ykgzdjbxx =sparkSession.sql(
            s"""
               |select
               |    gzdbh,yhbh,slbs,ywlbbh,zsslrq,gzdztdm,gdsj
               |from ykgzdjbxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        ykgzdjbxx.createOrReplaceTempView("ykgzdjbxx")



        /*----36---------------------------------------------------------------*/
        val table36 ="impala::csg_ods_yx.wfrt_task_exec_info" // TODO: 这张表找不到
        val kuduMap36: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table36)
        val wfrt_task_exec_info1=sparkSession.read
          .options(kuduMap36)
          .format("org.apache.kudu.spark.kudu")
          .load()

        wfrt_task_exec_info1.createOrReplaceTempView("wfrt_task_exec_info")
        val wfrt_task_exec_info =sparkSession.sql(
            s"""
               |select
               |    instance_id,tache_name,task_commit_time
               |from wfrt_task_exec_info
               |
             """.stripMargin)
        wfrt_task_exec_info.createOrReplaceTempView("wfrt_task_exec_info")




        /*----37---------------------------------------------------------------*/
        //线损报表分区线损统计信息
        val table37 ="impala::csg_ods_yx.gk_xsbbfqxstjxx"  //换小写把
        val kuduMap37: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table37)
        val xsbbfqxstjxx1=sparkSession.read
          .options(kuduMap37)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xsbbfqxstjxx1.createOrReplaceTempView("fqxstjxx")
        val xsbbfqxstjxx =sparkSession.sql(
            s"""
               |select
               |    gddwbm,byxsl,bygdl,bysdl,bbny ny,khzb,khzbxx
               |from fqxstjxx
             """.stripMargin)
        xsbbfqxstjxx.createOrReplaceTempView("fqxstjxx") //线损报表分区线损统计信息




        /*----38---------------------------------------------------------------*/
        //2020-01-13
        val table38 ="nzwot_sp_so_runmode_device"  //todo 这个表没有
        val kuduMap38: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table38)
        val nzwot_sp_so_runmode_device1=sparkSession.read.options(kuduMap38).format("org.apache.kudu.spark.kudu").load()
        nzwot_sp_so_runmode_device1.createOrReplaceTempView("nzwot_sp_so_runmode_device")
        val nzwot_sp_so_runmode_device =sparkSession.sql(
            s"""
               |select
               |    execute_begin_time,execute_end_time,rolloff_line_id,rollon_line_id,mode_id
               |from nzwot_sp_so_runmode_device
               |
             """.stripMargin)
        nzwot_sp_so_runmode_device.createOrReplaceTempView("nzwot_sp_so_runmode_device")




        /*----39---------------------------------------------------------------*/
        val table39 ="nzwot_sp_so_runmode_manage"  //todo 这个表没有
        val kuduMap39: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table39)
        val nzwot_sp_so_runmode_manage1=sparkSession.read
          .options(kuduMap39)
          .format("org.apache.kudu.spark.kudu")
          .load()

        nzwot_sp_so_runmode_manage1.createOrReplaceTempView("nzwot_sp_so_runmode_manage")
        val nzwot_sp_so_runmode_manage =sparkSession.sql(
            s"""
               |select
               |    code,id,mode_type,mode_state,apply_time,turn_power_reason,turn_power_reason_type
               |from nzwot_sp_so_runmode_manage
             """.stripMargin)
        nzwot_sp_so_runmode_manage.createOrReplaceTempView("nzwot_sp_so_runmode_manage")


        /*----40---------------------------------------------------------------*/
        //环网操作信息维护
        val table40 ="impala::csg_ods_yx.gk_hwczxxwh"
        val kuduMap40: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table40)
        val hwczxxwh1=sparkSession.read
          .options(kuduMap40)
          .format("org.apache.kudu.spark.kudu")
          .load()

        hwczxxwh1.createOrReplaceTempView("hwczxxwh")
        val hwczxxwh =sparkSession.sql(
            s"""
               |select xlxdbs,xsny
               |from hwczxxwh
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        hwczxxwh.createOrReplaceTempView("hwczxxwh")






        /*----41---------------------------------------------------------------*/
        //11月18号新增
        //换表信息记录
        val table41 ="impala::csg_ods_yx.lc_hbxxjl"
        val kuduMap41: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table41)
        val hbxxjl1=sparkSession.read
          .options(kuduMap41)
          .format("org.apache.kudu.spark.kudu")
          .load()

        hbxxjl1.createOrReplaceTempView("hbxxjl")
        val hbxxjl =sparkSession.sql(
            s"""
               |select
               |    yxdnbbs,dfny
               |from hbxxjl where dqbm in (${cityCodeList})
             """.stripMargin)
        hbxxjl.createOrReplaceTempView("hbxxjl")




        /*----42------- 二次处理的表 --------------------------------------------------------*/
        //变电站考核表
        val bdzkhb =
        s"""
           |select
           |    distinct l.xlxdbs,y.yhbh
           |from all_xlxd l
           |inner join jld j on j.xlxdbs = l.xlxdbs
           |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '80'
           |where l.xlyxzt = '01' and l.dydjdm not in ('10','12','13','15') and l.xllbdm = '1'
           |
               """.stripMargin
        sparkSession.sql(bdzkhb).createOrReplaceTempView ("bdzkhb")

        //台区考核表
        val tqkhb =
            s"""
               |select distinct t.tqbs,y.yhbh
               |from all_tq t
               |inner join jld j on j.tqbs = t.tqbs
               |inner join ydkh y on y.yhbh = j.yhbh and y.yhlbdm = '60'
               |where t.yxztdm = '01'
               |
             """.stripMargin

        sparkSession.sql(tqkhb).createOrReplaceTempView("tqkhb")

        //变电站关联表
        val bdzglb = sparkSession.sql(
            s"""
               |select
               |    bx.xlxdbs,bd.bdzbs,bd.bdzbh,bd.bdzmc
               |from bdzxlgx bx
               |join bdz bd on bd.bdzbs = bx.bdzbs
               |
             """.stripMargin)
        bdzglb.createOrReplaceTempView("bdzglb")




        /*----43---------------------------------------------------------------*/
        //购电量价费汇总信息表 2019年11月19日11:23:10 lixc
        val table43 ="impala::csg_ods_yx.sc_gdljfhzxx"
        val kuduMap43: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table43)
        val gdljfhzxx1=sparkSession.read
          .options(kuduMap43)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gdljfhzxx1.createOrReplaceTempView("gdljfhzxx")
        val gdljfhzxx =sparkSession.sql(
            s"""
               |select dcbh,gdyf from gdljfhzxx where dqbm in (${cityCodeList})
             """.stripMargin)
        gdljfhzxx.createOrReplaceTempView("gdljfhzxx")




        /*----44---------------------------------------------------------------*/
        //电厂辅助信息表 2019年11月19日11:23:14 lixc
        val table44 ="impala::csg_ods_yx.sc_dcfzxx"
        val kuduMap44: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table44)
        val dcfzxx1=sparkSession.read
          .options(kuduMap44)
          .format("org.apache.kudu.spark.kudu")
          .load()

        dcfzxx1.createOrReplaceTempView("dcfzxx")
        val dcfzxx =sparkSession.sql(
            s"""
               |select dcbh,dydj,gdldz
               |from dcfzxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        dcfzxx.createOrReplaceTempView("dcfzxx")  //电厂辅助信息表






        /*----45---------------------------------------------------------------*/
        //结算单元表 2019年11月19日11:23:14 lixc
        val table45 ="impala::csg_ods_yx.sc_jsdy"
        val kuduMap45: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table45)
        val jsdy1=sparkSession.read
          .options(kuduMap45)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jsdy1.createOrReplaceTempView("jsdy")
        val jsdy =sparkSession.sql(
            s"""
               |select dcbh,fgbs,gsgx from jsdy
             """.stripMargin)
        jsdy.createOrReplaceTempView("jsdy")





        /*----46-------  二次操作 --------------------------------------------------------*/
        //线路地方电厂上网电量 2019年11月19日11:14:42 lixc
        val xldfdcswdl =
        s"""
           |select a.xlxdbs,sum(coalesce(a.swdl,0)) swdl
           |from (
           |	select dy.xlxdbs,c.jldbh,c.yhbh,sum(coalesce(c.bjdl,0)+coalesce(c.jbdl,0)) swdl,'BFFG' fgbz
           |	from cbxx c
           |	join yhdyxx dy on dy.yhbh = c.yhbh
           |	join ydkh y on y.yhbh = c.yhbh and y.yhztdm <> '2'
           |	where c.dfny = ${nowMonth} and c.sslxdm = '221' and c.yhlbdm = '40'
           |	and exists (select 1 from gdljfhzxx a,dcfzxx b
           |		where c.yhbh = a.dcbh and c.dfny = a.gdyf
           |		and b.dcbh = a.dcbh and b.dydj = '08' and b.gdldz <> '2')
           |	group by dy.xlxdbs,c.jldbh,c.yhbh
           |
           |  union all
           |	select dy.xlxdbs,c.jldbh,c.yhbh,sum(coalesce(c.bjdl,0)+coalesce(c.jbdl,0)) swdl,'FFG' fgbz
           |	from cbxx c
           |	join yhdyxx dy on dy.yhbh = c.yhbh
           |	join ydkh y on y.yhbh = c.yhbh and y.yhztdm <> '2'
           |	where c.dfny = ${nowMonth} and c.sslxdm in ('222','223','224') and c.yhlbdm = '40'
           |	and exists (select 1 from gdljfhzxx a,dcfzxx b
           |		where c.yhbh = a.dcbh and c.dfny = a.gdyf
           |		and b.dcbh = a.dcbh and b.dydj = '08' and b.gdldz <> '2')
           |	group by dy.xlxdbs,c.jldbh,c.yhbh
           |) a
           |where a.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = a.yhbh) = '0' then 'BFFG' else 'FFG' end)
           |group by a.xlxdbs
           |
                 |
             """.stripMargin
        sparkSession.sql(xldfdcswdl).createOrReplaceTempView("xldfdcswdl") //线路地方电厂上网电量

        //台区地方电厂上网电量 2019年11月19日11:14:42 lixc
        val tqdfdcswdl =
            s"""
               |select
               |    dy.tqbs,sum(coalesce(c.bjdl,0)+coalesce(c.jbdl,0)) swdl
               |from cbxx c
               |join yhdyxx dy on dy.yhbh = c.yhbh
               |join ydkh y on y.yhbh = c.yhbh and y.yhztdm <> '2'
               |where c.dfny = ${nowMonth} and c.sslxdm = '221' and c.yhlbdm = '40'
               |    and exists (select 1
               |                from gdljfhzxx a,dcfzxx b
               |		        where c.yhbh = a.dcbh and c.dfny = a.gdyf
               |		            and b.dcbh = a.dcbh and b.dydj in ('02','03')
               |              )
               |group by dy.tqbs
               |
             """.stripMargin
        sparkSession.sql(tqdfdcswdl).createOrReplaceTempView("tqdfdcswdl")





        /*----47---------------------------------------------------------------*/
        //变损标准表 2019年11月20日17:04:22 lixc
        val table47 ="impala::csg_ods_yx.hs_bsbz"
        val kuduMap47: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table47)
        val bsbz1=sparkSession.read
          .options(kuduMap47)
          .format("org.apache.kudu.spark.kudu")
          .load()

        bsbz1.createOrReplaceTempView("bsbz")
        val bsbz =sparkSession.sql(
            s"""
               |select
               |    byqrl,shlxdm,ygbs
               |from bsbz
             """.stripMargin)
        bsbz.createOrReplaceTempView("bsbz")




        /*----48-- 二次操作  -------------------------------------------------------------*/
        //变压器月固定损耗 2019年11月20日17:04:28 lixc
        val byqygdsh =
        s"""
           |select
           |    t.sbbs,min(b.ygbs) zxygbs
           |from yxbyq t
           |join bsbz b on b.byqrl = t.edrl and b.shlxdm = t.shlxdm
           |where t.gbzbbz = 1 and t.yxztdm = '01'
           |group by t.sbbs
           |
             """.stripMargin
        sparkSession.sql(byqygdsh).createOrReplaceTempView("byqygdsh")





        /*----49---------------------------------------------------------------*/
        //电子化移交工作单信息表 2019年11月21日19:40:58 lixc
        val table49 ="impala::csg_ods_yx.yp_dzhyjgzdxx"  //在服务域 fw.yp.
        val kuduMap49: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table49)
        val dzhyjgzdxx1=sparkSession.read
          .options(kuduMap49)
          .format("org.apache.kudu.spark.kudu")
          .load()

        dzhyjgzdxx1.createOrReplaceTempView("dzhyjgzdxx")
        val dzhyjgzdxx =sparkSession.sql(
            s"""
               |select
               |    dzhyjgdh,yhbh,ykgdbh
               |from dzhyjgzdxx
             """.stripMargin)
        dzhyjgzdxx.createOrReplaceTempView("dzhyjgzdxx") //电子化移交工作单信息表



        /*----50---------------------------------------------------------------*/
        //20200108  人员表

        val table50 ="impala::csg_ods_yx.xt_ry"
        val kuduMap50: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table50)
        val ry1=sparkSession.read
          .options(kuduMap50)
          .format("org.apache.kudu.spark.kudu")
          .load()

        ry1.createOrReplaceTempView("ry")
        val ry =sparkSession.sql(
            s"""
               |select rybs,rymc
               |from ry
               |
             """.stripMargin)
        ry.createOrReplaceTempView("ry") //人员表



        /*----51---------------------------------------------------------------*/
        // gis系统  trans_id-跟变压器的gisid对应, circuit_id-跟线路线段的gisid对应
        val table51 ="gis_transcircuitsupply"  //todo 这个表没有
        val kuduMap51: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table51)
        val transcircuitsupply1=sparkSession.read
          .options(kuduMap51)
          .format("org.apache.kudu.spark.kudu")
          .load()

        transcircuitsupply1.createOrReplaceTempView("transcircuitsupply")
        val transcircuitsupply =sparkSession.sql(
            s"""
               |select
               |    trans_id,circuit_id,main_supply
               |from transcircuitsupply
             """.stripMargin)
        transcircuitsupply.createOrReplaceTempView("transcircuitsupply")





        /*----52---------------------------------------------------------------*/
        val table52 ="gis_lvcustomersupply"  //todo 这个表没有
        val kuduMap52: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table52)
        val gis_lvcustomersupply1=sparkSession.read.options(kuduMap52).format("org.apache.kudu.spark.kudu").load()
        gis_lvcustomersupply1.createOrReplaceTempView("gis_lvcustomersupply")
        val gis_lvcustomersupply =sparkSession.sql(
            s"""
               |select
               |    trans_id,customer_mrid
               |from gis_lvcustomersupply
             """.stripMargin)
        gis_lvcustomersupply.createOrReplaceTempView("gis_lvcustomersupply")


        /*----53---------------------------------------------------------------*/
        val table53 ="gis_g_dm_function_location" //todo 这个表没有
        val kuduMap53: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table53)
        val gis_g_dm_function_location1=sparkSession.read
          .options(kuduMap53)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gis_g_dm_function_location1.createOrReplaceTempView("gis_g_dm_function_location")
        val gis_g_dm_function_location =sparkSession.sql(
            s"""
               |select
               |    id,classify_id,xfmr_type,fl_name
               |from gis_g_dm_function_location1
             """.stripMargin)
        gis_g_dm_function_location.createOrReplaceTempView("gis_g_dm_function_location")


        /*----54------ ---------------------------------------------------------*/
        //2020.02.21  获取业务类别代码
        val ywlbList= sparkSession.sql("select ywbm,ywbmmc from ywjl").collect()
        val ywlbMap = scala.collection.mutable.Map[String, String]()
        for (ywlb <- ywlbList) {
            //todo 把业务编码，业务编码名称 存到 Map里
            ywlbMap.put(ywlb.getAs[String]("ywbm"), ywlb.getAs[String]("ywbmmc"))
        }
        val ywlbBroadcast= sparkSession.sparkContext.broadcast(ywlbMap)

        //todo 由 ywbm ->ywbmmc
        sparkSession.udf.register("getywlbmc", (ywlb: String) => {
            ywlbBroadcast.value.get(ywlb).getOrElse(null)

        })



        /*----55---------------------------------------------------------------*/
        //异常供电单位线路对应关系
        val table55 ="ycgddwxlgx"   //todo 这个表没有
        val kuduMap55: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table55)
        val ycgddwxlgx1=sparkSession.read
          .options(kuduMap55)
          .format("org.apache.kudu.spark.kudu")
          .load()

        ycgddwxlgx1.createOrReplaceTempView("ycgddwxlgx")
        val ycgddwxlgx =sparkSession.sql(
            s"""
               |select * from ycgddwxlgx
             """.stripMargin)
        ycgddwxlgx.createOrReplaceTempView("ycgddwxlgx")




        /*----56---------------------------------------------------------------*/
        //异常规则表（这个好像是输出表）
        val table56 ="gk_xsycgz"  //todo 这个表没有
        val kuduMap56: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table56)
        val xsycgz1=sparkSession.read
          .options(kuduMap56)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xsycgz1.createOrReplaceTempView("xsycgz")
        val xsycgz =sparkSession.sql(
            s"""
               |select *
               |from xsycgz
             """.stripMargin)
        xsycgz.createOrReplaceTempView("xsycgz")//异常规则表
        xsycgz


        /*----57---------------------------------------------------------------*/
        //lixc20200506新加 购电交易差错退补信息
        val table57 ="impala::csg_ods_yx.sc_gdjycctbxx"
        val kuduMap57: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table57)
        val gdjycctbxx1=sparkSession.read
          .options(kuduMap57)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gdjycctbxx1.createOrReplaceTempView("gdjycctbxx")
        val gdjycctbxx =sparkSession.sql(
            s"""
               |select
               |    gddwbm,ccyf,dcbh,dcmc,tbdl,jsdybh,tbzt,dqbm
               |from gdjycctbxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        gdjycctbxx.createOrReplaceTempView("gdjycctbxx") //购电交易差错退补信息





        /*----58---------------------------------------------------------------*/
        //lixc20200506新加 电厂机组信息
        val table58 ="sc_dcjzxx"
        val kuduMap58: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table58)
        val dcjzxx1=sparkSession.read
          .options(kuduMap58)
          .format("org.apache.kudu.spark.kudu")
          .load()
        dcjzxx1.createOrReplaceTempView("dcjzxx")
        val dcjzxx =sparkSession.sql(
            s"""
               |select
               |    jsdybh,jldbh,dqbm
               |from dcjzxx where dqbm in (${cityCodeList})
             """.stripMargin)
        dcjzxx.createOrReplaceTempView("dcjzxx") //电厂机组信息





        /*----59---------------------------------------------------------------*/
        //lixc20200506新加 抄表区段辅助
        val table59 ="impala::csg_ods_yx.lc_cbqdfz"
        val kuduMap59: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table59)
        val cbqdfz1=sparkSession.read
          .options(kuduMap59)
          .format("org.apache.kudu.spark.kudu")
          .load()

        cbqdfz1.createOrReplaceTempView("cbqdfz")
        val cbqdfz =sparkSession.sql(
            s"""
               |select
               |    cbqdbh,cblr,dqbm
               |from cbqdfz where dqbm in (${cityCodeList})
             """.stripMargin)
        cbqdfz.createOrReplaceTempView("cbqdfz") //抄表区段辅助

        /*----60---------------------------------------------------------------*/

        //lixc20200506新加 线损专变公用用户
        val table60 ="impala::csg_ods_yx.gk_xszbgyyh"
        val kuduMap60: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table60)
        val xszbgyyh1=sparkSession.read
          .options(kuduMap60)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xszbgyyh1.createOrReplaceTempView("xszbgyyh")
        val xszbgyyh =sparkSession.sql(
            s"""
               |select
               |    xlxdbs,yhbh
               |from xszbgyyh
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        xszbgyyh.createOrReplaceTempView("xszbgyyh") //线损专变公用用户


        /*----61---------------------------------------------------------------*/
        //lixc20200506新加 核算计量点关系
        val table61 ="impala::csg_ods_yx.hs_jldgx_d"  //hs_jldgx_d
        val kuduMap61: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table61)
        val hs_jldgx1=sparkSession.read
          .options(kuduMap61)
          .format("org.apache.kudu.spark.kudu")
          .load()

        hs_jldgx1.createOrReplaceTempView("hs_jldgx")
        val hs_jldgx =sparkSession.sql(
            s"""
               |select
               |    yhbh,glyhbh,dfny
               |from hs_jldgx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        hs_jldgx.createOrReplaceTempView("hs_jldgx") //核算计量点关系


        /*----62---------------------------------------------------------------*/
        //lixc20200506新加 非周期计费申请
        val table62 ="impala::csg_ods_yx.hs_fzqjfsq"
        val kuduMap62: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table62)
        val fzqjfsq1=sparkSession.read
          .options(kuduMap62)
          .format("org.apache.kudu.spark.kudu")
          .load()

        fzqjfsq1.createOrReplaceTempView("fzqjfsq")
        val fzqjfsq =sparkSession.sql(
            s"""
               |select
               |    gzdbh,yhbh,ywlbdm,ywzlbh,cldfny,clgzdbh
               |from fzqjfsq
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        fzqjfsq.createOrReplaceTempView("fzqjfsq") //非周期计费申请


        /*----63---------------------------------------------------------------*/
        //lixc20200604新加 计量点关系
        val table63 ="impala::csg_ods_yx.kh_jldgx"
        val kuduMap63: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table63)
        val jldgx1=sparkSession.read
          .options(kuduMap63)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jldgx1.createOrReplaceTempView("jldgx")
        val jldgx =sparkSession.sql(
            s"""
               |select
               |    jldbh,gxlxdm,gljldbh,yhbh,glyhbh,dqbm
               |from jldgx where dqbm in (${cityCodeList})
             """.stripMargin)
        jldgx.createOrReplaceTempView("jldgx") //计量点关系



        /*----64---------------------------------------------------------------*/
        //lixc20200520新加 计量自动化故障任务信息
        val table64 ="impala::csg_ods_yx.sb_jlzdhgzrwxx"
        val kuduMap64: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table64)
        val jlzdhgzrwxx1=sparkSession.read
          .options(kuduMap64)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jlzdhgzrwxx1.createOrReplaceTempView("jlzdhgzrwxx")
        val jlzdhgzrwxx =sparkSession.sql(
            s"""
               |select
               |    rwbs,gzdbh,yhbh,gzqkjs,gzfxf,xxlydm,gzfxrq,pgrq
               |from jlzdhgzrwxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        jlzdhgzrwxx.createOrReplaceTempView("jlzdhgzrwxx")//计量自动化故障任务信息


        /*----65---------------------------------------------------------------*/
        //lixc20200520新加 计量自动化故障任务明细
        val table65 ="impala::csg_ods_yx.sb_jlzdhgzrwmx"
        val kuduMap65: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table65)
        val jlzdhgzrwmx1=sparkSession.read
          .options(kuduMap65)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jlzdhgzrwmx1.createOrReplaceTempView("jlzdhgzrwmx")
        val jlzdhgzrwmx =sparkSession.sql(
            s"""
               |select
               |    rwmxbs,rwbs,gzdbh,yhbh,jldbh,zcbh,ccbh,sblbdm,sblx,zdgzlx,zdgzxx,zdgzxldm
               |from jlzdhgzrwmx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        jlzdhgzrwmx.createOrReplaceTempView("jlzdhgzrwmx") //计量自动化故障任务明细




        /*----66---------------------------------------------------------------*/
        //lixc20200602三相负荷不平衡率tq_41 start
        //户表终端关系
        val table66 ="tmr.hbzdgx" //todo 这个表没有
        val kuduMap66: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table66)
        val hbzdgx1=sparkSession.read
          .options(kuduMap66)
          .format("org.apache.kudu.spark.kudu")
          .load()

        hbzdgx1.createOrReplaceTempView("hbzdgx")
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
               |from hbzdgx a
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
        hbzdgx.createOrReplaceTempView("hbzdgx") //户表终端关系


        /*----67---------------------------------------------------------------*/
        //测量点电流电压
        val table67 =tableNameC                           //这个怎么用
        val kuduMap67: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table67)
        val clddldy1=sparkSession.read
          .options(kuduMap67)
          .format("org.apache.kudu.spark.kudu")
          .load()

        clddldy1.createOrReplaceTempView("clddldy")
        val clddldy =sparkSession.sql(
            s"""
               |select *
               |from (
               |	select d.*,
               |		row_number() over(partition by d.cldbs,d.sjsj order by d.etl_file_timtstamp desc) as num
               |	from clddldy d
               |	where d.sjsj like '${_nowMonth}%' and d.etl_city in (${cityNameList})
               |	order by d.sjsj
               |	) t
               |where t.num = 1
               |
             """.stripMargin)
        clddldy.createOrReplaceTempView("clddldy") //测量点电流电压



        /*----68--  二次操作 -------------------------------------------------------------*/
        //三相负荷不平衡率

        val sxfhbphl = sparkSession.sql(
            s"""
               |select
               |    a.yhbh,sum(case when a.bphl > 30 then 1 else 0 end) bphsl,count(a.bphl) zsl,
               |	round(sum(case when a.bphl > 30 then 1 else 0 end)/count(a.bphl)*100,2) sxfhbphl
               |from (
               |	select
               |        h.yhbh,h.jldbh,h.bjzcbh,d.cldbs,d.sjsj,d.adl,d.bdl,d.cdl,zddl(d.adl,d.bdl,d.cdl) zddl,
               |        zxdl(d.adl,d.bdl,d.cdl) zxdl,
               |        round((zddl(d.adl,d.bdl,d.cdl)-zxdl(d.adl,d.bdl,d.cdl))/zddl(d.adl,d.bdl,d.cdl)*100,2) bphl
               |    from ydkh y
               |	join hbzdgx h on h.yhbh = y.yhbh
               |	join clddldy d on d.cldbs = h.cldbs
               |	where y.yhlbdm = '60'
               |	) a
               |group by a.yhbh
               |
             """.stripMargin)

        sxfhbphl.createOrReplaceTempView("sxfhbphl")
        //三相负荷不平衡率tq_41 end


        /*-------------------------------------------------------------------*/
        /*-------------------------------------------------------------------*/


    }
}
