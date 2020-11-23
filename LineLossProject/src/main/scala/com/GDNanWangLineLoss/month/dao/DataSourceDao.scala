package com.GDNanWangLineLoss.month.dao

import com.GDNanWangLineLoss.month.bean.Variables._
import com.GDNanWangLineLoss.month.util.Functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.Map

/*
imlala看到的表名是   db.tablename，
对应的kudu表名是： impala::db.tablename.  // 2020/10/16 kudu全部的表都没有impala前缀
 */

object DataSourceDao {
    var start=0L
    var end=0L

/*----生成计量表码合并视图-- 计量的表还没有入库  -------------------------------------------------------------*/
    /**
    * @Description: 生成计量表码合并视图-- 计量的表还没有入库
    * @Param: [sparkSession, url]
    * @return: void
    * @Date: 2020/10/16
    */
    def mergeTables(sparkSession:SparkSession, url:String)={

        start= System.currentTimeMillis()
        /*--------------------------------------------------------------------------*/
        //1 户表终端关系
        val rxs_hbzdgx1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "tmr.hbzdgx"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        rxs_hbzdgx1.createOrReplaceTempView("rxs_hbzdgx")
        val rxs_hbzdgx =sparkSession.sql(
            s"""
               |select a.cldbs,a.bjlx,a.yhbh,a.jldbh,a.bjzcbh, a.zhbl, a.cldlxdm, a.jldbs, min(a.etl_time) etl_time
               |from rxs_hbzdgx a where a.etl_city in (${cityNameList})
               |group by a.cldbs, a.bjlx, a.yhbh, a.jldbh, a.bjzcbh, a.zhbl, a.cldlxdm, a.jldbs
               |
             """.stripMargin)
        rxs_hbzdgx.repartition(resultPartition).createOrReplaceTempView("rxs_hbzdgx ") //户表终端关系
        //rxs_hbzdgx.cache()
        //rxs_hbzdgx.count()
        /*--------------------------------------------------------------------------*/

        //2
        val rxs_dycldrdjbm_first1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> addOneTableName))
          .format("org.apache.kudu.spark.kudu")
          .load()

        rxs_dycldrdjbm_first1.createOrReplaceTempView("rxs_dycldrdjbm_first")
        val rxs_dycldrdjbm_first =sparkSession.sql(
            s"""
               |select * from (
               |  select b.*,row_number() over(partition by b.cldbs order by b.etl_time desc) as num
               |  from (
               |    select *
               |    from rxs_dycldrdjbm_first a
               |    where a.sjsj='${v_sjsj}' and a.etl_city in (${cityNameList})
               |    ) b
               |) t
               |where t.num = 1
               |
             """.stripMargin)
        rxs_dycldrdjbm_first.repartition(resultPartition)
          .createOrReplaceTempView("rxs_dycldrdjbm_first") // TODO: 这是什么表
        //rxs_dycldrdjbm_first.cache()
        //rxs_dycldrdjbm_first.count()

        /*--------------------------------------------------------------------------*/

        //3
        val rxs_dycldrdjbm_secend1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> tableName))
          .format("org.apache.kudu.spark.kudu")
          .load()

        rxs_dycldrdjbm_secend1.createOrReplaceTempView("rxs_dycldrdjbm_secend")// TODO: 这是什么表
        val rxs_dycldrdjbm_secend =sparkSession.sql(
            s"""
               |select *
               |from (
               |  select b.*,
               |    row_number() over(partition by b.cldbs order by b.etl_time desc) as num
               |  from (
               |    select *
               |    from rxs_dycldrdjbm_secend a
               |    where a.sjsj='${v_sjsj_before}' and a.etl_city in (${cityNameList})
               |    ) b
               |) t
               |where t.num = 1
               |
             """.stripMargin)
        rxs_dycldrdjbm_secend.repartition(resultPartition)
          .createOrReplaceTempView("rxs_dycldrdjbm_secend ")
        //rxs_dycldrdjbm_secend.cache()
        //rxs_dycldrdjbm_secend.count()

        /*--------------------------------------------------------------------------*/
        //计量合并后的数据
        val tmp_rxs_jlbmhb = sparkSession.sql(
            s"""
               |select
               |   a.cldbs,--测量点标识
               |   a.bjlx,--表计类型
               |   a.yhbh,--用户编号
               |   a.jldbh,--计量点编号
               |   a.bjzcbh,--表计资产编号
               |   a.zhbl,
               |   a.cldlxdm,--测量点类型代码
               |   a.jldbs,--计量点标识
               |   a.etl_time,
               |   bc.sjsj bccbrq,
               |   sc.sjsj sccbrq,
               |   row_number() over(partition by a.yhbh, a.jldbh, a.bjzcbh order by a.jldbs desc) as index_num,
               |   bc.sjsj,
               |   cast(coalesce(case when bc.zxygz='' then '0' else bc.zxygz end,'0')as double) zxygz,--正向有功总
               |   cast(coalesce(case when bc.zxygf='' then '0' else bc.zxygf end,'0')as double) zxygf,--正向有功峰
               |   cast(coalesce(case when bc.zxygp='' then '0' else bc.zxygp end,'0')as double) zxygp,--正向有功平
               |   cast(coalesce(case when bc.zxygg='' then '0' else bc.zxygg end,'0')as double) zxygg,--正向有功谷
               |   cast(coalesce(case when bc.zxygj='' then '0' else bc.zxygj end,'0')as double) zxygj,--正向有功尖
               |   cast(coalesce(case when bc.zxwgz='' then '0' else bc.zxwgz end,'0')as double) zxwgz,--正向无功总
               |   cast(coalesce(case when bc.fxygz='' then '0' else bc.fxygz end,'0')as double) fxygz,--反向有功总
               |   cast(coalesce(case when bc.fxygf='' then '0' else bc.fxygf end,'0')as double) fxygf,--反向有功峰
               |   cast(coalesce(case when bc.fxygp='' then '0' else bc.fxygp end,'0')as double) fxygp,--反向有功平
               |   cast(coalesce(case when bc.fxygg='' then '0' else bc.fxygg end,'0')as double) fxygg,--反向有功谷
               |   cast(coalesce(case when bc.fxygj='' then '0' else bc.fxygj end,'0')as double) fxygj,--反向有功尖
               |   cast(coalesce(case when bc.fxwgz='' then '0' else bc.fxwgz end,'0')as double) fxwgz,--反向无功总
               |   sc.sjsj sjsj_sc,
               |   cast(coalesce(case when sc.zxygz='' then '0' else sc.zxygz end,'0')as double) zxygz_sc,--正向有功总
               |   cast(coalesce(case when sc.zxygf='' then '0' else sc.zxygf end,'0')as double) zxygf_sc,--正向有功峰
               |   cast(coalesce(case when sc.zxygp='' then '0' else sc.zxygp end,'0')as double) zxygp_sc,--正向有功平
               |   cast(coalesce(case when sc.zxygg='' then '0' else sc.zxygg end,'0')as double) zxygg_sc,--正向有功谷
               |   cast(coalesce(case when sc.zxygj='' then '0' else sc.zxygj end,'0')as double) zxygj_sc,--正向有功尖
               |   cast(coalesce(case when sc.zxwgz='' then '0' else sc.zxwgz end,'0')as double) zxwgz_sc,--正向无功总
               |   cast(coalesce(case when sc.fxygz='' then '0' else sc.fxygz end,'0')as double) fxygz_sc,--反向有功总
               |   cast(coalesce(case when sc.fxygf='' then '0' else sc.fxygf end,'0')as double) fxygf_sc,--反向有功峰
               |   cast(coalesce(case when sc.fxygp='' then '0' else sc.fxygp end,'0')as double) fxygp_sc,--反向有功平
               |   cast(coalesce(case when sc.fxygg='' then '0' else sc.fxygg end,'0')as double) fxygg_sc,--反向有功谷
               |   cast(coalesce(case when sc.fxygj='' then '0' else sc.fxygj end,'0')as double) fxygj_sc,--反向有功尖
               |   cast(coalesce(case when sc.fxwgz='' then '0' else sc.fxwgz end,'0')as double) fxwgz_sc--反向无功总
               |from rxs_hbzdgx a
               |join rxs_dycldrdjbm_first bc on a.cldbs = bc.cldbs
               |join rxs_dycldrdjbm_secend sc on a.cldbs = sc.cldbs
            """.stripMargin).filter("index_num=1")
        tmp_rxs_jlbmhb.repartition(resultPartition).createOrReplaceTempView("tmp_rxs_jlbmhb")  //todo 计量编码合并
        //tmp_rxs_jlbmhb.cache()
        //tmp_rxs_jlbmhb.count()

        /*--------------------------------------------------------------------------*/

        //4
        val rxs_gycldssbm_first1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> addOneTableNameG))
          .format("org.apache.kudu.spark.kudu")
          .load()

        rxs_gycldssbm_first1.createOrReplaceTempView("rxs_gycldssbm_first")
        val rxs_gycldssbm_first =sparkSession.sql(
            s"""
               |select *
               |from (
               |  select b.*,
               |    row_number() over(partition by b.cldbs order by b.etl_time desc) as num
               |  from (
               |    select *
               |    from rxs_gycldssbm_first a
               |    where a.sjsj='${v_sjsj}' and a.etl_city in (${cityNameList})) b
               |) t
               |where t.num = 1
               |
             """.stripMargin)
        rxs_gycldssbm_first.repartition(resultPartition).createOrReplaceTempView("rxs_gycldssbm_first ")
        //rxs_dycldrdjbm_first.cache()
        //rxs_dycldrdjbm_first.count()


        /*--------------------------------------------------------------------------*/

        //5
        val rxs_gycldssbm_secend1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> tableNameG))
          .format("org.apache.kudu.spark.kudu")
          .load()

        rxs_gycldssbm_secend1.createOrReplaceTempView("rxs_gycldssbm_secend")
        val rxs_gycldssbm_secend =sparkSession.sql(
            s"""
               |select *
               |from (
               |  select b.*,
               |    row_number() over(partition by b.cldbs order by b.etl_time desc) as num
               |  from (
               |    select *
               |    from rxs_gycldssbm_secend a
               |    where a.sjsj='${v_sjsj_before}' and a.etl_city in (${cityNameList})
               |    ) b
               |) t
               |where t.num = 1
               |
             """.stripMargin)
        rxs_gycldssbm_secend.repartition(resultPartition).createOrReplaceTempView("rxs_gycldssbm_secend ")
        //rxs_dycldrdjbm_secend.cache()
        //rxs_dycldrdjbm_secend.count()


        /*--------------------------------------------------------------------------*/
        val tmp_rxs_jlbmhb_g = sparkSession.sql(
            s"""
               |select
               |    a.cldbs,--测量点标识
               |    a.bjlx,--表计类型
               |    a.yhbh,--用户编号
               |    a.jldbh,--计量点编号
               |    a.bjzcbh,--表计资产编号
               |    a.zhbl,
               |    a.cldlxdm,--测量点类型代码
               |    a.jldbs,--计量点标识
               |    a.etl_time,
               |    bc.sjsj bccbrq,
               |    sc.sjsj sccbrq,
               |    row_number() over(partition by a.yhbh, a.jldbh, a.bjzcbh order by a.jldbs desc) as index_num,
               |    bc.sjsj,
               |    cast(coalesce(case when bc.zxygz='' then '0' else bc.zxygz end,'0')as double) zxygz,--正向有功总
               |    cast(coalesce(case when bc.zxygf='' then '0' else bc.zxygf end,'0')as double) zxygf,--正向有功峰
               |    cast(coalesce(case when bc.zxygp='' then '0' else bc.zxygp end,'0')as double) zxygp,--正向有功平
               |    cast(coalesce(case when bc.zxygg='' then '0' else bc.zxygg end,'0')as double) zxygg,--正向有功谷
               |    cast(coalesce(case when bc.zxygj='' then '0' else bc.zxygj end,'0')as double) zxygj,--正向有功尖
               |    cast(coalesce(case when bc.zxwgz='' then '0' else bc.zxwgz end,'0')as double) zxwgz,--正向无功总
               |    cast(coalesce(case when bc.fxygz='' then '0' else bc.fxygz end,'0')as double) fxygz,--反向有功总
               |    cast(coalesce(case when bc.fxygf='' then '0' else bc.fxygf end,'0')as double) fxygf,--反向有功峰
               |    cast(coalesce(case when bc.fxygp='' then '0' else bc.fxygp end,'0')as double) fxygp,--反向有功平
               |    cast(coalesce(case when bc.fxygg='' then '0' else bc.fxygg end,'0')as double) fxygg,--反向有功谷
               |    cast(coalesce(case when bc.fxygj='' then '0' else bc.fxygj end,'0')as double) fxygj,--反向有功尖
               |    cast(coalesce(case when bc.fxwgz='' then '0' else bc.fxwgz end,'0')as double) fxwgz,--反向无功总
               |    sc.sjsj sjsj_sc,
               |    cast(coalesce(case when sc.zxygz='' then '0' else sc.zxygz end,'0')as double) zxygz_sc,--正向有功总
               |    cast(coalesce(case when sc.zxygf='' then '0' else sc.zxygf end,'0')as double) zxygf_sc,--正向有功峰
               |    cast(coalesce(case when sc.zxygp='' then '0' else sc.zxygp end,'0')as double) zxygp_sc,--正向有功平
               |    cast(coalesce(case when sc.zxygg='' then '0' else sc.zxygg end,'0')as double) zxygg_sc,--正向有功谷
               |    cast(coalesce(case when sc.zxygj='' then '0' else sc.zxygj end,'0')as double) zxygj_sc,--正向有功尖
               |    cast(coalesce(case when sc.zxwgz='' then '0' else sc.zxwgz end,'0')as double) zxwgz_sc,--正向无功总
               |    cast(coalesce(case when sc.fxygz='' then '0' else sc.fxygz end,'0')as double) fxygz_sc,--反向有功总
               |    cast(coalesce(case when sc.fxygf='' then '0' else sc.fxygf end,'0')as double) fxygf_sc,--反向有功峰
               |    cast(coalesce(case when sc.fxygp='' then '0' else sc.fxygp end,'0')as double) fxygp_sc,--反向有功平
               |    cast(coalesce(case when sc.fxygg='' then '0' else sc.fxygg end,'0')as double) fxygg_sc,--反向有功谷
               |    cast(coalesce(case when sc.fxygj='' then '0' else sc.fxygj end,'0')as double) fxygj_sc,--反向有功尖
               |    cast(coalesce(case when sc.fxwgz='' then '0' else sc.fxwgz end,'0')as double) fxwgz_sc--反向无功总
               |from rxs_hbzdgx a
               |join rxs_gycldssbm_first bc on a.cldbs = bc.cldbs
               |join rxs_gycldssbm_secend sc on a.cldbs = sc.cldbs
               |
            """.stripMargin).filter("index_num=1")
        tmp_rxs_jlbmhb_g.repartition(resultPartition).createOrReplaceTempView("tmp_rxs_jlbmhb_g")
        //tmp_rxs_jlbmhb_g.cache()
        //tmp_rxs_jlbmhb_g.count()

        /*--------------------------------------------------------------------------*/

        val jlbmhb = tmp_rxs_jlbmhb.union(tmp_rxs_jlbmhb_g)
        jlbmhb.repartition(resultPartition).createOrReplaceTempView("jlbmhb") //计量编码合并
        /*--------------------------------------------------------------------------*/
        end = System.currentTimeMillis()
        println("计量表数据源程序运行"+ (end-start)/1000 + "秒")

    }

/*----------------------------------------------------------------------*/














    /**
    * @Description: 数据源
    * @Param: [sparkSession, url]
    * @return: void
    * @Date: 2020/10/16
    */
    def odsGetBaseDatas(sparkSession:SparkSession,url:String)={
        start= System.currentTimeMillis()
        /*-----1 抄表信息--------------------------------------------------------------*/
        /*
        抄表信息（过去6个月）  yhbh-用户编号,zcbh-资产编号,zhbl-综合倍率,scbss-上次表示数,bcbss-本次表示数,
                bqcbcs-本期抄表示数,sccbrq-上次抄表日期,cbsj-抄表时间,bjdl-报警电量,jldbh-计量点编号,
                dfny-电费年月,yhmc-用户名称,jldcbfsdm-计量点抄表方式

        lixc20200506新加字段 cbsxh-抄表顺序号,sjcbfsdm-实际抄表方式代码,gzdbh-工作单编号,ywlbdm-业务类别代码,
                dqbm-地区编码,gddwbm-供电单位编码,cbrbs-抄表人标识,yddz-用电地址,cbqdbh-抄表区段编号,jldcbsxh-计量点抄表顺序号
         */

        val table1 ="csg_ods_yx.lc_cbxx_d_kudu"    //todo 这个在南网是全部的表
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
//        println("1 抄表信息" + cbxx.show(5))

        val l = cbxx.count()
        println(" 1")


        /*-----2 用电客户--------------------------------------------------------------*/
        //用电客户   yhbh-用户编号,yhmc-用户名称,yhztdm-用电状态,dydjdm-电压等级代码,gddwbm-供电单位编码,
        // lhrq-立户日期,cbqdbh-抄表区段,cbzq-抄表周期
        val table2 ="csg_ods_yx.kh_ydkh"  //todo 所有的 npmis的前缀全部去掉
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
        ydkh.createOrReplaceTempView("ydkh") // 2020/10/21 用电客户
//        println("2 用电客户" + ydkh.show(5))
        sparkSession.catalog.dropTempView("ydkh1")
        println(" 2")



        /*-----3 计量点--------------------------------------------------------------*/
        //计量点   jldbh-计量点编号,jldmc-计量点名称,xlxdbs-线路线段标识,yhbh-用户编号,tqbs-台区标识,jlfsdm-计量方式,jxfsdm-接线方式代码
        //lixc20200506新加字段 jldydjdm-计量电压等级代码
        val table3 ="csg_ods_yx.kh_jld"
        val kuduMap3: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table3)
        val jld1=sparkSession.read
          .options(kuduMap3)
          .format("org.apache.kudu.spark.kudu")
          .load()

        jld1.createOrReplaceTempView("jld1")

        val jld=sparkSession.sql(
            s"""
               |select
               |    jldbh,jldmc,jldytdm,xlxdbs,yhbh,tqbs,jlfsdm,jxfsdm,jldztdm,jldlbdm,cbfsdm,gddwbm,jldydjdm
               |from jld1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        jld.createOrReplaceTempView("jld")
        sparkSession.catalog.dropTempView("jld1")
//        println("3 计量点" + jld.show(5))
        println(" 3")


        /*----4 线路线段---------------------------------------------------------------*/
        //线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
        //lixc20200506只查配线
        val table4 ="csg_ods_yx.dw_xlxd"
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
//        println("4 线路线段" + all_xlxd.show(5))

        // 2020/10/27 注册函数
        Functions.getXLbhAndXLmc(sparkSession,all_xlxd)



        println(" 4")


        /*----5 台区---------------------------------------------------------------*/
        //台区  tqbs-台区标识,tqbh-台区编号,tqmc-台区名称
        val table5 ="csg_ods_yx.dw_tq"
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
//        println("5 台区" + all_tq.show(5))
        println(" 5")

        /*----6 线路台区关系---------------------------------------------------------------*/
        //线路台区关系  xlxdbs-线路线段标识,tqbs-台区标识
        val table6 ="csg_ods_yx.dw_xltqgx"
        val kuduMap6: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table6)
        val xltqgx1=sparkSession.read
          .options(kuduMap6)
          .format("org.apache.kudu.spark.kudu")
          .load()

        xltqgx1.createOrReplaceTempView("xltqgx1")
        val xltqgx =sparkSession.sql(
            s"""
               |select xlxdbs,tqbs
               |from xltqgx1
               |where dqbm in (${cityCodeList})
               |
             """.stripMargin)
        xltqgx.createOrReplaceTempView("xltqgx")
        sparkSession.catalog.dropTempView("xltqgx1") //线路台区关系
//        println("6 线路台区关系" + xltqgx.show(5))
        println(" 6")



        /*----7 用户电源信息---------------------------------------------------------------*/
        //用户电源信息  xlxdbs-线路线段标识,tqbs-台区标识,yhbh-用户编号,dybh-电源编号,dyxzdm-电源状态代码
        val table7 ="csg_ods_yx.kh_yhdyxx"
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
//        println("7 用户电源信息" + yhdyxx.show(5))
        println(" 7")


        /*----8 变压器---------------------------------------------------------------*/
        // 运行变压器  sbbs-唯一标识,mc-名称,zcmrid-资产mrid ,tqbs-台区标识,yhbh-用户编号,
        // edrl-设备铭牌上登记的容量,gbzbbz-公变专变标志,edrl-额定容量,
        // shlxdm-损耗类型代码,sbxhdm-设备型号代码,yxztdm-运行状态代码,kzsh-空载损耗,gddwbm-供电单位代码

        val table8 ="csg_ods_yx.dw_yxbyq"
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
//        println("8 变压器" + yxbyq.show(5))
        println(" 8")



        /*----9 计量点运行电能表关系---------------------------------------------------------------*/
        //计量点运行电能表关系  jldbh-计量点编号,yxdnbbs-运行电能表标识  接近一个亿的数据量
        val table9 ="csg_ods_yx.kh_jlddnbgx"
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
//        println("9 计量点运行电能表关系" + jlddnbgx.show(5))
        println(" 9")



        /*----10 运行电能表---------------------------------------------------------------*/
        //运行电能表 yxdnbbs-运行电能表标识,csbs-参数标识
        val table10 ="csg_ods_yx.sb_yxdnb"
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
//        println(" 10 运行电能表" + yxdnb.show(5))
        println(" 10")




        /*----11 电能表参数---------------------------------------------------------------*/  // 2020/10/19 读到这里就出问题了
        //电能表参数  csbs-参数标识,eddydm-额定电压
//        val table11 ="csg_ods_yx.sb_dnbcs"
//        val kuduMap11: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table11)
//        val dnbcs1=sparkSession.read
//          .options(kuduMap11)
//          .format("org.apache.kudu.spark.kudu")
//          .load()
        val table11 ="csg_ods_yx.sb_dnbcs"
        val dnbcs1 = sparkSession.read.format("org.apache.kudu.spark.kudu")
          .options(Map[String,String]("kudu.master"->url,"kudu.table"-> "csg_ods_yx.sb_dnbcs" ))
          .load


        dnbcs1.createOrReplaceTempView("dnbcs")
        val dnbcs =sparkSession.sql(
            s"""
               |select csbs,eddydm
               |from dnbcs
             """.stripMargin)
        dnbcs.createOrReplaceTempView("dnbcs") //电能表参数
//        println(" 11 电能表参数" + dnbcs.show(5))
        println(" 11")






        /*----12 计量点信息---------------------------------------------------------------*/
        //计量点信息(过去6个月) yhbh-用户编号,yhmc-用户名称,jldytdm-计量表分类,jfdl-计费电量,dfny-电费年月,xlxdbs-线路线段标识,
        //      tbgzdbh-退补工作单编号,tqbs-台区标识，jldbh-计量点编号,gddwbm-供电单位编码,
        //      yhlbdm-用户类别代码,bqcbcs-本期抄表次数,yhztdm-用户状态代码,jlfsdm-计量方式代码,ygbsdl-有功变损电量,
        //      ygcjdl-有功抄见电量,jldydjdm-计量点电压等级代码,hyfldm-行业分类代码
        //
        //lixc20200506新加字段 djdm-电价代码,cbjhbh-抄表计划编号,jldcbsxh-计量点抄表顺序号,yddz-用电地址,
        //      cbqdbh-抄表区段编号,mfdl-免费电量,jfrl-计费容量,ywlbdm-业务类别代码,jldxh-计量点序号,ygtbdl-有功退补电量,
        //      ygfbkjdl-有功分表扣减电量,dqbm-地区编码,cbrq-抄表日期,dwbm-单位编码
        val table12 ="csg_ods_yx.hs_jldxx_d_kudu"  //hs_jldxx_d
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
        jldxx_all.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("jldxx_all")//计量点信息
        sparkSession.catalog.dropTempView("jldxx1")
//        println(" 12 计量点信息" + jldxx_all.show(5))
        println(" 12")





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
        println(" 13 计量点信息")
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
        jldxx.cache().createOrReplaceTempView("jldxx") //计量点信息
//        println(" 13 计量点信息" + jldxx.show(5))
        println(" 13")

        /*----14 退补电量信息---------------------------------------------------------------*/
        //退补电量信息 ygdl-有功电量,gzdbh-工作单编号,ccyf-差错月份
        println(" 14")
        val table14 ="csg_ods_yx.hs_tbdlxx"
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
//        println(" 14 退补电量信息" + tbdlxx.show(5))
        println(" 14")


        /*----15 退补明细---------------------------------------------------------------*/
        //退补明细
        println(" 15")
        val table15 ="csg_ods_yx.hs_tbmx"
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
//        println(" 15 退补明细" + cztb.show(5))
        println(" 15")





        /*----16 光伏用户档案信息---------------------------------------------------------------*/
        //光伏用户档案信息 yhbh-用户编号
        println(" 16")
        val table16 ="csg_ods_yx.sc_gf_gfyhdaxx"
        val kuduMap16: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table16)
        val gfyhdaxx1=sparkSession.read
          .options(kuduMap16)
          .format("org.apache.kudu.spark.kudu")
          .load()

        gfyhdaxx1.createOrReplaceTempView("gfyhdaxx")
        val gfyhdaxx =sparkSession.sql(
            s"""
               |select yhbh
               |from gfyhdaxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        gfyhdaxx.createOrReplaceTempView("gfyhdaxx") //光伏用户档案信息
//        println(" 16 光伏用户档案信息" + gfyhdaxx.show(5))
        println(" 16")


        /*----17 光伏用户关联信息---------------------------------------------------------------*/
        //光伏用户关联信息 gfhyhbh-光伏户用户编号,gdhyhbh-购电户用户编号,ydhyhbh-用电户用户编号
        println(" 17")
        val table17 ="csg_ods_yx.sc_gf_gfyhglxx"
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
        gfyhglxx.createOrReplaceTempView("gfyhglxx") //光伏用户关联信息
//        println(" 17 光伏用户关联信息" + gfyhglxx.show(5))
        println(" 17")



        /*----18 线路编组信息---------------------------------------------------------------*/
        //线路编组信息  bdzbh-变电站编号,bdzmc-变电站名称,bdzbs-变电站标识,xlbh-线路编号,xlmc-线路名称,
        //      xlxdbs-线路线段标识,xlbzbh-线路编组编号,create_time-创建时间,gddwbm-供电单位编码
        println(" 18")
        val table18 ="csg_ods_yx.gk_xlbzxx"
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
//        println(" 18 线路编组信息" + xlbzxx.show(5))
        println(" 18")



        /*----19 台区编组信息---------------------------------------------------------------*/
        //台区编组信息
        println(" 19")
        val table19 ="csg_ods_yx.gk_tqbzxx"
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
//        println(" 19 台区编组信息" + tqbzxx.show(5))
        println(" 19")


        /*----20 违窃信息 ---------------------------------------------------------------*/
        //违窃信息   chsj-查获时间,yzbdl-应追补电量,yhbh-用户编号,gzdbh-工作单编号
        println(" 20")
        val table20 ="csg_ods_yx.fw_wqxx"
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
        wqxx.createOrReplaceTempView("wqxx") //违窃信息
//        println(" 20 违窃信息" + wqxx.show(5))
        println(" 20")



        /*----21 工作单信息---------------------------------------------------------------*/
        //工作单信息  gzdbh-工作单编号
        println(" 21")
        val table21 ="csg_ods_yx.xt_gzdxx"
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
               |from gzdxx
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        gzdxx.createOrReplaceTempView("gzdxx")
//        println(" 21 工作单信息" + gzdxx.show(5))
        println(" 21")





        /*----22 线路和台区指标---------------------------------------------------------------*/
        //线路和台区指标
        println(" 22")
        val table22 ="csg_ods_yx.gk_xlhtqzb"
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
//        println(" 22 线路和台区指标" + xlhtqzb.show(5))
        println(" 22")



        /*----23 线损责任人---------------------------------------------------------------*/
        //线损责任人  rybh-人员编号,rymc-人员名称,rybs-人员标识,xszrrbs-线损责任人标识
        println(" 23")
        val table23 ="csg_ods_yx.gk_xszrr"
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
        xszrr.createOrReplaceTempView("xszrr") //线损责任人
//        println(" 23 线损责任人" + xszrr.show(5))
        println(" 23")


/*     // 2020/10/23 月线路未采集用户清单 不涉及到这表
        /*----24 应收电费记录---------------------------------------------------------------*/
        报错：Caused by: org.apache.kudu.client.NonRecoverableException: Tablet is lagging too much to be able to serve snapshot scan. Lagging by: 11753316501 ms, (max is 30000 ms):
        集群问题
 */


 /*
        //应收电费记录  yhbh-用户编号,yhmc-用户名称,ysdf-应收电费,jldbh-计量点编号,gddwbm-供电单位编码,yhlbdm-用户类别代码
        //lixc20200506新增字段 ywlbdm-业务类别代码,djdm-电价代码
        val table24 ="csg_ods_yx.zw_ysdfjl_d_kudu"  //zw-账务域
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
               |where dqbm in (${cityCodeList})  --用户所在的地区编码
               |
             """.stripMargin)
        ysdfjl.createOrReplaceTempView("ysdfjl") //应收电费记录
        println(" 24 应收电费记录" + ysdfjl.show(5))

*/


        /*----25抄表区段信息---------------------------------------------------------------*/
        //抄表区段信息  cbqdbh-抄表区段编号,cbqdmc-抄表区段名称
        println(" 25")
        val table25 ="csg_ods_yx.lc_cbqdxx"
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
        cbqdxx.createOrReplaceTempView("cbqdxx") //抄表区段信息
//        println(" 25抄表区段信息" + cbqdxx.show(5))
        println(" 25")




        /*----26 运行计量自动化终端---------------------------------------------------------------*/
        //运行计量自动化终端   yxjlzdhzdbs-运行计量自动化标识,yhbh-用户编号,sblbdm-设备类别代码
        println(" 26")
        val table26 ="csg_ods_yx.sb_yxjlzdhzd"
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
        yxjlzdhzd.createOrReplaceTempView("yxjlzdhzd")  //运行计量自动化终端
//        println(" 26 运行计量自动化终端" + yxjlzdhzd.show(5))
        println(" 26")



        /*----27 计量自动化终端采集关系记录---------------------------------------------------------------*/
        //计量自动化终端采集关系记录   yxjlzdhzdbs-运行计量自动化标识,cjdxyhbh-采集对象用户编号
        println(" 27")
        val table27 ="csg_ods_yx.sb_jlzdhzdcjgxjl"
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
//        println(" 27 计量自动化终端采集关系记录" + jlzdhzdcjgxjl.show(5))
        println(" 27 ")



        /*----28 变电站线路关系---------------------------------------------------------------*/
        //变电站线路关系
/*
        val table28 ="csg_ods_yx.dw_bdzxlgx"  // 2020/10/20 这个表不在kudu数据库,在hive库
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
        println(" 28 变电站线路关系-" + bdzxlgx.show(5))
*/
        // 2020/10/20 这个表不在kudu数据库,在hive库
        val bdzxlgx1 = sparkSession.sql("select bdzxlglgxbs, bdzbs, xlxdbs, jcxbz, kgbh, xlctyxfsbz, xlctyxfsbz2,cjsj,czsj,dqbm from csg_ods_yx.dw_bdzxlgx ")
        val bdzxlgx = bdzxlgx1.where(s"dqbm in (${cityCodeList})").select("xlxdbs","bdzbs")
        bdzxlgx.createOrReplaceTempView("bdzxlgx")
//        println(" 28 变电站线路关系-" + bdzxlgx.show(5))
        println(" 28 ")



        /*----29 变电站---------------------------------------------------------------*/
        //变电站
/*
        println(" 29")
        val table29 ="csg_ods_yx.dw_bdz"
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
        bdz.createOrReplaceTempView("bdz") //变电站
        println("29 变电站" + bdz.show(5))
*/
        // 2020/10/20 在hive表数据库
        val bdz1 = sparkSession.sql("select * from csg_ods_yx.dw_bdz")
        val bdz = bdz1.where(s"dqbm in (${cityCodeList})").select("bdzbs", "bdzbh", "bdzmc")
        bdz.createOrReplaceTempView("bdz")
//        println("29 变电站" + bdz.show(5))
        println("29 ")

        /*----30 业务级联---------------------------------------------------------------*/
        //业务级联

        val table30 ="csg_ods_yx.xt_ywjl"
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
        ywjl.createOrReplaceTempView("ywjl")//业务级联
//        println(" 业务级联" + ywjl.show(5))
        println(" 30")




        /*----31 工作单信息历史---------------------------------------------------------------*/
        //工作单信息历史

        val table31 ="csg_ods_yx.xt_gzdxxls"
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
        gzdxxls.createOrReplaceTempView("gzdxxls") //工作单信息历史
//        println(" 31 工作单信息历史" + gzdxxls.show(5))
        println(" 31")




        /*----32 组织---------------------------------------------------------------*/
        //组织  zzbm-组织编号,zzmc-组织名称

        val table32 ="csg_ods_yx.xt_zz"
        val kuduMap32: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table32)
        val zz1=sparkSession.read
          .options(kuduMap32)
          .format("org.apache.kudu.spark.kudu")
          .load()

        zz1.createOrReplaceTempView("zz")
        //zzbm 组织编码, zzmc 组织名称,  zzlxdm 组织类型代码,  dqbm 地区编码
        val zz =sparkSession.sql(
            s"""
               |select
               |    zzbm,zzmc,zzlxdm,dqbm
               |from zz
             """.stripMargin)
        zz.createOrReplaceTempView("zz") //各组织关系
//        println(" 32 组织" + zz.show(5))
        println(" 32")

        // 2020/10/27 获取供电所编码
        Functions.getbm(sparkSession,zz) // 2020/10/27 获取编码
        Functions.getDQbm(sparkSession,zz) // 2020/10/27 获取地区编码函数





        /*----33 代码编码---------------------------------------------------------------*/
        //代码编码

        val table33 ="csg_ods_yx.xt_dmbm"
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
        dmbm.createOrReplaceTempView("dmbm") //代码编码
//        println(" 33 代码编码" + dmbm.show(5))
        println(" 33")


        /*----34 线路管理单位---------------------------------------------------------------*/
        //线路管理单位

/*
        val table ="csg_ods_yx.dw_xlgldw"
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
        println(" 34 线路管理单位" + xlgldw.show(5))
*/
        val xlgldw1 = sparkSession.sql("select * from csg_ods_yx.dw_xlgldw")
        val xlgldw = xlgldw1.where(s"dqbm in (${cityCodeList})")
        xlgldw.createOrReplaceTempView("xlgldw")
//        println(" 34 线路管理单位" + xlgldw.show(5))
        println(" 34")// 2020/10/20 在hive数据库



        /*----35 业扩工作单基本信息---------------------------------------------------------------*/
        //业扩工作单基本信息

        val table35 ="csg_ods_yx.fw_ykgzdjbxx"
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
        ykgzdjbxx.createOrReplaceTempView("ykgzdjbxx")  //业扩工作单基本信息
//        println(" 35 业扩工作单基本信息" + ykgzdjbxx.show(5))
        println(" 35")

/*
        /*----36---------------------------------------------------------------*/
        println(" 36")
        val table36 ="csg_ods_yx.wfrt_task_exec_info" // TODO: 这张什么表？  读取不了
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
        println(" wfrt_task_exec_info:" + wfrt_task_exec_info.show(5))
*/


        /*----37 线损报表分区线损统计信息---------------------------------------------------------------*/
        //线损报表分区线损统计信息

        val table37 ="csg_ods_yx.gk_xsbbfqxstjxx"
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
//        println(" 37 线损报表分区线损统计信息-" + xsbbfqxstjxx.show(5))
        println(" 37")


        /*----38---------------------------------------------------------------*/
        //2020-01-13

        val table38 ="csg_ods_zc.sp_so_runmode_device"  // 2020/10/16 表的详情没有
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
//        println(" 抄表信息" + cbxx1.show(5))
        println(" 38")


        /*----39---------------------------------------------------------------*/

        val table39 ="csg_ods_zc.sp_so_runmode_manage"   // 2020/10/16 表的详情没有
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
        println(" 39")

  /*

        /*----40---------------------------------------------------------------*/
        //环网操作信息维护
        val table40 ="csg_ods_yx.gk_hwczxxwh" //todo 这个表在 csg_ods_yx 没有  2020/10/16 用于维护环网转供、跨区域线路联络开关表计档案关系
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
*/



        /*----41 换表信息记录---------------------------------------------------------------*/
        //11月18号新增
        //换表信息记录

        val table41 ="csg_ods_yx.lc_hbxxjl"
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
               |from hbxxjl
               |where dqbm in (${cityCodeList})
             """.stripMargin)
        hbxxjl.createOrReplaceTempView("hbxxjl")
//        println(" 41 换表信息记录" + hbxxjl.show(5))
        println(" 41")



        /*----42 变电站考核表------- 二次处理的表 --------------------------------------------------------*/
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
        Functions.getBDZxx(sparkSession)// 2020/11/3 自定义函数获取变电站信息（3个）
        println(" 42")



        /*----43 购电量价费汇总信息表---------------------------------------------------------------*/
        //购电量价费汇总信息表 2019年11月19日11:23:10 lixc

        val table43 ="csg_ods_yx.sc_gdljfhzxx"
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
//        println(" 43 购电量价费汇总信息表" + gdljfhzxx.show(5))
        println(" 43")



        /*----44 电厂辅助信息表---------------------------------------------------------------*/
        //电厂辅助信息表 2019年11月19日11:23:14 lixc

        val table44 ="csg_ods_yx.sc_dcfzxx"
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
//        println(" 44 电厂辅助信息表" + dcfzxx.show(5))
        println(" 44")




/*
        /*----45 结算单元表 ---------------------------------------------------------------*/
        //结算单元表 2019年11月19日11:23:14 lixc
        println(" 45")
        val table45 ="csg_ods_yx.sc_jsdy"
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
        jsdy.createOrReplaceTempView("jsdy")  // 2020/10/26 结算单元表
        println(" 45 结算单元表" + jsdy.show(5))

*/


/*  // 2020/10/20 jsdy 这个表没有，注释掉了
        /*----46-------  二次操作 --------------------------------------------------------*/
        //线路地方电厂上网电量 2019年11月19日11:14:42 lixc
        println(" 46")
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

 */

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
        sparkSession.sql(tqdfdcswdl).createOrReplaceTempView("tqdfdcswdl") //台区地方电厂上网电量
        println(" 台区地方电厂上网电量")





        /*----47 变损标准表 ---------------------------------------------------------------*/
        //变损标准表 2019年11月20日17:04:22 lixc

        val table47 ="csg_ods_yx.hs_bsbz"
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
//        println(" 47 变损标准表 " + bsbz.show(5))
        println(" 47")



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
        sparkSession.sql(byqygdsh).createOrReplaceTempView("byqygdsh")//变压器月固定损耗
        println(" 48")




        /*----49 电子化移交工作单信息表---------------------------------------------------------------*/
        //电子化移交工作单信息表 2019年11月21日19:40:58 lixc

        val table49 ="csg_ods_yx.yp_dzhyjgzdxx"  //在服务域 fw.yp.
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
//        println(" 49 电子化移交工作单信息表" + dzhyjgzdxx.show(5))
        println(" 49")

        /*----50 人员表---------------------------------------------------------------*/
        //20200108  人员表

        val table50 ="csg_ods_yx.xt_ry"
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
//        println(" 50 人员表" + ry.show(5))
        println(" 50")


        /*----51---------------------------------------------------------------*/
        // 2020/10/16  csg_ods_gis系统  trans_id-跟变压器的gisid对应, circuit_id-跟线路线段的gisid对应

        val table51 ="csg_ods_gis.transcircuitsupply"  // 2020/10/16 没找到这个表的详情
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
//        println(" 51 抄表信息" + cbxx1.show(5))
        println(" 51")



        /*----52---------------------------------------------------------------*/
         // 2020/10/16 没有找到表详情
        // 2020/10/23  在hive表里面
        val gis_lvcustomersupply1 = sparkSession.sql("select * from csg_ods_gis.lvcustomersupply")
        val gis_lvcustomersupply = gis_lvcustomersupply1.select("trans_id", "customer_mrid")
        gis_lvcustomersupply.createOrReplaceTempView("gis_lvcustomersupply")
//        println(" 52 table " + gis_lvcustomersupply.show(5))
        println(" 52")

        /*----53---------------------------------------------------------------*/
        // 2020/10/16 没有找到表详情
        // 2020/10/23  在hive表里面
        val gis_g_dm_function_location1 = sparkSession.sql("select * from csg_ods_gis.g_dm_function_location")
        val gis_g_dm_function_location = gis_g_dm_function_location1.select("id","classify_id","xfmr_type","fl_name")
        gis_g_dm_function_location.createOrReplaceTempView("gis_lvcustomersupply")
//        println(" 53 table " + gis_g_dm_function_location.show(5))



        /*----54------ 基于临时表操作 ---------------------------------------------------------*/
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
        println(" 54") // 2020/11/3 日志看到54，说明跑完54了


/*  2020-10-15
        /*----55 异常供电单位线路对应关系---------------------------------------------------------------*/
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
        ycgddwxlgx.createOrReplaceTempView("ycgddwxlgx")  //异常供电单位线路对应关系
        println(" 抄表信息" + cbxx1.show(5))
println(" 55")

    */

        /*----56---------------------------------------------------------------*/
        //异常规则表（这个好像是输出表）
        val table56 ="impala::lineloss.gk_xsycgz"  // 2020/11/2
        val kuduMap56: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table56)
        val xsycgz=sparkSession.read
          .options(kuduMap56)
          .format("org.apache.kudu.spark.kudu")
          .load()
//        xsycgz.show(5)
        xsycgz.createOrReplaceTempView("xsycgz")//异常规则表
        println("56 抄表信息")

        Functions.getGZmc(sparkSession,xsycgz) // 2020/10/27 获取规则名称函数
        Functions.getYcgzbh(sparkSession,xsycgz) // 2020/10/27 获取异常规则编号
        Functions.getYcgzfl(sparkSession,xsycgz) // 2020/10/27 获取异常规则分类
        Functions.decideInfoSystem(sparkSession,xsycgz) // 2020/10/27 判断是否是一级分类中的信息系统问题
        Functions.getGzParameter(sparkSession,xsycgz) // 2020/10/27 获取规则参数函数




        /*----57 购电交易差错退补信息---------------------------------------------------------------*/
        //lixc20200506新加 购电交易差错退补信息

        val table57 ="csg_ods_yx.sc_gdjycctbxx"
        val kuduMap57: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table57)
        try {
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
            gdjycctbxx.show(5)
        }catch {
            case e:Exception => e.printStackTrace()
        }finally{
            println(" 57")
        }





        /*----58 电厂机组信息---------------------------------------------------------------*/
        //lixc20200506新加 电厂机组信息

        val table58 ="csg_ods_yx.sc_dcjzxx"
        val kuduMap58: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table58)
        try {
            val dcjzxx1=sparkSession.read
              .options(kuduMap58)
              .format("org.apache.kudu.spark.kudu")
              .load()
            dcjzxx1.createOrReplaceTempView("dcjzxx")
            val dcjzxx =sparkSession.sql(
                s"""
                   |select
                   |    jsdybh,jldbh,dqbm
                   |from dcjzxx
                   |where dqbm in (${cityCodeList})
             """.stripMargin)
            dcjzxx.createOrReplaceTempView("dcjzxx") //电厂机组信息

        }catch {
            case e:Exception => e.printStackTrace()
        }
        println(" 58")

        //        println(" 58 电厂机组信息" + dcjzxx.show(5))




        /*----59 抄表区段辅助---------------------------------------------------------------*/
        //lixc20200506新加 抄表区段辅助

        val table59 ="csg_ods_yx.lc_cbqdfz"
        val kuduMap59: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table59)
        try {
            val cbqdfz1=sparkSession.read
              .options(kuduMap59)
              .format("org.apache.kudu.spark.kudu")
              .load()

            cbqdfz1.createOrReplaceTempView("cbqdfz")
            val cbqdfz =sparkSession.sql(
                s"""
                   |select
                   |    cbqdbh,cblr,dqbm
                   |from cbqdfz
                   |where dqbm in (${cityCodeList})
             """.stripMargin)
            cbqdfz.createOrReplaceTempView("cbqdfz") //抄表区段辅助

        }catch {
            case e:Exception => e.printStackTrace()
        }
        println(" 59")
//        println(" 59 抄表区段辅助" + cbqdfz.show(5))


        /*----60 线损专变公用用户---------------------------------------------------------------*/
        //lixc20200506新加 线损专变公用用户
        val table60 ="csg_ods_yx.gk_xszbgyyh"
        val kuduMap60: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table60)
        try {
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

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            println("60")
        }

//        println(" 60 线损专变公用用户" + xszbgyyh.show(5))


        /*----61 核算计量点关系---------------------------------------------------------------*/
        //lixc20200506新加 核算计量点关系
        val table61 ="csg_ods_yx.hs_jldgx_d"
        val kuduMap61: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table61)
        try {
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

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            println("61  ")
        }

//        println(" 61 核算计量点关系" + hs_jldgx.show(5))



        /*----62 非周期计费申请---------------------------------------------------------------*/
        //lixc20200506新加 非周期计费申请
        println(" 62")
        val table62 ="csg_ods_yx.hs_fzqjfsq"
        val kuduMap62: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table62)
        try {
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

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            println("62")
        }

//        println(" 62 非周期计费申请" + fzqjfsq.show(5))

        /*----63 计量点关系---------------------------------------------------------------*/
        //lixc20200604新加 计量点关系
        val table63 ="csg_ods_yx.kh_jldgx"
        val kuduMap63: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table63)
        try {
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

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            println("63")
        }

//        println(" 63 计量点关系" + jldgx.show(5))


        /*----64 计量自动化故障任务信息---------------------------------------------------------------*/
        //lixc20200520新加 计量自动化故障任务信息
         // 2020/10/23 报错：Caused by: org.apache.kudu.client.NonRecoverableException: cannot complete before timeout: ScanRequest

        val table64 ="csg_ods_yx.sb_jlzdhgzrwxx"
        val kuduMap64: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table64)
        try {
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
            jlzdhgzrwxx.show(5)

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            println("64")
        }





        /*----65 计量自动化故障任务明细---------------------------------------------------------------*/
       // 2020/10/23 报错：Caused by: org.apache.kudu.client.NonRecoverableException: cannot complete before timeout: ScanRequest   不是 月线路未采集用户清单 必须的数据，先注释掉

        //lixc20200520新加 计量自动化故障任务明细
        val table65 ="csg_ods_yx.sb_jlzdhgzrwmx"
        val kuduMap65: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table65)
        try {
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
            jlzdhgzrwmx.show(5)

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            println("65 计量自动化故障任务明细")
        }





        /*   // 2020/11/3 有空问 祥成
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
        val table67 =tableNameC // TODO: 2020-10-15 没有这个表
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
*/


        /*----68--  二次操作 -------------------------------------------------------------*/
        //三相负荷不平衡率

 /*  2020-10-15    上面的clddldy表不存在
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

        sxfhbphl.createOrReplaceTempView("sxfhbphl")  //三相负荷不平衡率
        //三相负荷不平衡率tq_41 end
*/

        end = System.currentTimeMillis()
        println("程序运行时间" + (end-start)/1000 + "秒") // 2020/10/16
        /*-------------------------------------------------------------------*/
        /*-------------------------------------------------------------------*/


    }


    /*--------------------------------------------------------------------------*/
    var xhjyqybz = 0//  现货交易启用标志
    var gstqbz = 0 //购售同期标志
    var xsgdbz =0 //  线损过渡标志
    var gdqcbcs = 0 //过渡期抄表次数

    var cbnyq = 0
    var cbnyz = 0
    var dcqsyq = 0
    var dcqssy = 0
    /*--------------------------------------------------------------------------*/


    /**
    * @Description: 电量明细
    * @Param: [sparkSession, url]
    * @return: void
    * @Date: 2020/10/16
    */
    def powerDetail(sparkSession:SparkSession,url:String)={
        println("电量明细数据源")
        start=System.currentTimeMillis()

/*        // 2020/10/23 报错：Caused by: org.apache.kudu.client.RecoverableException: Service unavailable: Timed out: could not wait for desired snapshot timestamp to be consistent: Tablet is lagging too much to be able to serve snapshot scan
            读不出来先注释
        // 库表全局系数
        val xt_qjxs=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "csg_ods_yx.xt_qjxs"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        xt_qjxs.createOrReplaceTempView("xt_qjxs") // 2020/10/21 库表全局系数
        println(" 1 库表全局系数" + xt_qjxs.show(5))



        /*-------------------------------------------------------------------*/

        //v1  现货交易启用标志
        val xhjyqybzArr=xt_qjxs.collect()

        for (num <- xhjyqybzArr){
            xhjyqybz = num.getAs[Int]("xhjyqybz")  //这东西返回给哪里调用？  //  现货交易启用标志
        }
        println("现货交易启用标志:"+xhjyqybz)
*/
        /*-------------------------------------------------------------------*/

/*
        // 2020/10/23 xt_qjxs 这个表数据读不出来
        //v2 购售同期标志
        val gstqbzArr =sparkSession.sql(
            s"""
               |select cast(t.xsz as Int) gstqbz
               |from xt_qjxs t
               |where t.xsbh = '52624'
               |
             """.stripMargin).collect()

        //        var gstqbz = 0
        for (num <- gstqbzArr){
            gstqbz=num.getAs[Int]("gstqbz") // 购售同期标志
        }
        //销毁临时表
        //sparkSession.catalog.dropTempView("yxxt_npmis_xt_qjxs")
        println("购售同期标志:"+gstqbz)
*/


//        /*-------------------------------------------------------------------*/
//        //v3  线损过渡标志
//        // 2020/10/23 xt_qjxs 这个表数据读不出来
//        val xsgdbzArr =sparkSession.sql(
//            s"""
//               |select cast(t.xsz as Int) xsgdbz
//               |from xt_qjxs t
//               |where t.xsbh = '54624'
//               |
//             """.stripMargin).collect()
//        //        var xsgdbz = 0
//        for (num <- xsgdbzArr){
//            xsgdbz = num.getAs[Int]("xsgdbz") //  线损过渡标志
//        }
//        //销毁临时表
//        //sparkSession.catalog.dropTempView("_yxxt_npmis_xt_qjxs")
//        println("线损过渡标志:"+xsgdbz)



        /*-------------------------------------------------------------------*/
        //4 过渡期抄表次数
        // 2020/10/23 xt_qjxs 这个表数据读不出来
//        val gdqcbcsArr =sparkSession.sql(
//            s"""
//               |select cast(t.xsz as Int) gdqcbcs
//               |from xt_qjxs t
//               |where t.xsbh = '52625'
//               |
//             """.stripMargin).collect()
//
//        for (num <- gdqcbcsArr){
//            gdqcbcs = num.getAs[Int]("gdqcbcs") // 过渡期抄表次数
//        }
//        //销毁临时表
//        //sparkSession.catalog.dropTempView("_yxxt_npmis_xt_qjxs")
//        println("过渡期抄表次数:"+gdqcbcs)

        /*-----------上面求了4个变量--------------------------------------------------------*/
        //        var cbnyq = 0;
        //        var cbnyz = 0;
        //        var dcqsyq = 0;
        //        var dcqssy = 0;

        /*-------------------------------------------------------------------*/
        // 准备专变 非周期数据TMP_GK_XSFZQJFDL
        val xsfzqjfdl =
        s"""
           |select
           |    a.gddwbm,a.dfny,a.yhlbdm,a.xlxdbs,a.tqbs,a.jfdl,a.mfdl,
           |    a.cbrq,a.jldbh,a.ywlbdm,a.cbqdbh,a.gzdbh,a.dwbm,a.djdm,a.dqbm
           |from jldxx a
           |where a.dfny = ${nowMonth}
           |    and a.yhlbdm in ('10', '11', '30','98')
           |    and a.djdm not in ('00000000', '39002101')
           |    and a.jldydjdm not in ('10', '12', '13', '15')
           |    and a.cbjhbh like 'f%'
           |union all
           |select
           |    a.gddwbm,a.dfny,a.yhlbdm,a.xlxdbs,a.tqbs,a.jfdl,a.mfdl,
           |    a.cbrq,a.jldbh,a.ywlbdm,a.cbqdbh,a.gzdbh,a.dwbm,a.djdm,a.dqbm
           |from jldxx a
           |where a.dfny = ${lastMonth}
           |    and a.yhlbdm in ('10', '11', '30','98')
           |    and a.djdm not in ('00000000', '39002101')
           |    and a.jldydjdm not in ('10', '12', '13', '15')
           |    and a.cbjhbh like 'f%'
           |
             """.stripMargin
        sparkSession.sql(xsfzqjfdl).createOrReplaceTempView("xsfzqjfdl")  // 2020/10/21 准备专变 非周期数据
//        sparkSession.sql("select * from xsfzqjfdl limit 5").show()
        println(" 2 准备专变 非周期数据")

        /*-------------------------------------------------------------------*/
        //线路售电量明细
        // 1线路售电量专变用户周期部分
        // 2线路售电量台区考核表本月一次抄表部分
        // 3线路售电量违约窃电部分
        // 4线路售电量变电站考核表反向电量部分
        // 5线路售电量变电站考核表反向退补电量部分
        // 6线路售电量专变公用的扣减电量部分
        // 7线路售电量专变用户非周期部分
        // 8线路售电量台区考核户反向部分
        // 9线路售电量台区考核表上月二次抄表部分
//        val xlsdlmx =
//        s"""
//           |select * from (
//           |-- 1 线路售电量专变用户周期部分
//           |  select
//           |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,null zcbh,cbqdbh,
//           |    yhlbdm,(coalesce(jfdl, 0) + coalesce(mfdl, 0)) ygzdl,jfrl,null cbsxh,null cbrbs,
//           |    null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'1' dllx
//           |  from jldxx t
//           |  where t.djdm <> '00000000'
//           |      and t.jldydjdm not in ('10', '12', '13', '15')
//           |      and t.yhlbdm in ('10', '11', '30','98')
//           |      and t.dfny = ${nowMonth} and (t.cbjhbh not like 'f%' or t.cbjhbh is null)
//           |  union all
//           |-- 2 线路售电量台区考核表本月一次抄表部分
//           |  select
//           |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,null zcbh,cbqdbh,
//           |    yhlbdm,(coalesce(ygzdl, 0) - coalesce(ygbsdl, 0)) ygzdl,jfrl,null cbsxh,
//           |    null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'2' dllx
//           |  from jldxx t
//           |  where t.dfny = ${nowMonth} and t.yhlbdm = '60' and t.bqcbcs = 1
//           |  union all
//           |-- 3 线路售电量违约窃电部分
//           |  select
//           |    t.gddwbm,t.dfny,t.yhbh,t.yhmc,0 jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,null zcbh,
//           |    t.cbqdbh,t.yhlbdm,coalesce(t.jfdl, 0) ygzdl,0 jfrl,null cbsxh,null cbrbs,t.czsj,t.jldbh,
//           |    null scbss,null bcbss,nullzhbl,0 ygbsdl,null sjcbfsdm,'3' dllx
//           |  from ysdfjl t --应收电费记录
//           |  where t.ywlbdm = '0500' and t.djdm not in ('00000000', '39002101')
//           |      and t.yhlbdm <> '20'
//           |      and t.dfny = ${nowMonth}
//           |  union all
//           |-- 4 线路售电量变电站考核表反向电量部分
//           |  select
//           |    e.gddwbm, e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs, e.yddz,e.tqbs,f.zcbh,
//           |    e.cbqdbh,e.yhlbdm,(coalesce(f.bjdl,0) + coalesce(f.jbdl,0)) ygzdl,e.jfrl,f.cbsxh,null cbrbs,
//           |    f.cbsj,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'4' dllx
//           |  from jldxx e, cbxx f
//           |  where e.dfny = ${nowMonth}
//           |      and e.yhlbdm = '80' and f.yhbh = e.yhbh
//           |      and f.gzdbh = e.gzdbh and e.ywlbdm = f.ywlbdm and f.dfny = e.dfny and f.jldbh = e.jldbh
//           |      and f.sslxdm = '221'
//           |  union all
//           |-- 5 线路售电量变电站考核表反向退补电量部分
//           |  select
//           |    e.gddwbm,e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs,e.yddz,e.tqbs,f.zcbh,e.cbqdbh,
//           |    e.yhlbdm,coalesce(e.ygtbdl, 0) ygzdl,e.jfrl,f.cbsxh,null cbrbs,f.cbsj,e.jldbh,f.scbss,f.bcbss,
//           |    f.zhbl,e.ygbsdl,f.sjcbfsdm,'5' dllx
//           |  from jldxx e, cbxx f
//           |  where e.dfny = ${nowMonth}
//           |      and e.yhlbdm = '80' and f.yhbh = e.yhbh
//           |      and f.gzdbh = e.gzdbh and f.dfny = e.dfny and f.jldbh = e.jldbh
//           |      and f.sslxdm = '221'
//           |  union all
//           |-- 6 线路售电量专变公用的扣减电量部分
//           |  select
//           |    e.gddwbm, e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs,e.yddz, e.tqbs,null zcbh,
//           |    e.cbqdbh, e.yhlbdm, e.ygfbkjdl ygzdl,e.jfrl,null cbsxh,null cbrbs,null cbsj,e.jldbh,
//           |    null scbss,null bcbss,null zhbl,e.ygbsdl,null sjcbfsdm,'6' dllx
//           |  from jldxx e, xszbgyyh f
//           |  where e.dfny = ${nowMonth}
//           |      and e.yhlbdm = '10'
//           |      and e.xlxdbs = f.xlxdbs and e.yhbh = f.yhbh
//           |      and not exists (select 1 from hs_jldgx g where f.yhbh = g.yhbh and g.yhbh = g.glyhbh and g.dfny = e.dfny)
//           |  union all
//           |-- 7 线路售电量专变用户非周期部分
//           |  select
//           |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,null zcbh,cbqdbh,yhlbdm,
//           |    (coalesce(jfdl, 0) + coalesce(mfdl, 0)) ygzdl,jfrl,null cbsxh,null cbrbs,null cbsj,
//           |    t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'7' dllx
//           |  from jldxx t
//           |  where exists (
//           |                select 1
//           |                from xsfzqjfdl e --准备专变 非周期数据
//           |                join (
//           |                  select
//           |                    a.cbqdbh,case when a.cblr < 10 then concat('0',a.cblr) else a.cblr end cblr
//           |                  from (
//           |                    select q.cbqdbh,max(q.cblr) cblr
//           |                    from cbqdfz q --抄表区段辅助
//           |                    group by q.cbqdbh) a) f
//           |                on f.cbqdbh = e.cbqdbh
//           |                where e.gzdbh = t.gzdbh and e.ywlbdm = t.ywlbdm and e.jldbh = t.jldbh
//           |                      and getDay(e.cbrq) <= concat('202005',f.cblr)
//           |                      and getDay(e.cbrq) > concat('${nowMonth}',f.cblr)
//           |              )
//           |  union all
//           |-- 8 线路售电量台区考核户反向部分
//           |  select
//           |    e.gddwbm, e.dfny, e.yhbh, e.yhmc, e.jldcbsxh, e.bqcbcs,e.xlxdbs, e.yddz,e.tqbs,f.zcbh,e.cbqdbh, e.yhlbdm,
//           |    -coalesce(f.bjdl,0) ygzdl,e.jfrl,f.cbsxh,null cbrbs,
//           |    f.cbsj,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'8' dllx
//           |  from jldxx e, cbxx f
//           |  where e.dfny = ${nowMonth}
//           |        and e.yhlbdm = '60' and f.yhbh = e.yhbh
//           |        and f.gzdbh = e.gzdbh and e.ywlbdm = f.ywlbdm and f.dfny = e.dfny and f.jldbh = e.jldbh
//           |        and f.sslxdm = '221'
//           |  union all
//           |-- 9 线路售电量台区考核表上月二次抄表部分
//           |  select
//           |    gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |    null zcbh,cbqdbh,yhlbdm,(coalesce(ygzdl, 0) - coalesce(ygbsdl, 0)) ygzdl,jfrl,
//           |    null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'9' dllx
//           |  from jldxx t
//           |  where t.dfny = ${lastMonth} and t.yhlbdm = '60' and t.bqcbcs = 2
//           | ) a
//           |order by a.dllx
//           |
//             """.stripMargin

        // 2020/10/23 ysdfjl 应收电费记录读不出来
//        sparkSession.sql(xlsdlmx).createOrReplaceTempView("xlsdlmx") //线路售电量明细
//        sparkSession.sql("select * from xlsdlmx limit 5").show()
        println(" 3 线路售电量明细")



        /*-------------------------------------------------------------------*/
        //台区售电量明细
        // 1台区售电量本月公变部分
        // 2台区售电量单、双月公变上月部分
        // 3台区售电量本月违约窃电部分
        // 4台区售电量单、双月台区上月违约窃电部分
        // 5台区售电量非周期本月一次部分
        // 6台区售电量单、双月非周期上月一次部分
        // 7台区售电量单、双月非周期上上月二次部分
        // 8台区售电量台区考核表户反向电量部分
        // 9台区售电量台单、双月区考核表户上月反向电量部分
        // 10台区售电量非周期上月二次部分
//        val tqsdlmx =
//        s"""
//           |select * from (
//           |-- 1台区售电量本月公变部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'1' dllx
//           |    from jldxx t
//           |    where t.dfny = ${nowMonth} and (t.cbjhbh not like 'f%' or t.cbjhbh is null)
//           |         and t.djdm <> '00000000' and t.YHLBDM in('20','98')
//           |    union all
//           |-- 2台区售电量单、双月公变上月部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'2' dllx
//           |    from jldxx t
//           |    where t.dfny = ${lastMonth} and (t.cbjhbh not like 'f%' or t.cbjhbh is null)
//           |        and t.djdm <> '00000000' and t.yhlbdm in('20','98')
//           |        and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |-- 3台区售电量本月违约窃电部分
//           |    select
//           |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,null jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
//           |        null zcbh,t.cbqdbh,t.yhlbdm,coalesce(t.jfdl, 0) ygzdl,null jfrl,
//           |        null cbsxh,null cbrbs,t.czsj,t.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'3' dllx
//           |    from ysdfjl t --应收电费记录
//           |    where t.ywlbdm = '0500' and t.djdm not in ('00000000', '39002101')
//           |         and t.dfny = ${nowMonth}
//           |    union all
//           |-- 4台区售电量单、双月台区上月违约窃电部分
//           |    select
//           |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,null jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
//           |        null zcbh,t.cbqdbh,t.yhlbdm,coalesce(t.jfdl, 0) ygzdl,null jfrl,
//           |        null cbsxh,null cbrbs,t.czsj,t.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'4' dllx
//           |    from ysdfjl t
//           |    where t.ywlbdm = '0500' and t.djdm not in ('00000000', '39002101')
//           |         and t.dfny = ${lastMonth}
//           |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |-- 5台区售电量非周期本月一次部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'5' dllx
//           |    from jldxx t
//           |    where t.dfny = ${nowMonth} and t.cbjhbh like 'f%' and t.bqcbcs = 1
//           |         and t.djdm <> '00000000' and t.yhlbdm = '20'
//           |    union all
//           |-- 6台区售电量单、双月非周期上月一次部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'6' dllx
//           |    from jldxx t
//           |    where t.dfny = ${lastMonth} and t.cbjhbh like 'f%' and t.bqcbcs = 1
//           |         and t.djdm <> '00000000' and t.yhlbdm = '20'
//           |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |-- 7台区售电量单、双月非周期上上月二次部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'7' dllx
//           |    from jldxx t
//           |    where t.dfny = ${lastMonth2} and t.cbjhbh like 'f%' and t.bqcbcs = 2
//           |         and t.djdm <> '00000000' and t.yhlbdm = '20'
//           |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |-- 8台区售电量台区考核表户反向电量部分
//           |    select
//           |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
//           |        f.zcbh,t.cbqdbh,t.yhlbdm,(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,
//           |        null cbrbs,f.cbsj,t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm,'8' dllx
//           |    from jldxx t, _cbxx f
//           |    where t.dfny =  ${nowMonth}
//           |         and t.yhlbdm = '60'
//           |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
//           |         and f.sslxdm = '221'
//           |    union all
//           |-- 9台区售电量台单、双月区考核表户上月反向电量部分
//           |    select
//           |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
//           |        f.zcbh,t.cbqdbh,t.yhlbdm,(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,
//           |        null cbrbs,f.cbsj,t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm,'9' dllx
//           |    from jldxx t, cbxx f
//           |    where t.dfny =  ${lastMonth}
//           |         and t.yhlbdm = '60'
//           |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
//           |         and f.sslxdm = '221'
//           |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |    -- 10台区售电量非周期上月二次部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(jfdl,0)+coalesce(mfdl,0)) as ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,t.jldbh,null scbss,null bcbss,null zhbl,t.ygbsdl,null sjcbfsdm,'10' dllx
//           |    from jldxx t
//           |    where t.dfny = ${lastMonth} and t.cbjhbh like 'f%' and t.bqcbcs = 2
//           |         and t.djdm <> '00000000' and t.yhlbdm = '20'
//           |) a
//           |order by a.dllx
//           |
//             """.stripMargin

        // 2020/10/23 ysdfjl 应收电费记录读不出来
//        sparkSession.sql(tqsdlmx).createOrReplaceTempView("tqsdlmx") //台区售电量明细
//        sparkSession.sql("select * from tqsdlmx limit 5").show()
        println("4 台区售电量明细")
        /*-------------------------------------------------------------------*/

        // 线路供电量增加地方电厂上网电量 取根据 计量点 从 量测抄表信息 获取电量
        // 线路地方电厂上网电量
        val xldfdcswdlNew =
        s"""
           |select sum(coalesce(c.bjdl, 0) + coalesce(c.jbdl, 0)) bjdl,
           |     c.jldbh,
           |     'bffg' fgbz,
           |     c.yhbh,
           |     e.xlxdbs,
           |     c.dfny ny,
           |     c.dqbm,
           |     c.bqcbcs,
           |     c.sslxdm
           |from cbxx c, jld e
           |where c.dfny = ${nowMonth}
           | and c.yhbh = e.yhbh
           | and c.yhbh = e.yhbh
           | and c.jldbh = e.jldbh
           | and e.jldytdm <> '410'
           | and c.sslxdm in ('121', '221') --正向有功总 反向有功总
           | and e.jldydjdm not in ('02', '03') --排除电压等级为 交流220v、交流为380v
           |  and exists (select 1
           |    from gdljfhzxx a, dcfzxx b --购电量价费汇总信息表 电厂辅助信息表
           |   where c.yhbh = a.dcbh
           |     and c.dfny = a.gdyf
           |     and b.dcbh = a.dcbh)
           |group by e.xlxdbs, c.jldbh, c.yhbh, c.dfny, c.dqbm, c.bqcbcs,c.sslxdm
           |union all
           |select sum(coalesce(c.bjdl, 0) + coalesce(c.jbdl, 0)) bjdl,
           |     c.jldbh,
           |     'ffg' fgbz,
           |     c.yhbh,
           |     e.xlxdbs,
           |     c.dfny ny,
           |     c.dqbm,
           |     c.bqcbcs,
           |     c.sslxdm
           |from cbxx c, jld e
           |where c.dfny = ${nowMonth}
           | and c.yhbh = e.yhbh
           | and c.yhbh = e.yhbh
           | and c.jldbh = e.jldbh
           | and e.jldytdm <> '410'
           | and c.sslxdm in ('123', '124', '125', '222', '223', '224') --正向和反向有功峰平谷
           | and e.jldydjdm not in ('02', '03') --排除电压等级为 交流220v、交流为380v
           |  and exists (select 1
           |    from gdljfhzxx a, dcfzxx b
           |   where c.yhbh = a.dcbh
           |     and c.dfny = a.gdyf
           |     and b.dcbh = a.dcbh)
           |group by e.xlxdbs, c.jldbh, c.yhbh, c.dfny, c.dqbm, c.bqcbcs,c.sslxdm
           |
             """.stripMargin

        sparkSession.sql(xldfdcswdlNew).createOrReplaceTempView("xldfdcswdlNew") // TODO:线路地方电厂上网电量
//        sparkSession.sql("select * from xldfdcswdlNew limit 5").show()
        println(" 5 线路地方电厂上网电量")  // 2020/10/26 这张表输出没数据

        /*-------------------------------------------------------------------*/


        //线路供电量明细
        // 1线路供电量线路总表部分
        // 2线路供电量线路总表退补电量部分
        // 3线路供电量地方电厂退补电量部分
        // 4线路供电量地方电厂上网电量本月一次部分
        // 5线路供电量光伏反向上网电量部分
        // 6线路供电量地方电厂上网电量上月二次部分
        // 7线路供电量地方电厂上网电量部分
//        val xlgdlmx =
//        s"""
//           |select * from (
//           |-- 1线路供电量线路总表部分
//           |  select
//           |    f.gddwbm,e.dfny, e.yhbh, e.yhmc, e.jldxh, e.xlxdbs,e.yddz,f.zcbh,e.cbqdbh,e.yhlbdm,
//           |     (coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0) - coalesce(e.ygfbkjdl, 0)) ygzdl,e.jfrl,f.cbsxh,f.cbrbs,e.jldbh,f.scbss,
//           |     f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'1' dllx
//           |  from jldxx e, cbxx f
//           |  where e.dfny = ${nowMonth}
//           |     and e.yhlbdm = '80' and f.yhbh = e.yhbh
//           |     and f.gzdbh = e.gzdbh and f.dfny = e.dfny and e.ywlbdm = f.ywlbdm and f.jldbh = e.jldbh and f.dqbm = e.dqbm and f.sslxdm = '121'
//           |  union all
//           |
//           |-- 2线路供电量线路总表退补电量部分
//           |  select
//           |    e.gddwbm,e.dfny, e.yhbh, e.yhmc, e.jldxh, e.xlxdbs,e.yddz,f.zcbh,e.cbqdbh, e.yhlbdm,coalesce(e.ygtbdl, 0) ygzdl,e.jfrl,
//           |    f.cbsxh,f.cbrbs,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'2' dllx
//           |  from jldxx e, cbxx f
//           |  where e.dfny = ${nowMonth}
//           |      and e.yhlbdm = '80' and f.yhbh = e.yhbh
//           |      and f.gzdbh = e.gzdbh and f.dfny = e.dfny and f.jldbh = e.jldbh and f.sslxdm = '121'
//           |  union all
//           |
//           |-- 3线路供电量地方电厂退补电量部分
//           |  select
//           |    c.gddwbm,c.ccyf dfny,c.dcbh yhbh,c.dcmc yhmc,0 jldxh, e.xlxdbs, null yddz, null zcbh, null cbqdbh, '40' yhlbdm,
//           |    coalesce(c.tbdl, 0) ygzdl,0 jfrl,null cbsxh,null cbrbs,d.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'3' dllx
//           |  from gdjycctbxx c,dcjzxx d,jld e  --2020-10-21 购电交易差错退补信息 电厂机组信息 计量点（有笛卡尔积）
//           |  where c.ccyf = ' || v_dcqsny || '
//           |     and c.jsdybh = d.jsdybh and d.jldbh = e.jldbh
//           |     and c.tbzt = '1' and exists (select 1 from dcfzxx b where b.dcbh = c.dcbh
//           |     and b.dydj not in ('02', '03'))
//           |  union all
//           |
//           |-- 4线路供电量地方电厂上网电量本月一次部分
//           |  select g.gddwbm, g.dfny, g.yhbh, g.yhmc,
//           |      1 jldxh,c.xlxdbs,g.yddz,g.zcbh,g.cbqdbh,'40' yhlbdm,c.bjdl ygzdl,
//           |      null jfrl,g.cbsxh,g.cbrbs,null jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'4' dllx
//           |  from xldfdcswdlNew c,cbxx g  --线路地方电厂上网电量 抄表信息
//           |  where c.jldbh = g.jldbh and c.yhbh = g.yhbh
//           |      and c.ny = g.dfny and c.sslxdm = g.sslxdm and c.bqcbcs = g.bqcbcs
//           |      and c.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = c.yhbh) = '0' then 'BFFG' else 'FFG' end)
//           |      and not exists (select 1 from dcfzxx d where c.yhbh = d.dcbh and d.gdldz = '2')
//           |      and c.ny = ' || v_dcqsny || ' and c.bqcbcs = 1
//           |      and exists(select 1 from _xldfdcswdlNew d where d.fgbz = c.fgbz and d.ny = ' || v_dcqssy || '
//           |      and d.bqcbcs = 2 and d.xlxdbs = c.xlxdbs)
//           |  union all
//           |-- 5线路供电量光伏反向上网电量部分
//           |  select
//           |    f.gddwbm,e.dfny,e.yhbh,e.yhmc,e.jldxh,e.xlxdbs,e.yddz,f.zcbh,e.cbqdbh,e.yhlbdm,
//           |      coalesce(f.bjdl, 0) ygzdl,e.jfrl,f.cbsxh,f.cbrbs,e.jldbh,f.scbss,f.bcbss,f.zhbl,e.ygbsdl,f.sjcbfsdm,'5' dllx
//           |  from jldxx e, cbxx f
//           |  where e.dfny = ${nowMonth}
//           |      and e.yhlbdm = '60' and f.yhbh = e.yhbh and f.gzdbh = e.gzdbh and f.dfny = e.dfny
//           |      and e.ywlbdm = f.ywlbdm and f.jldbh = e.jldbh and f.sslxdm = '221'
//           |      and exists (select 1 from gdljfhzxx a ,dcfzxx b,yhdyxx d where d.yhbh = a.dcbh
//           |      and d.xlxdbs = e.xlxdbs and a.gdyf = ${nowMonth} and b.dcbh = a.dcbh
//           |      and b.dydj in('02','03'))
//           |  union all
//           |-- 6线路供电量地方电厂上网电量上月二次部分
//           |  select
//           |    g.gddwbm, g.dfny, g.yhbh, g.yhmc,
//           |    1 jldxh,c.xlxdbs,g.yddz,g.zcbh,g.cbqdbh,'40' yhlbdm,c.bjdl ygzdl,
//           |    null jfrl,g.cbsxh,g.cbrbs,null jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'6' dllx
//           |  from xldfdcswdlNew c,cbxx g  --2020-10-21 线路地方电厂上网电量 抄表信息
//           |  where c.jldbh = g.jldbh and c.yhbh = g.yhbh
//           |      and c.ny = g.dfny and c.sslxdm = g.sslxdm and c.bqcbcs = g.bqcbcs
//           |      and c.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = c.yhbh) = '0' then 'BFFG' else 'FFG' end)
//           |      and not exists (select 1 from dcfzxx d where c.yhbh = d.dcbh and d.gdldz = '2')
//           |      and  c.ny = ' || v_dcqsny || ' and c.bqcbcs = 2
//           |  union all
//           |-- 7线路供电量地方电厂上网电量部分
//           |  select
//           |    g.gddwbm, g.dfny, g.yhbh, g.yhmc,
//           |    1 jldxh,c.xlxdbs,g.yddz,g.zcbh,g.cbqdbh,'40' yhlbdm,c.bjdl ygzdl,
//           |    null jfrl,g.cbsxh,g.cbrbs,null jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'7' dllx
//           |  from xldfdcswdlNew c,cbxx g --2020-10-21 线路地方电厂上网电量 抄表信息
//           |  where c.jldbh = g.jldbh and c.yhbh = g.yhbh
//           |      and c.ny = g.dfny and c.sslxdm = g.sslxdm and c.bqcbcs = g.bqcbcs
//           |      and c.fgbz = (case when (select max(e.fgbs) from jsdy e where e.gsgx = '2' and e.dcbh = c.yhbh) = '0' then 'BFFG' else 'FFG' end)
//           |      and not exists (select 1 from dcfzxx d where c.yhbh = d.dcbh and d.gdldz = '2')
//           |      and  c.ny = ' || v_dcqsny || '
//           |      and not exists(select 1 from xldfdcswdlNew d where d.fgbz = c.fgbz and d.ny = ' || v_dcqsny || ' and d.bqcbcs = 2
//           |      and d.xlxdbs = c.xlxdbs)
//           |) a
//           |order by a.dllx
//           |
//             """.stripMargin

        // 2020/10/26 结算单元表jsdy 不存在
//        sparkSession.sql(xlgdlmx).createOrReplaceTempView("xlgdlmx") // TODO: 线路供电量明细
//        sparkSession.sql("select * from xlgdlmx limit 5").show()
        println(" 6 线路供电量明细")

        /*-------------------------------------------------------------------*/
        //台区供电量明细
        // 1台区供电量台区考核表当月部分
        // 2单、双月台区供电量台区考核表上月部分
        // 3台区供电量光伏上网上月部分
        // 4单、双月台区供电量光伏上网上上月部分
        // 5台区供电量反向电量扣减部分
        // 6单、双月台区供电量反向电量扣减部分
        // 7台区供电量光伏上网退补上月部分
        // 8单、双月台区供电量光伏上网退补上上月部分
//        val tqgdlmx =
//        s"""
//           |select *
//           |from (
//           |-- 1台区供电量台区考核表当月部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(e.ygzdl, 0) - coalesce(e.ygbsdl, 0)) ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,e.ygbsdl,null sjcbfsdm,'1' dllx
//           |    from jldxx e
//           |    where e.dfny = ${nowMonth} and e.yhlbdm = '60'
//           |    union all
//           |-- 2单、双月台区供电量台区考核表上月部分
//           |    select
//           |        gddwbm,dfny,yhbh,yhmc,jldcbsxh,bqcbcs,xlxdbs,yddz,tqbs,
//           |        null zcbh,cbqdbh,yhlbdm,(coalesce(e.ygzdl, 0) - coalesce(e.ygbsdl, 0)) ygzdl,jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,e.ygbsdl,null sjcbfsdm,'2' dllx
//           |    from jldxx e
//           |    where e.dfny = ${lastMonth} and e.yhlbdm = '60'
//           |    and exists (select 1 from all_tq a where e.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |-- 3台区供电量光伏上网上月部分
//           |    select
//           |        max(c.gddwbm),c.dfny,c.yhbh,max(c.yhmc),max(c.jldcbsxh),max(c.bqcbcs),d.xlxdbs,max(c.yddz),d.tqbs,max(c.zcbh),max(c.cbqdbh),max(c.yhlbdm),
//           |        sum(coalesce(c.bjdl,0) + coalesce(c.jbdl,0)) ygzdl,null jfrl,
//           |        max(c.cbsxh),max(c.cbrbs),max(c.cbsj),c.jldbh,max(c.scbss),max(c.bcbss),max(c.zhbl),null ygbsdl,max(c.sjcbfsdm),'3' dllx
//           |    from cbxx c, yhdyxx d, jld e --2020-10-21 抄表信息 用户电源信息 计量点
//           |    where c.dfny = ' || v_dcqsny ||' and c.yhbh = d.yhbh
//           |    and d.xlxdbs = e.xlxdbs and c.yhbh = e.yhbh and c.jldbh = e.jldbh and c.yhlbdm = '40' and c.sslxdm = '221'
//           |    and exists (select 1 from gdljfhzxx a, dcfzxx b
//           |            where c.yhbh = a.dcbh and c.dfny = a.gdyf and b.dcbh = a.dcbh
//           |                and b.dydj in ('02', '03')
//           |                )
//           |    group by d.xlxdbs, d.tqbs,c.jldbh,c.yhbh,c.dfny,c.dqbm
//           |    union all
//           |-- 4单、双月台区供电量光伏上网上上月部分
//           |    select
//           |        max(c.gddwbm),c.dfny,c.yhbh,max(c.yhmc),max(c.jldcbsxh),max(c.bqcbcs),d.xlxdbs,
//           |        max(c.yddz),d.tqbs,max(c.zcbh),max(c.cbqdbh),max(c.yhlbdm),
//           |        sum(coalesce(c.bjdl,0) + coalesce(c.jbdl,0)) ygzdl,null jfrl,
//           |        max(c.cbsxh),max(c.cbrbs),max(c.cbsj),c.jldbh,max(c.scbss),max(c.bcbss),max(c.zhbl),null ygbsdl,max(c.sjcbfsdm),'4' dllx
//           |    from cbxx c, yhdyxx d, jld e --2020-10-21 抄表信息 用户电源信息 计量点
//           |    where c.dfny = ' || v_dcqssy ||' and c.yhbh = d.yhbh and d.xlxdbs = e.xlxdbs
//           |        and c.yhbh = e.yhbh and c.jldbh = e.jldbh and c.yhlbdm = '40' and c.sslxdm = '221' and e.jldydjdm in ('02', '03')
//           |        and exists (select 1 from gdljfhzxx a, dcfzxx b where c.yhbh = a.dcbh and c.dfny = a.gdyf and b.dcbh = a.dcbh)
//           |        and exists (select 1 from all_tq a where d.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    group by d.xlxdbs, d.tqbs,c.jldbh,c.yhbh,c.dfny,c.dqbm
//           |    union all
//           |-- 5台区供电量反向电量扣减部分
//           |    select
//           |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
//           |        f.zcbh,t.cbqdbh,t.yhlbdm,-(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,null cbrbs,f.cbsj,
//           |        t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm ,'5' dllx
//           |    from jldxx t, cbxx f
//           |    where t.dfny =  ${nowMonth}
//           |         and t.yhlbdm = '60'
//           |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
//           |         and f.sslxdm = '221'
//           |    union all
//           |    -- 6单、双月台区供电量反向电量扣减部分
//           |    select
//           |        t.gddwbm,t.dfny,t.yhbh,t.yhmc,t.jldcbsxh,t.bqcbcs,t.xlxdbs,t.yddz,t.tqbs,
//           |        f.zcbh,t.cbqdbh,t.yhlbdm,-(coalesce(f.bjdl, 0) + coalesce(f.jbdl, 0)) as ygzdl,t.jfrl,f.cbsxh,null cbrbs,f.cbsj,
//           |        t.jldbh,f.scbss,f.bcbss,f.zhbl,t.ygbsdl,f.sjcbfsdm,'6' dllx
//           |    from jldxx t, cbxx f
//           |    where t.dfny =  ${lastMonth}
//           |         and t.yhlbdm = '60'
//           |         and f.yhbh = t.yhbh and f.gzdbh = t.gzdbh and f.dfny = t.dfny and t.ywlbdm = f.ywlbdm and f.jldbh = t.jldbh
//           |         and f.sslxdm = '221'
//           |         and exists (select 1 from all_tq a where t.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |    union all
//           |    -- 7台区供电量光伏上网退补上月部分
//           |    select
//           |        c.gddwbm,c.ccyf dfny,c.dcbh yhbh,c.dcmc yhmc,0 jldcbsxh,null bqcbcs, e.xlxdbs,null yddz,null tqbs,
//           |        null zcbh,null cbqdbh, '40' yhlbdm,coalesce(c.tbdl, 0) ygzdl,0 jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'7' dllx
//           |    from gdjycctbxx c,dcjzxx d,jld e --购电交易差错退补信息 电厂机组信息 计量点
//           |    where c.ccyf = ' || v_dcqsny || '
//           |         and c.jsdybh = d.jsdybh and d.jldbh = e.jldbh
//           |         and c.tbzt = '1' and exists (select 1 from dcfzxx b where b.dcbh = c.dcbh
//           |         and e.jldydjdm in ('02', '03'))
//           |    union all
//           |    -- 8单、双月台区供电量光伏上网退补上上月部分
//           |    select
//           |        c.gddwbm,c.ccyf dfny,c.dcbh yhbh,c.dcmc yhmc,0 jldcbsxh,null bqcbcs, e.xlxdbs,null yddz,null tqbs,
//           |        null zcbh,null cbqdbh, '40' yhlbdm,coalesce(c.tbdl, 0) ygzdl,0 jfrl,
//           |        null cbsxh,null cbrbs,null cbsj,e.jldbh,null scbss,null bcbss,null zhbl,null ygbsdl,null sjcbfsdm,'8' dllx
//           |    from gdjycctbxx c,dcjzxx d,jld e --购电交易差错退补信息 电厂机组信息 计量点
//           |    where c.ccyf = ' || v_dcqssy || '
//           |         and c.jsdybh = d.jsdybh and d.jldbh = e.jldbh
//           |         and c.tbzt = '1' and exists (select 1 from dcfzxx b where b.dcbh = c.dcbh
//           |         and e.jldydjdm in ('02', '03'))
//           |         and exists (select 1 from all_tq a where e.tqbs = a.tqbs and a.cbzq in ('2','3'))
//           |
//           |    ) a
//           |order by a.dllx
//           |
//             """.stripMargin
//        sparkSession.sql(tqgdlmx).repartition(200).createOrReplaceTempView("tqgdlmx") // TODO: 台区供电量明细
//        sparkSession.sql("select * from tqgdlmx limit 5").show()
        println(" 7 台区供电量明细")
        // 2020/10/26 报错：cannot complete before timeout: ScanRequest
        // 2020/10/26 肯定单个子任有问题，暂时不排查

        end=System.currentTimeMillis()

        /*-------------------------------------------------------------------*/



    }


    /**
    * @Description: 异常线路和台区带来的额外段落
    * @Param: [sparkSession, url]
    * @return: void
    * @Date: 2020/10/16
    */
    def extraSection(sparkSession:SparkSession,url:String)={
    //1 线路线损统计信息  xlbh-线路编号,xlmc-线路名称,xlxdbs-线路线段标识,ny-年月,byxsl-本月线损率,
    //                  bygdl-本月供电量,bysdl-本月售电量,gddwbm-供电单位编码,bdzbh-变电站编号,
    //                  bdzmc-变电站名称,bdzbs-变电站标识,khzb-考核指标,khzbxx-考核指标下限
        println("异常线路和台区带来的额外段落")
        start =System.currentTimeMillis()

    val npmis_gk_xlxstjxx = sparkSession.read
      .options(Map("kudu.master" -> url,"kudu.table" -> "csg_ods_yx.gk_xlxstjxx"))
      .format("org.apache.kudu.spark.kudu")
      .load()

    npmis_gk_xlxstjxx.createOrReplaceTempView("xlxstjxx")
    val xlxstjxx =sparkSession.sql(
        s"""
           |select
           |    xlbh,xlmc,xlxdbs,ny,byxsl,bygdl,bysdl,gddwbm,bdzbh,bdzmc,
           |    bdzbs,khzb,khzbxx,xszrrbs,dqbm,sqxsl,ssqxsl,byxsl,ny
           |from xlxstjxx
           |where dqbm in (${cityCodeList})
           |
             """.stripMargin)
    xlxstjxx.createOrReplaceTempView("xlxstjxx") //线路线损统计信息
        try {
//            xlxstjxx.show(5)
        }catch {
            case e:Exception => e.printStackTrace()
        }
        println(" 1 线路线损统计信息" )

    /*-------------------------------------------------------------------*/

        //2 台区线损统计信息
        // tqbs-台区标识,tqbh-台区编号,tqmc-台区名称,bygdl-本月供电量,bysdl-本月售电量,gddwbm-供电单位编码,bdzbs-变电站标识
        val npmis_gk_tqxstjxx =sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "csg_ods_yx.gk_tqxstjxx"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        npmis_gk_tqxstjxx.createOrReplaceTempView("tqxstjxx")
        val tqxstjxx =sparkSession.sql(
            s"""
               |select
               |    tqbs,tqbh,tqmc,bygdl,bysdl,ny,gddwbm,bdzbs,xlxdbs,xlbh,xlmc,tqbs,tqbh,tqmc,
               |    xszrrbs,dqbm,sqxsl,ssqxsl,byxsl,khzb,khzbxx,ny
               |from tqxstjxx --台区线损统计信息
               |where dqbm in (${cityCodeList})
               |
                 """.stripMargin)
        tqxstjxx.createOrReplaceTempView("tqxstjxx") //台区线损统计信息
        try {
//            tqxstjxx.show(5)
        }catch {
            case e:Exception => e.printStackTrace()
        }
        println(" 2 台区线损统计信息" )
    /*-------------------------------------------------------------------*/

    //线损异常(线路和台区)明细  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,xltqbz-线路台区标识(1-线路,2-台区),
    //                      gddwbm-供电单位编码,ny-年月,byxsl-本月线损率,bygdl-本月供电量,bysdl-本月售电量
    //                      tqbs-台区标识,tqbh-台区编号,tqmc-台区名称
    // 自己汇总  缺乏sfxf,bz 字段
    sparkSession.sql(
        s"""
           |select
           |    distinct xl.ny,xl.gddwbm,substr(xl.gddwbm,1,length(xl.gddwbm)-2) sjdwbm,
           |    substr(xl.gddwbm,1,length(xl.gddwbm)-4) ssjdwbm,${xlbz} xltqbz,xl.xlxdbs,
           |    l.xlbh xlbh,l.xlmc xlmc,null tqbs,null tqbh,null tqmc,xl.bygdl,xl.bysdl,xl.byxsl,
           |    xl.xszrrbs,r.rymc xszrr,bz.xlbzbh zhxx,null sfxf,xl.dqbm,null bz,xl.khzbxx,xl.khzb,
           |    xl.sqxsl,xl.ssqxsl,xl.bdzbs,getbdzbh(xl.xlxdbs) bdzbh,getbdzmc(xl.xlxdbs) bdzmc
           |from xlxstjxx xl
           |left join all_xlxd l on xl.xlxdbs = l.xlxdbs
           |left join xlbzxx bz on xl.xlxdbs = bz.xlxdbs and bz.bzny = xl.ny
           |left join ry r on r.rybs = xl.xszrrbs
           |where (xl.byxsl > xl.khzb or xl.byxsl < xl.khzbxx) and (xl.bygdl > 30000 or xl.bysdl > 30000)
           |union all
           |select
           |    distinct tq.ny,tq.gddwbm,substr(tq.gddwbm,1,length(tq.gddwbm)-2) sjdwbm,
           |    substr(tq.gddwbm,1,length(tq.gddwbm)-4) ssjdwbm,${tqbz} xltqbz,tq.xlxdbs,l.xlbh xlbh,
           |    l.xlmc xlmc,tq.tqbs tqbs,tq.tqbh tqbh,tq.tqmc tqmc,tq.bygdl,tq.bysdl,tq.byxsl,tq.xszrrbs,
           |    r.rymc xszrr,bz.tqbzbh zhxx,null sfxf,tq.dqbm,null bz,tq.khzbxx,tq.khzb,tq.sqxsl,
           |    tq.ssqxsl,tq.bdzbs,getbdzbh(tq.xlxdbs) bdzbh,getbdzmc(tq.xlxdbs) bdzmc
           |from tqxstjxx tq
           |left join all_xlxd l on tq.xlxdbs = l.xlxdbs
           |left join tqbzxx bz on tq.tqbs = bz.tqbs and bz.bzny = tq.ny
           |left join ry r on r.rybs = tq.xszrrbs
           |where (tq.byxsl > tq.khzb or tq.byxsl < tq.khzbxx) and (tq.bygdl > 10000 or tq.bysdl > 10000)
            """.stripMargin).createOrReplaceTempView("xsycxlhtqmx")//线损异常(线路和台区)明细

        try {
//            sparkSession.sql("select * from xsycxlhtqmx limit 5").show()
        }catch {
            case e:Exception => e.printStackTrace()
        }
        println(" 3 线损异常(线路和台区)明细")


    /*-------------------------------------------------------------------*/

    //4  异常线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
    val xlxd1 =sparkSession.read
      .options(Map("kudu.master" -> url,"kudu.table" -> "csg_ods_yx.dw_xlxd"))
      .format("org.apache.kudu.spark.kudu")
      .load()

    xlxd1.createOrReplaceTempView("xlxd")

    val xlxd =sparkSession.sql(
        s"""
           |select
           |    l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm,y.gddwbm
           |from xlxd l  --与all_xlxd join 也一样
           |join xsycxlhtqmx y on l.xlxdbs = y.xlxdbs
           |where y.ny = ${nowMonth} and y.xltqbz = ${xlbz}
           |
             """.stripMargin)
        xlxd.createOrReplaceTempView("xlxd") //异常线路线段
        try {
//            xlxd.show(5)
        }catch {
            case e:Exception => e.printStackTrace()
        }

        println(" 4 异常线路线段")

    /*-------------------------------------------------------------------*/

        //5 异常台区  tqbs-台区标识,tqbh-台区编号,tqmc-台区名称
        val tq1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "csg_ods_yx.dw_tq"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        tq1.createOrReplaceTempView("tq")

        //与异常的信息join，拿到异常的拿到异常的台区
        val tq =sparkSession.sql(
            s"""
               |select
               |    t.tqbs,t.tqbh,t.tqmc,t.yxztdm,t.gddwbm
               |from tq t
               |join xsycxlhtqmx y on t.tqbs = y.tqbs
               |where t.gddwbm is not null and y.ny = ${nowMonth} and y.xltqbz = ${tqbz}
               |
                 """.stripMargin)
        tq.createOrReplaceTempView("tq") //异常台区
        try {
//            tq.show(5)
        }catch {
            case e:Exception => e.printStackTrace()
        }
        println(" 5 异常台区")

        /*-------------------------------------------------------------------*/
        //2020.04.07添加
        //根据线路线段表示获取上期线损率   xlxdbs 线路线段标识    bqxsl 线损率
        val xlxdSqxsl = sparkSession.sql(
            s"""
               |select
               |    xlxdbs,cast(byxsl as string) bqxsl
               |from xlxstjxx --线路线损统计信息
               |where ny=${lastMonth}
               |
               |""".stripMargin)

        val xlxdSqxslList= xlxdSqxsl.collect()
        val xlxdSqxslMap = Map[String, String]()
        for (xsl <- xlxdSqxslList) {
            xlxdSqxslMap.put(xsl.getAs[String]("xlxdbs"), xsl.getAs[String]("bqxsl"))
        }

        //广播变量
        val xlxdSqxslBroadcast= sparkSession.sparkContext.broadcast(xlxdSqxslMap)

        //udf-获取线段的线损率
        sparkSession.udf.register("getXlxdSqXsl", (xlxdbs: String)  => {
            xlxdSqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })

        //根据线路线段表示获取上上期线损率
        val xlxdSsqxsl = sparkSession.sql(s"select xlxdbs,cast(byxsl as string) bqxsl from xlxstjxx where ny=${lastMonth2}")

        val xlxdSsqxslList= xlxdSsqxsl.collect()
        val xlxdSsqxslMap = Map[String, String]()
        for (xsl <- xlxdSsqxslList) {
            xlxdSsqxslMap.put(xsl.getAs[String]("xlxdbs"), xsl.getAs[String]("bqxsl"))
        }
        val xlxdSsqxslBroadcast= sparkSession.sparkContext.broadcast(xlxdSsqxslMap)
        sparkSession.udf.register("getXlxdSsqXsl", (xlxdbs: String)=> {
            xlxdSsqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })
        println("6")
        /*-------------------------------------------------------------------*/

        //根据台区标识获取上期线损率
        val tqSqxsl = sparkSession.sql(
            s"""
               |select
               |    tqbs,cast(byxsl as string) bqxsl
               |from tqxstjxx where ny=${lastMonth}
                 """.stripMargin)

        val tqSqxslList= tqSqxsl.collect()
        val tqSqxslMap = scala.collection.mutable.Map[String, String]()
        for (xsl <- tqSqxslList) {
            tqSqxslMap.put(xsl.getAs[String]("tqbs"), xsl.getAs[String]("bqxsl"))
        }

        //广播变量
        val tqSqxslBroadcast= sparkSession.sparkContext.broadcast(tqSqxslMap)
        //udf-获取标识
        sparkSession.udf.register("getTqSqXsl", (xlxdbs: String) => {
            tqSqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })
        println("7")

        //根据台区标识获取上上期线损率
        val tqSsqxsl = sparkSession.sql(
            s"""
               |select
               |    tqbs,cast(byxsl as string) bqxsl
               |from tqxstjxx
               |where ny=${lastMonth2}
               |""".stripMargin)

        val tqSsqxslList= tqSsqxsl.collect()
        val tqSsqxslMap = scala.collection.mutable.Map[String, String]()
        for (xsl <- tqSsqxslList) {
            tqSsqxslMap.put(xsl.getAs[String]("tqbs"), xsl.getAs[String]("bqxsl"))
        }
        val tqSsqxslBroadcast= sparkSession.sparkContext.broadcast(tqSsqxslMap)

        //输入key ，获取value
        sparkSession.udf.register("getTqSsqXsl", (xlxdbs: String) => {
            tqSsqxslBroadcast.value.get(xlxdbs).getOrElse("0")
        })
        println("8")

        end = System.currentTimeMillis()
        println("计量表数据源程序运行"+ (end-start)/1000 + "秒")
        println("数据源读取完毕")

}




}
