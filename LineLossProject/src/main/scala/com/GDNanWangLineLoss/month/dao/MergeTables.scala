package com.GDNanWangLineLoss.month.dao

import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.SparkSession

/**
  * 生成计量表码合并视图
  *  (与原来zepperlin 有差异)
  */
object MergeTables {

    private val  url ="10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"

    def mergeTables(sparkSession:SparkSession)={

        /*--------------------------------------------------------------------------*/
        //1 户表终端关系
        val rxs_hbzdgx1=sparkSession.read
          .options(Map("kudu.master" -> url,"kudu.table" -> "tmr.hbzdgx"))
          .format("org.apache.kudu.spark.kudu")
          .load()

        rxs_hbzdgx1.createOrReplaceTempView("rxs_hbzdgx")
        val rxs_hbzdgx =sparkSession.sql(
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
               |  from rxs_hbzdgx a where a.etl_city in (${cityNameList})
               |  group by a.cldbs,
               |           a.bjlx,
               |           a.yhbh,
               |           a.jldbh,
               |           a.bjzcbh,
               |           a.zhbl,
               |           a.cldlxdm,
               |           a.jldbs
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


    }


}
