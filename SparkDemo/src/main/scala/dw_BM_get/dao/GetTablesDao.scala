package dw_BM_get.dao

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GetTablesDao {


    def OdsGetCbxx(sparkSession:SparkSession, url:String)={
        val table ="csg_ods_yx.lc_cbxx_d_kudu"
        val dt=""
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
          //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }

    def OdsGetYdkh(sparkSession:SparkSession, url:String)={
        val table ="csg_ods_yx.kh_ydkh"
        val dt=""
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }


    def OdsGetJld(sparkSession:SparkSession, url:String)={
        val table ="csg_ods_yx.kh_jld"
        val dt=""
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }



    def OdsGetZz(sparkSession:SparkSession, url:String)={
        val table ="csg_ods_yx.xt_zz"
        val dt=""
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }



    def OdsGetDmbm(sparkSession:SparkSession, url:String)={
        val table ="csg_ods_yx.xt_dmbm"
        val dt=""
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }


    def OdsGetXlgldw(sparkSession:SparkSession, url:String)={
        sparkSession.sql("select * from csg_ods_yx.dw_xlgldw") // 2020/10/22 这张表在hive里面

//
//        val table ="csg_ods_yx.dw_xlgldw"
//        val dt=""
//        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
//        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }


    def OdsGetXlxd(sparkSession:SparkSession, url:String)={
        val table ="csg_ods_yx.dw_xlxd"
        val dt=""
        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
        sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
        //.where(s"dt='${dt}'") //不知道能不能导增量数据
    }





}
