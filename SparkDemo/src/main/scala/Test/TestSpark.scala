package Test

import org.apache.spark.sql.SparkSession

/**
  * @Author: panzibin
  */
object TestSpark {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("demo").config("spark.sql.shuffle.partitions", "100").config("spark.dynamicAllocation.enabled",false).getOrCreate()
        import org.apache.kudu.client._
        import org.apache.kudu.spark.kudu.KuduContext
        import collection.JavaConverters._
        val cbxx = spark.read
          .options(Map("kudu.master" -> "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051", "kudu.table" -> "csg_ods_yx.lc_cbxx_d_kudu"))
          .format("org.apache.kudu.spark.kudu").load
        cbxx.createOrReplaceTempView("_cbxx")
        val ydkh = spark.read
          .options(Map("kudu.master" -> "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051", "kudu.table" -> "csg_ods_yx.kh_ydkh"))
          .format("org.apache.kudu.spark.kudu").load
        ydkh.createOrReplaceTempView("_ydkh")
        val jld = spark.read
          .options(Map("kudu.master" -> "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051", "kudu.table" -> "csg_ods_yx.kh_jld"))
          .format("org.apache.kudu.spark.kudu").load
        jld.createOrReplaceTempView("_jld")
        val xlxd = spark.read
          .options(Map("kudu.master" -> "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051", "kudu.table" -> "csg_ods_yx.dw_xlxd"))
          .format("org.apache.kudu.spark.kudu").load
        xlxd.createOrReplaceTempView("_xlxd")


        val xl_11 =
            s"""
         select l.xlxdbs,l.xlbh,l.xlmc,y.yhbh,y.yhmc,y.yhlbdm yhlbdm,j.jldbh,c.zcbh
         from _cbxx c
         join _ydkh y on c.yhbh=y.yhbh
         join _jld j on j.jldbh=c.jldbh and j.jldytdm <> '410'
         join _xlxd l on l.xlxdbs=j.xlxdbs
         where c.dfny= 202004
         and (c.scbss is null or c.bcbss is null)
         and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and y.dydjdm = '08'))
         and y.yhztdm <> '2'
         limit 10
      """
        val count: Long = spark.sql(xl_11).count()
        println(s"规则1.1共${count}条")
    }
}