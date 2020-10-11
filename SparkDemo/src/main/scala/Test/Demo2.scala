package Test

import java.lang
import java.util.UUID

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable

/** 利用udf函数，转换成编码
  * @Author: panzibin
  */
object Demo2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("demo").config("spark.sql.shuffle.partitions", "100").config("spark.dynamicAllocation.enabled",false).getOrCreate()
        import spark.implicits._
        import org.apache.kudu.client._
        import org.apache.kudu.spark.kudu.KuduContext
        import collection.JavaConverters._
        val url = "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"

        /*--------------读取kudu的表格形成临时表-------------------------------------------------------*/
        /*-------------创建3张临时表 _cbxx --- _ydkh   _jld  --     --------------------------------------------------*/
        //val kuduMap = Map[String,String]("kudu.master" -> url , "kudu.table" -> "csg_ods_yx.lc_cbxx_d_kudu")

        val cbxx = spark.read
          .options(Map("kudu.master" -> "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051" , "kudu.table" -> "csg_ods_yx.lc_cbxx_d_kudu"))
          .format("org.apache.kudu.spark.kudu").load
         cbxx.createOrReplaceTempView("_cbxx")

        val ydkh = spark.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "csg_ods_yx.kh_ydkh"))
          .format("org.apache.kudu.spark.kudu").load
        ydkh.createOrReplaceTempView("_ydkh")

        val jld = spark.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "csg_ods_yx.kh_jld"))
          .format("org.apache.kudu.spark.kudu").load
        jld.createOrReplaceTempView("_jld")

        /*--------------有创建5张表 _zz  _dmbm  _xlgldw  _xlxd   _xlxd  ----------------------------------------------------------*/
        //组织  zzbm-组织编号,zzmc-组织名称
        val zz = spark.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "csg_ods_yx.xt_zz"))
          .format("org.apache.kudu.spark.kudu").load
        zz.createOrReplaceTempView("_zz")
        //代码编码
        val dmbm = spark.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "csg_ods_yx.xt_dmbm"))
          .format("org.apache.kudu.spark.kudu").load
        dmbm.createOrReplaceTempView("_dmbm")

        val xlgldw = spark.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "ods_yx.dw_xlgldw"))
          .format("org.apache.kudu.spark.kudu").load
        xlgldw.createOrReplaceTempView("_xlgldw")

        val xlxd = spark.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "csg_ods_yx.dw_xlxd"))
          .format("org.apache.kudu.spark.kudu").load
        xlxd.createOrReplaceTempView("_xlxd")

        spark.sql("select l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm,x.gddwbm from _xlxd l join _xlgldw x on l.xlxdbs = x.xlxdbs")
          .createOrReplaceTempView("_xlxd")

/*---------------------自定义了几个函数  getUUID handleNumber getGdsbm getQxjbm  getDsjbm getzzmc getdmbmmc getdqbm-----*/
        //1 自定义获取uuid函数
        spark.udf.register("getUUID", () => { // TODO: 这个函数不需要传参数
            val uuid = UUID.randomUUID().toString  //UUID.randomUUID() 生成唯一识别码
            uuid.replaceAll("-", "")  //(regex:正则对象，replacement:替换成的字符串)
        })
        //2 处理数值类型null值
       spark.udf.register("handleNumber", (value: Any) => { //todo (value:Any)参数为任何值
            value match {
                case null => "-999"
                case a:lang.Double => a.toString
                case b: java.math.BigDecimal => b.toString
                case c:lang.Long => c.toString
                case d:String => d
                case _ => "-999"
            }
        })
        //获取供电所编码
        val zzlxdmList: Array[Row] = zz.collect()
        val zzlxdmMap = scala.collection.mutable.Map[String, String]()
        // TODO: new mutable.HashMap[String,String]()

        for (zzlxdm <- zzlxdmList) {
            zzlxdmMap.put(zzlxdm.getAs[String]("zzbm"), zzlxdm.getAs[String]("zzlxdm"))
        }
        val zzlxdmBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(zzlxdmMap) //todo 广播变量

        /*--------------获取三级编码-----返回的值怎么用---------------------------------------------------*/
        //3
        spark.udf.register("getGdsbm", (gddwbm: String) => { // TODO: 广东电网编码
            gddwbm match {
                case null => "-"
                case _ =>{
                    val zzlxdm: String = zzlxdmBroadcast.value.get(gddwbm).getOrElse(null)
                    if ("5".equals(zzlxdm)) {
                        gddwbm
                    } else {
                        "-"
                    }
                }
            }
        })

        //4
        //获取区县局编码
        spark.udf.register("getQxjbm", (gddwbm: String) => {
            gddwbm match {
                case null => "-"
                case _ =>{
                    val zzlxdm: String = zzlxdmBroadcast.value.get(gddwbm).getOrElse(null)
                    if ("5".equals(zzlxdm)) {
                        gddwbm.substring(0, gddwbm.length - 2)
                    } else if ("4".equals(zzlxdm)) {
                        gddwbm
                    } else {
                        "-"
                    }
                }
            }
        })

        //5
        //获取地市局编码
        spark.udf.register("getDsjbm", (gddwbm: String) => {
            gddwbm match {
                case null => "-"
                case _ =>{
                    val zzlxdm: String = zzlxdmBroadcast.value.get(gddwbm).getOrElse(null)
                    if ("5".equals(zzlxdm) || "4".equals(zzlxdm)) {
                        gddwbm.substring(0, 4)
                    } else if("3".equals(zzlxdm) ) {
                        gddwbm
                    } else {
                        "-"
                    }
                }
            }
        })

        /*-------------------------组织  zzbm-组织编号,zzmc-组织名称-------------------------------------------*/
        //自定义获取组织名称函数
        val zzList: Array[Row] = spark.sql("select zzbm,zzmc,zzlxdm from _zz").collect()
        val zzMap = scala.collection.mutable.Map[String, String]()
        for (zz <- zzList) {
            zzMap.put(zz.getAs[String]("zzbm"), zz.getAs[String]("zzmc"))  //获取组织编号，组织名称
        }
        val zzBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(zzMap)
        //6
        spark.udf.register("getzzmc", (zzbm: String) => {
            zzBroadcast.value.get(zzbm).getOrElse("-")
        })
        //自定义获取代码编码名称
        val dmbmList: Array[Row] = spark.sql("select dmbm,dmbmmc,dmfl from _dmbm").collect()
        val dmbmMap = scala.collection.mutable.Map[String, String]()
        for (dmbm <- dmbmList) {
            dmbmMap.put(dmbm.getAs[String]("dmfl").concat("_").concat(dmbm.getAs[String]("dmbm")), dmbm.getAs[String]("dmbmmc"))
        }
        val dmbmBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(dmbmMap)
        //7
        spark.udf.register("getdmbmmc", (dmfl: String, dmbm: String) => {
            dmbmBroadcast.value.get(dmfl+"_"+dmbm).getOrElse(null)
        })
        //获取地区编码函数
        val dqbmList: Array[Row] = zz.collect()
        val dqbmMap = scala.collection.mutable.Map[String, String]()
        for (dqbm <- dqbmList) {
            dqbmMap.put(dqbm.getAs[String]("zzbm"), dqbm.getAs[String]("dqbm"))
        }
        val dqbmBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(dqbmMap)
        //8
        spark.udf.register("getdqbm", (zzbm: String) => {
            dqbmBroadcast.value.get(zzbm).getOrElse("031200")
        })

        /*---------------sql关联的表格是在程序里查询的临时表----------------------------------------------------------------*/
        val xl_11 =
            s"""
         select
         getUUID() id,
         null create_id,
         cast('2020-05-01 00:00:00' as timestamp) create_time,
         cast('2020-05-01 00:00:00' as timestamp) update_time,
         null updator_id,
         l.gddwbm,
         202004 tjsj,
         '1' tjzq,
         '1' xltqbz,
         l.xlxdbs,
         l.xlbh,
         l.xlmc,
         null,
         null,
         null,
         y.yhbh,
         y.yhmc,
         y.yhlbdm yhlbdm,
         j.jldbh,
         c.zcbh,
         handleNumber(c.zhbl),
         'xl_11' ycgzbh,
         getDsjbm(l.gddwbm) dsjbm,
         getQxjbm(l.gddwbm) qxjbm,
         getGdsbm(l.gddwbm) gdsbm,
         getzzmc(getDsjbm(l.gddwbm)) dsj,
         getzzmc(getQxjbm(l.gddwbm)) qxj,
         getzzmc(getGdsbm(l.gddwbm)) gds,
         getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
         getdqbm(l.gddwbm) dqbm,
         202004 nybm
         from _cbxx c
         join _ydkh y on c.yhbh=y.yhbh
         join _jld j on j.jldbh=c.jldbh and j.jldytdm <> '410'
         join _xlxd l on l.xlxdbs=j.xlxdbs
         where c.dfny= 202004
         and (c.scbss is null or c.bcbss is null)
         and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and y.dydjdm = '08'))
         and y.yhztdm <> '2'
         limit 1
      """
        spark.sql(xl_11).write
          .options(Map("kudu.master"-> url, "kudu.table"-> "lineloss.gk_yxlwcjyhqd2"))
          .mode(SaveMode.Overwrite)
          .format("org.apache.kudu.spark.kudu").save
        /* 客户端的写入模式*/
        //  kuduContext.updateRows(df, "test_table")


    }
}