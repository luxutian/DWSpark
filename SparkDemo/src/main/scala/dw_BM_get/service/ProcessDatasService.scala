package dw_BM_get.service

import java.lang
import java.util.UUID

import dw_BM_get.dao.GetTablesDao
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable

object ProcessDatasService {
    def etlDateService(sparkSession:SparkSession, url :String)={
        //1 从dao 层获取基础数据
        val cbxx = GetTablesDao.OdsGetCbxx(sparkSession, url)
        val dmbm = GetTablesDao.OdsGetDmbm(sparkSession, url)
        val jld = GetTablesDao.OdsGetJld(sparkSession, url)
        val xlgldw = GetTablesDao.OdsGetXlgldw(sparkSession, url)
        val xlxd = GetTablesDao.OdsGetXlxd(sparkSession, url)
        val ydkh = GetTablesDao.OdsGetYdkh(sparkSession, url)
        val zz = GetTablesDao.OdsGetZz(sparkSession, url)

        cbxx.show(5)
         dmbm.show(5)
         jld.show(5)
         xlgldw.show(5)
         xlxd.show(5)
         ydkh.show(5)
         zz.show(5)


      /*  spark.sql("select l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm,x.gddwbm
                    from _xlxd l
                    join _xlgldw x
                    on l.xlxdbs = x.xlxdbs")
          .createOrReplaceTempView("_xlxd")  */

        val xlxd2 = xlxd.join(xlgldw, Seq("xlxdbs")).select("xlxdbs", "xlbh", "xlmc", "gisid", "xlyxzt", "dydjdm", "xllbdm", "gddwbm")
        xlxd2.show(5)
        println("打印表数据输出")
        /*---------------注册成临时表--给sql使用------------------------------------------------------------------*/
        //2 形成临时表
        cbxx.createOrReplaceTempView("cbxx")
        ydkh.createOrReplaceTempView("ydkh")
        jld.createOrReplaceTempView("jld")
        zz.createOrReplaceTempView("zz")
        dmbm.createOrReplaceTempView("dmbm")
        xlgldw.createOrReplaceTempView("xlgldw")
        //xlxd.createOrReplaceTempView("_xlxd")
        xlxd2.createOrReplaceTempView("xlxd")


/*-----------------------------------------------------------------------------------------*/
        //获取供电所编码
        val zzlxdmList: Array[Row] = zz.collect()
        val zzlxdmMap = scala.collection.mutable.Map[String, String]()
        // TODO: new mutable.HashMap[String,String]()

        for (zzlxdm <- zzlxdmList) {
            zzlxdmMap.put(zzlxdm.getAs[String]("zzbm"), zzlxdm.getAs[String]("zzlxdm"))
        }
        val zzlxdmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(zzlxdmMap) //todo 广播变量

        /*-------------------------组织  zzbm-组织编号,zzmc-组织名称-------------------------------------------*/
        //自定义获取组织名称函数
        val zzList: Array[Row] = sparkSession.sql("select zzbm,zzmc,zzlxdm from zz").collect()
        val zzMap = scala.collection.mutable.Map[String, String]()
        for (zz <- zzList) {
            zzMap.put(zz.getAs[String]("zzbm"), zz.getAs[String]("zzmc"))  //获取组织编号，组织名称
        }
        val zzBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(zzMap)

        //自定义获取代码编码名称
        val dmbmList: Array[Row] = sparkSession.sql("select dmbm,dmbmmc,dmfl from dmbm").collect()
        val dmbmMap = scala.collection.mutable.Map[String, String]()
        for (dmbm <- dmbmList) {
            dmbmMap.put(dmbm.getAs[String]("dmfl").concat("_").concat(dmbm.getAs[String]("dmbm")), dmbm.getAs[String]("dmbmmc"))
        }
        val dmbmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(dmbmMap)

        //获取地区编码函数
        val dqbmList: Array[Row] = zz.collect()
        val dqbmMap = scala.collection.mutable.Map[String, String]()
        for (dqbm <- dqbmList) {
            dqbmMap.put(dqbm.getAs[String]("zzbm"), dqbm.getAs[String]("dqbm"))
        }
        val dqbmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(dqbmMap)



        /***********自定义 UDF****************************************************************************/
        //1 自定义获取uuid函数
        sparkSession.udf.register("getUUID", () => { // TODO: 这个函数不需要传参数
            val uuid = UUID.randomUUID().toString  //UUID.randomUUID() 生成唯一识别码
            uuid.replaceAll("-", "")  //(regex:正则对象，replacement:替换成的字符串)
        })
        //2 处理数值类型null值
        sparkSession.udf.register("handleNumber", (value: Any) => { //todo (value:Any)参数为任何值
            value match {
                case null => "-999"
                case a:lang.Double => a.toString
                case b: java.math.BigDecimal => b.toString
                case c:lang.Long => c.toString
                case d:String => d
                case _ => "-999"
            }
        })
        //3广东电网编码
        sparkSession.udf.register("getGdsbm", (gddwbm: String) => { // TODO: 广东电网编码
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

        //4 获取区县局编码
        sparkSession.udf.register("getQxjbm", (gddwbm: String) => {
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

        //5 获取地市局编码
        sparkSession.udf.register("getDsjbm", (gddwbm: String) => {
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

        //6 获取组织名称
        sparkSession.udf.register("getzzmc", (zzbm: String) => {
            zzBroadcast.value.get(zzbm).getOrElse("-")
        })

        //7
        sparkSession.udf.register("getdmbmmc", (dmfl: String, dmbm: String) => {
            dmbmBroadcast.value.get(dmfl+"_"+dmbm).getOrElse(null)
        })
        //8
        sparkSession.udf.register("getdqbm", (zzbm: String) => {
            dqbmBroadcast.value.get(zzbm).getOrElse("031200")
        })



        /*---------------sql关联的表格是在程序里查询的临时表----------------------------------------------------------------*/
        val xl_11 =
            s"""
               |select
               |	getUUID() id,
               |	null create_id,
               |	cast('2020-05-01 00:00:00' as timestamp) create_time,
               |	cast('2020-05-01 00:00:00' as timestamp) update_time,
               |	null updator_id,
               |	l.gddwbm,
               |	202004 tjsj,
               |	'1' tjzq,
               |	'1' xltqbz,
               |	l.xlxdbs,
               |	l.xlbh,
               |	l.xlmc,
               |	null,
               |	null,
               |	null,
               |	y.yhbh,
               |	y.yhmc,
               |	y.yhlbdm yhlbdm,
               |	j.jldbh,
               |	c.zcbh,
               |	handleNumber(c.zhbl),
               |	'xl_11' ycgzbh,
               |	getDsjbm(l.gddwbm) dsjbm,
               |	getQxjbm(l.gddwbm) qxjbm,
               |	getGdsbm(l.gddwbm) gdsbm,
               |	getzzmc(getDsjbm(l.gddwbm)) dsj,
               |	getzzmc(getQxjbm(l.gddwbm)) qxj,
               |	getzzmc(getGdsbm(l.gddwbm)) gds,
               |	getdmbmmc('YHLBDM',y.yhlbdm) yhlb,
               |	getdqbm(l.gddwbm) dqbm,
               |	202004 nybm
               |from cbxx c
               |join ydkh y on c.yhbh=y.yhbh
               |join jld j on j.jldbh=c.jldbh and j.jldytdm <> '410'
               |join xlxd l on l.xlxdbs=j.xlxdbs
               |where c.dfny= 202004
               |	and (c.scbss is null or c.bcbss is null)
               |	and (y.yhlbdm in ('80','60','10','11') or (y.yhlbdm = '40' and y.dydjdm = '08'))
               |	and y.yhztdm <> '2'
               |limit 1
               |
               |""".stripMargin
        val result = sparkSession.sql(xl_11)
        result.show(5)
        println("输出表打印")
            result.coalesce(3).write
              .options(Map("kudu.master"-> url, "kudu.table"-> "impala::lineloss.gk_yxlwcjyhqd")) // 2020/10/22 月线路未采集用户清单
              .mode(SaveMode.Append)  // 2020/10/22 官网只支持append
              .format("kudu").save

        /* 客户端的写入模式*/
        //  kuduContext.updateRows(df, "test_table")


    }

}
/*
建表语句
create table lineloss.gk_yxlwcjyhqd
(
id                   string comment '主键',
creator_id           string comment '创建人id',
create_time          timestamp comment '创建时间',
update_time          timestamp comment '修改时间',
updator_id           string comment '修改人id',
gddwbm               string comment '供电单位编码',
tjsj                 int comment '年月',
tjzq                 string comment '日或者月',
xltqbz               string comment '线路台区标志',
xlxdbs               string comment '线路线段标识',
xlbh                 string comment '线路编号',
xlmc                 string comment '线路名称',
tqbs                 string comment '台区标识',
tqbh                 string comment '台区编号',
tqmc                 string comment '台区名称',
yhbh                 string comment '用户编号',
yhmc                 string comment '用户名称',
yhlbdm               string comment '用户类别代码',
jldbh                string comment '计量点编号',
zcbh                 string comment '资产编号',
zhbl                 double comment '综合倍率',
ycgzbh               string comment '异常规则编号',
dsjbm                string comment '地市局编码',
qxjbm                string comment '区县局编码',
gdsbm                string comment '供电所编码',
dsj                  string comment '地市局',
qxj                  string comment '区县局',
gds                  string comment '供电所',
yhlb                 string comment '用户类别',
dqbm                 string not null comment '地区编码，用于分区。',
nybm                 int not null comment '年月编码，用于分区。',
primary key (id)
)
partition by hash partitions 16
stored as kudu;

 */