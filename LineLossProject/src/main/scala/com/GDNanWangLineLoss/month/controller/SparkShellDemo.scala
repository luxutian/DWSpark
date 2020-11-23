package com.GDNanWangLineLoss.month.controller

import java.lang
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.GDNanWangLineLoss.month.bean.City
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * @Date:2020/11/6/10:30
 * @Description:
 */
object SparkShellDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName(this.getClass.getName)
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
        val ssc = spark.sparkContext


        /************************************************************************************************/
        //线路编号
        val XL_11 = "'XL-Y-CHS01'"
        val XL_12 = "'XL-Y-CHS02'"
        val XL_13 = "'XL-Y-CHS03'"
        val XL_14 = "'XL-Y-CHS04'"
        val XL_15 = "'XL-Y-CHS05'"
        val XL_16 = "'XL-Y-CHS06'"
        val XL_17 = "'XL-Y-CHS07'"
        val XL_18 = "'XL-Y-CHS08'"
        val XL_19 = "'XL-Y-CHS09'"
        val XL_110 = "'XL-Y-CHS10'"
        val XL_111 = "'XL-Y-CHS11'"
        val XL_21 = "'XL-Y-DA01'"
        val XL_22 = "'XL-Y-DA02'"
        val XL_23 = "'XL-Y-DA03'"
        val XL_24 = "'XL-Y-DA04'"
        val XL_25 = "'XL-Y-DA05'"
        val XL_26 = "'XL-Y-DA06'"
        val XL_27 = "'XL-Y-DA07'"
        val XL_28 = "'XL-Y-DA08'"
        val XL_29 = "'XL-Y-DA09'"
        val XL_210 = "'XL-Y-DA10'"
        val XL_211 = "'XL-Y-DA11'"
        val XL_212 = "'XL-Y-DA12'"
        val XL_213 = "'XL-Y-DA13'"
        val XL_31 = "'XL-Y-JL01'"
        val XL_32 = "'XL-Y-JL02'"
        val XL_33 = "'XL-Y-JL03'"
        val XL_34 = "'XL-Y-JL04'"
        val XL_35 = "'XL-Y-JL05'"
        val XL_36 = "'XL-Y-JL06'"
        val XL_41 = "'XL-Y-SC01'"
        val XL_42 = "'XL-Y-SC02'"
        val XL_43 = "'XL-Y-SC03'"
        val XL_44 = "'XL-Y-SC04'"
        val XL_45 = "'XL-Y-SC05'"
        val XL_46 = "'XL-Y-SC06'"
        val XL_47 = "'XL-Y-SC07'"
        val XL_48 = "'XL-Y-SC08'"
        val XL_49 = "'XL-Y-SC09'"
        val XL_410 = "'XL-Y-SC10'"
        val XL_51 = "'XL-Y-YD01'"
        val XL_52 = "'XL-Y-YD02'"
        val XL_53 = "'XL-Y-YD03'"
        val XL_61 = "'XL-Y-QT01'"


        //台区编号
        val TQ_11 = "'TQ-Y-CHS01'"
        val TQ_12 = "'TQ-Y-CHS02'"
        val TQ_13 = "'TQ-Y-CHS03'"
        val TQ_14 = "'TQ-Y-CHS04'"
        val TQ_15 = "'TQ-Y-CHS05'"
        val TQ_16 = "'TQ-Y-CHS06'"
        val TQ_17 = "'TQ-Y-CHS07'"
        val TQ_18 = "'TQ-Y-CHS08'"
        val TQ_19 = "'TQ-Y-CHS09'"
        val TQ_21 = "'TQ-Y-DA01'"
        val TQ_22 = "'TQ-Y-DA02'"
        val TQ_23 = "'TQ-Y-DA03'"
        val TQ_24 = "'TQ-Y-DA04'"
        val TQ_25 = "'TQ-Y-DA05'"
        val TQ_26 = "'TQ-Y-DA06'"
        val TQ_27 = "'TQ-Y-DA07'"
        val TQ_28 = "'TQ-Y-DA08'"
        val TQ_29 = "'TQ-Y-DA09'"
        val TQ_210 = "'TQ-Y-DA10'"
        val TQ_211 = "'TQ-Y-DA11'"
        val TQ_212 = "'TQ-Y-DA12'"
        val TQ_213 = "'TQ-Y-DA13'"
        val TQ_214 = "'TQ-Y-DA14'"
        val TQ_215 = "'TQ-Y-DA15'"
        val TQ_31 = "'TQ-Y-JL01'"
        val TQ_32 = "'TQ-Y-JL02'"
        val TQ_33 = "'TQ-Y-JL03'"
        val TQ_34 = "'TQ-Y-JL04'"
        val TQ_35 = "'TQ-Y-JL05'"
        val TQ_41 = "'TQ-Y-SC01'"
        val TQ_42 = "'TQ-Y-SC02'"
        val TQ_43 = "'TQ-Y-SC03'"
        val TQ_44 = "'TQ-Y-SC04'"
        val TQ_45 = "'TQ-Y-SC05'"
        val TQ_46 = "'TQ-Y-SC06'"
        val TQ_51 = "'TQ-Y-YD01'"
        val TQ_52 = "'TQ-Y-YD02'"
        val TQ_61 = "'TQ-Y-QT01'"

        //信息系统问题编号
        val XXXTBH = "6000202"

        // 2020/11/6 1 设置全局变量
            // 2020/10/29 时间格式
        val df = new SimpleDateFormat("yyyyMM") //年月
        val _df = new SimpleDateFormat("yyyy-MM")
        val df_cjsj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dfYear = new SimpleDateFormat("yyyy")

        val calendar = Calendar.getInstance() // 2020/10/16 获取系统时间 (这不是一个直观的时间)


        //yyyy当前年份
        val year = dfYear.format(calendar.getTime)

        //创建时间
        val create_time: String = "'" + df_cjsj.format(calendar.getTime) + "'"

        //系统月份
        //    val currentMonth = df.format(calendar.getTime)
        val currentMonth = "202005"

        //T-1
        calendar.add(Calendar.MONDAY, -1) // 2020/10/22 减一个月
        //同期年月
        calendar.add(Calendar.YEAR, -1) // 2020/10/22 减一年


        //    val tqny = df.format(calendar.getTime) // 2020/10/22 减一年一月的时间点
        val tqny = "201904" // 2020/10/22 减一年一月的时间点


        calendar.add(Calendar.YEAR, 1)// 2020/10/22 加一年
        calendar.add(Calendar.MONDAY, 1) // 2020/10/22 加一个月

        //    val addOneMonth = df.format(calendar.getTime)
        val addOneMonth = "202005"
        //    val _addOneMonth = _df.format(calendar.getTime)
        val _addOneMonth = "2020-05"

        calendar.add(Calendar.MONDAY, -1) // 2020/10/22 又减一个月

        //yyyyMM当前月份
        //    val nowMonth = df.format(calendar.getTime)
        val nowMonth = "202005"  // val nowMonth = 201912


        //    val month = calendar.get(Calendar.MONTH)    // 2020/10/22 月份 MM （1 2 3 10 12）calendar 获取月份就只有月份
        val month = 5

        //yyyy-MM当前月份
        //    val _nowMonth = _df.format(calendar.getTime)// val _nowMonth = "2019-12"
        val _nowMonth = "2020-05"// val _nowMonth = "2019-12"

        calendar.add(Calendar.MONDAY, -1)  //todo  当前月份减一


        //年月编码
        val nybm = nowMonth

        //todo 上个月份
        //    val lastMonth = df.format(calendar.getTime)  // val lastMonth = 201911
        val lastMonth ="202004" // val lastMonth = 201911

        //yyyy-MM上个月份
        //    val _lastMonth = _df.format(calendar.getTime) // val _lastMonth = "2019-11"
        val _lastMonth = "2020-04" // val _lastMonth = "2019-11"


        //todo 前第2个月
        calendar.add(Calendar.MONDAY, -1) // TODO:
        //    val lastMonth2 = df.format(calendar.getTime)
        val lastMonth2 ="202003"

        //todo 前第3个月
        calendar.add(Calendar.MONDAY, -1)
        //    val lastMonth3 = df.format(calendar.getTime)
        val lastMonth3 = "202002"

        //todo 前第4个月
        calendar.add(Calendar.MONDAY, -1)
        //    val lastMonth4 = df.format(calendar.getTime)
        val lastMonth4 = "202001"

        //todo 前第5个月
        calendar.add(Calendar.MONDAY, -1)
        //    val lastMonth5 = df.format(calendar.getTime)
        val lastMonth5 = "201912"

        //统计周期月标识
        val ybs = "'1'"
        //统计周期日标识
        val rbs = "'2'"
        //线路标志
        val xlbz = "'1'"
        //台区标志
        val tqbz = "'2'"


        //结果表的schema
        //    val writeSchema = "gpsx_sxxm"
        val writeSchema = "impala::lineloss" // 2020/10/16


        //创建人id
        val creator_id = "null"
        //修改时间
        val update_time = create_time
        //修改人id
        val updator_id = "null"
        //工单翻月日期
        val gdfyrq = "'" + _nowMonth + "-28'"

        val resultPartition = 2

        //定义地市局
        val citySG:City = City("SG", "030200")
        val cityST:City = City("ST", "030500")
        val cityFS:City = City("FS", "030600")
        val cityZQ:City = City("ZQ", "031200")
        val cityZS:City = City("ZS", "032000")

        //val cityNameList = "'"+citySG.name+"','"+cityST.name+"','"+cityFS.name+"','"+cityZQ.name+"','"+cityZS.name+"'"
        //val cityCodeList = "'"+citySG.code+"','"+cityST.code+"','"+cityFS.code+"','"+cityZQ.code+"','"+cityZS.code+"'"
        val cityNameList = "'"+cityZS.name+"'"  // 2020/10/22 ZS
        val cityCodeList = "'"+cityZS.code+"'"  // 2020/10/22 032000

        //定义统计结束时间
        var v_sjsj = _addOneMonth + "-01 00:00:00"
        //定义统计开始时间
        var v_sjsj_before = _nowMonth + "-01 00:00:00"




        /*--------------------------------------------------------------------*/
        var yearMonth = nowMonth
        var tableName = "tmr_ods.to_new_dycldrdjbm"
        var tableNameG = "tmr_ods.to_new_gycldssbm"
        var tableNameC = "tmr_ods.to_new_clddldy"  //2020-10-15 没有这个表


            //如果数据时间不是当月，去相应月份表取数据
            if(!yearMonth.equals(currentMonth)){
                tableName = tableName + "_" + yearMonth
                tableNameG = tableNameG + "_" + yearMonth
                tableNameC = tableNameC + "_" + yearMonth
                try{
                    spark.sql(s"select 1 from ${tableName} limit 1").collect.size
                    spark.sql(s"select 1 from ${tableNameG} limit 1").collect.size
                    spark.sql(s"select 1 from ${tableNameC} limit 1").collect.size
                }catch{
                    case e:Exception => {
                        tableName = "tmr_ods.to_new_dycldrdjbm"
                        tableNameG = "tmr_ods.to_new_gycldssbm"
                        tableNameC = "tmr_ods.to_new_clddldy"
                    }
                }
                println(tableName)
                println(tableNameG)
                println(tableNameC)
            }



        //用于“生成计量表码合并视图”段落
        var addOneTableName = "tmr_ods.to_new_dycldrdjbm"  //没发现有这个表
        var addOneTableNameG = "tmr_ods.to_new_gycldssbm"


            //如果数据时间下月不是当月，去相应月份表取数据
            if(!addOneMonth.equals(currentMonth)){
                addOneTableName = addOneTableName + "_" + addOneMonth
                addOneTableNameG = addOneTableNameG + "_" + addOneMonth
                try{
                    spark.sql(s"select 1 from ${addOneTableName} limit 1").collect.size
                    spark.sql(s"select 1 from ${addOneTableNameG} limit 1").collect.size
                }catch{
                    case e:Exception => {
                        addOneTableName = "tmr_ods.to_new_dycldrdjbm"
                        addOneTableNameG = "tmr_ods.to_new_gycldssbm"
                    }
                }
                println(addOneTableName)
                println(addOneTableNameG)
            }


/*------------------------------------------------------------------------------------------------------------*/


        // 2020/11/6 2 读取数据源()



        // 2020/11/6 table1 cbxx
        spark.sql("use yxxt")
        //抄表信息
        val cbxx_all = spark.sql(
            s"""
               |select yhbh,zcbh,zhbl,scbss,bcbss,bqcbcs,sccbrq,cbsj,bjdl,jldbh,dfny,yhmc,jldcbfsdm,yxdnbbs,
               |sslxdm,bssce,yhlbdm,jbdl,cbsxh,sjcbfsdm,gzdbh,ywlbdm,dqbm,gddwbm,cbrbs,yddz,cbqdbh,jldcbsxh
               |from npmis_lc_cbxx
               |where dfny in (${nowMonth},${lastMonth}) and dqbm in (${cityCodeList})
               |""".stripMargin)
        cbxx_all.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("_cbxx_all")

        //抄表信息  yhbh-用户编号,zcbh-资产编号,zhbl-综合倍率,scbss-上次表示数,bcbss-本次表示数,bqcbcs-本期抄表示数,sccbrq-上次抄表日期,cbsj-抄表时间,bjdl-报警电量,
        //          jldbh-计量点编号,dfny-电费年月,yhmc-用户名称,jldcbfsdm-计量点抄表方式
        //lixc20200506新加字段 cbsxh-抄表顺序号,sjcbfsdm-实际抄表方式代码,gzdbh-工作单编号,ywlbdm-业务类别代码,dqbm-地区编码,gddwbm-供电单位编码,cbrbs-抄表人标识,yddz-用电地址,cbqdbh-抄表区段编号,jldcbsxh-计量点抄表顺序号
        val cbxx = spark.sql(
            s"""
               |
               |select yhbh,zcbh,zhbl,scbss,bcbss,bqcbcs,sccbrq,cbsj,bjdl,jldbh,dfny,yhmc,jldcbfsdm,
               |yxdnbbs,sslxdm,bssce,yhlbdm,jbdl,cbsxh,sjcbfsdm,gzdbh,ywlbdm,dqbm,gddwbm,cbrbs,yddz,cbqdbh,jldcbsxh
               |from _cbxx_all
               |where dfny in (${nowMonth},${lastMonth})
               |""".stripMargin)
        cbxx.cache().createOrReplaceTempView("_cbxx")

        // 2020/11/6 table2 jld
        //计量点   jldbh-计量点编号,jldmc-计量点名称,xlxdbs-线路线段标识,yhbh-用户编号,tqbs-台区标识,jlfsdm-计量方式,jxfsdm-接线方式代码
        //lixc20200506新加字段 jldydjdm-计量电压等级代码
        val jld = spark.sql(s"""select jldbh,jldmc,jldytdm,xlxdbs,yhbh,tqbs,jlfsdm,jxfsdm,jldztdm,jldlbdm,cbfsdm,gddwbm,jldydjdm from npmis_kh_jld where dqbm in (${cityCodeList})""")
        jld.createOrReplaceTempView("_jld")

        // 2020/11/6 table3 all_xlxd 线路路段
        //线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
        //lixc20200506只查配线
        val all_xlxd = spark.sql(s"""select l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm from npmis_dw_xlxd l where l.xllbdm = '1' and l.xlyxzt = '01' and l.dydjdm not in ('10','12','13','15') and dqbm in (${cityCodeList})""")
        all_xlxd.createOrReplaceTempView("_all_xlxd")


        // 2020/11/6 table4 xlxd 异常线路线段
        //异常线路线段  xlxdbs-线路线段标识,xlbh-线路编号,xlmc-线路名称,gisid-gis标识
        val xlxd = spark.sql(
            s"""
               |
               |select l.xlxdbs,l.xlbh,l.xlmc,l.gisid,l.xlyxzt,l.dydjdm,l.xllbdm,y.gddwbm
               |from npmis_dw_xlxd l
               |join _xsycxlhtqmx y on l.xlxdbs = y.xlxdbs
               |where y.ny = ${nowMonth} and y.xltqbz = ${xlbz}
               |""".stripMargin)
        xlxd.createOrReplaceTempView("_xlxd")


        // 2020/11/6 table5 ydkh用电客户
        //用电户   yhbh-用户编号,yhmc-用户名称,yhztdm-用电状态,dydjdm-电压等级代码,gddwbm-供电单位编码,lhrq-立户日期,cbqdbh-抄表区段,cbzq-抄表周期
        val ydkh = spark.sql(s"""select yhbh,yhmc,yhztdm,dydjdm,gddwbm,yhlbdm,lhrq,cbqdbh,cbzq,hyfldm from npmis_kh_ydkh where dqbm in (${cityCodeList})""")
        ydkh.createOrReplaceTempView("_ydkh")

        // 2020/11/6 table6 zz组织
        //组织  zzbm-组织编号,zzmc-组织名称
        val zz = spark.sql("select zzbm,zzmc,zzlxdm,dqbm from npmis_xt_zz")
        zz.createOrReplaceTempView("_zz")

        // 2020/11/6 table7 代码编码
        val dmbm = spark.sql("select dmbm,dmbmmc,dmfl from npmis_xt_dmbm")
        dmbm.createOrReplaceTempView("_dmbm")

        // 2020/11/6 table8 线路管理单位
        val xlgldw = spark.sql(s"""select * from npmis_dw_xlgldw where dqbm in (${cityCodeList})""")
        xlgldw.createOrReplaceTempView("_xlgldw")







        // 2020/11/6 4 广播变量编码
        //获取供电所编码
        val zzlxdmList: Array[Row] = zz.collect()
        val zzlxdmMap = scala.collection.mutable.Map[String, String]()
        // TODO: new mutable.HashMap[String,String]()

        for (zzlxdm <- zzlxdmList) {
            zzlxdmMap.put(zzlxdm.getAs[String]("zzbm"), zzlxdm.getAs[String]("zzlxdm"))
        }
        val zzlxdmBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(zzlxdmMap) //todo 广播变量

        /*-------------------------组织  zzbm-组织编号,zzmc-组织名称-------------------------------------------*/
        //自定义获取组织名称函数
        val zzList: Array[Row] = spark.sql("select zzbm,zzmc,zzlxdm from _zz").collect()
        val zzMap = scala.collection.mutable.Map[String, String]()
        for (zz <- zzList) {
            zzMap.put(zz.getAs[String]("zzbm"), zz.getAs[String]("zzmc"))  //获取组织编号，组织名称
        }
        val zzBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(zzMap)

        //自定义获取代码编码名称
        val dmbmList: Array[Row] = spark.sql("select dmbm,dmbmmc,dmfl from _dmbm").collect()
        val dmbmMap = scala.collection.mutable.Map[String, String]()
        for (dmbm <- dmbmList) {
            dmbmMap.put(dmbm.getAs[String]("dmfl").concat("_").concat(dmbm.getAs[String]("dmbm")), dmbm.getAs[String]("dmbmmc"))
        }
        val dmbmBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(dmbmMap)

        //获取地区编码函数
        val dqbmList: Array[Row] = zz.collect()
        val dqbmMap = scala.collection.mutable.Map[String, String]()
        for (dqbm <- dqbmList) {
            dqbmMap.put(dqbm.getAs[String]("zzbm"), dqbm.getAs[String]("dqbm"))
        }
        val dqbmBroadcast: Broadcast[mutable.Map[String, String]] = spark.sparkContext.broadcast(dqbmMap)


        // 2020/11/6 3 注册用户自定义函数
        //3.1 自定义获取uuid函数
        spark.udf.register("getUUID", () => { // TODO: 这个函数不需要传参数
            val uuid = UUID.randomUUID().toString  //UUID.randomUUID() 生成唯一识别码
            uuid.replaceAll("-", "")  //(regex:正则对象，replacement:替换成的字符串)
        })


        //3.2 处理数值类型null值
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

        //3.3广东电网编码
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

        //3.4 获取区县局编码
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

        //3.5 获取地市局编码
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

        //3.6 获取组织名称
        spark.udf.register("getzzmc", (zzbm: String) => {
            zzBroadcast.value.get(zzbm).getOrElse("-")
        })

        //3.7
        spark.udf.register("getdmbmmc", (dmfl: String, dmbm: String) => {
            dmbmBroadcast.value.get(dmfl+"_"+dmbm).getOrElse(null)
        })
        //3.8
        spark.udf.register("getdqbm", (zzbm: String) => {
            dqbmBroadcast.value.get(zzbm).getOrElse("031200")
        })

        //3.9
        spark.udf.register("getFormatDate", (time:Long) => {
            df_cjsj.format(new Date(time))
        })

        //3.10
        //获取供电单位编码前4个编码
        spark.udf.register("isFiveDsj", (gddwbm: String) => {
            gddwbm match {
                case s:String =>{
                    if(gddwbm.length<=4){
                        0
                    }else if("0305".equals(gddwbm.substring(0,4))||"0302".equals(gddwbm.substring(0,4))||"0306".equals(gddwbm.substring(0,4))||"0312".equals(gddwbm.substring(0,4))||"0320".equals(gddwbm.substring(0,4))){
                        1
                    }else{
                        0
                    }
                }
                case _ => 0
            }
        })


        // 2020/11/6 4 业务逻辑运算

        var start = 0l
        var end = 0l
        var reason = ""
        var isSuccess = 1


        start = System.currentTimeMillis()
        //线路1.1起止表码数据采集完整性检查
        try{
            //_xlcjwzl 线路采集完整率 取营销
            spark.sql(
                s"""
                   |
                   |select x.xlxdbs,round(count(distinct case when (c.scbss is not null and c.bcbss is not null and c.scbss != 0 and c.bcbss != 0) then c.jldbh else null end)/count(distinct c.jldbh)*100,3) cjwzl
                   |from _cbxx c
                   |join _jld j on j.jldbh=c.jldbh
                   |join _all_xlxd x on x.xlxdbs = j.xlxdbs
                   |where c.dfny= ${nowMonth}
                   |and ((c.yhlbdm in ('80','60') and c.sslxdm in ('121','221')) or (c.yhlbdm in ('10','11') and c.sslxdm = '121')
                   | or (c.yhlbdm = '40' and c.sslxdm in ('221','222','223','224')))
                   |group by x.xlxdbs
                   |""".stripMargin).createOrReplaceTempView("_xlcjwzl")

            val xl_11 =
                s"""
                   |select distinct ${creator_id} ,${create_time},${update_time},${updator_id},l.gddwbm,${nowMonth} tjsj,${ybs} tjzq,${xlbz} xltqbz,l.xlxdbs,l.xlbh,l.xlmc,null,null,null,getbdzbs(l.xlxdbs) bdzbs,getbdzbh(l.xlxdbs) bdzbh,getbdzmc(l.xlxdbs) bdzmc,
                   |k1.bdzkhbhh,null,y.yhbh,y.yhmc,y.yhlbdm yhlbdm,j.jldbh,c.zcbh,handleNumber(c.zhbl),
                   |getycgzbh(${XL_11}) ycgzbh,getDsjbm(l.gddwbm) dsjbm,getQxjbm(l.gddwbm) qxjbm,getGdsbm(l.gddwbm) gdsbm,
                   |getzzmc(getDsjbm(l.gddwbm)) dsj,getzzmc(getQxjbm(l.gddwbm)) qxj,getzzmc(getGdsbm(l.gddwbm)) gds,getdmbmmc('YHLBDM',y.yhlbdm) yhlb,getdqbm(l.gddwbm) dqbm,${nybm}
                   |from _cbxx c
                   |join _ydkh y on c.yhbh=y.yhbh
                   |join _jld j on j.jldbh=c.jldbh and j.jldytdm <> '410'
                   |join _xlxd l on l.xlxdbs=j.xlxdbs
                   |join _xlcjwzl w on w.xlxdbs = l.xlxdbs and w.cjwzl < 100
                   |lateral view outer explode(split(getbdzkhb(l.xlxdbs),',')) k1 as bdzkhbhh
                   |where c.dfny= ${nowMonth}
                   |and (c.scbss is null or c.bcbss is null or c.scbss = 0 or c.bcbss = 0)
                   |and ((c.yhlbdm in ('80','60') and c.sslxdm in ('121','221')) or (c.yhlbdm in ('10','11') and c.sslxdm = '121')
                   | or (c.yhlbdm = '40' and c.sslxdm in ('221','222','223','224')))
                   |and (y.yhlbdm in ('80','60','10','11'))
                   |and y.yhztdm <> '2'
                   |
                   |""".stripMargin
            spark.sql(xl_11).createOrReplaceTempView("RES_GK_WCJYHQD")
            spark.sql("select * from RES_GK_WCJYHQD where isFiveDsj(gddwbm) = 1").repartition(resultPartition).createOrReplaceTempView("RES_GK_WCJYHQD")
            spark.sql(s"insert into ${writeSchema}.GK_YXLWCJYHQD select getUUID(),*,tjsj fqrq from RES_GK_WCJYHQD").show(5)
//            spark.sql(s"insert into ${writeSchema}.GK_YXLWCJYHQD_HIS select getUUID(),*,tjsj fqrq from RES_GK_WCJYHQD")

//            spark.sql(s"select distinct ${creator_id},${create_time},${update_time},${updator_id},gddwbm,xltqbz,xlxdbs,null tqbs,tjzq,tjsj,ycgzbh,getycgzmc(ycgzbh) from RES_GK_WCJYHQD").repartition(resultPartition).createOrReplaceTempView("RES_GK_WCJYHQD_YCGDDWXLGX")
//            spark.sql(s"insert into ${writeSchema}.YCGDDWXLGX select getUUID(),*,tjsj fqrq from RES_GK_WCJYHQD_YCGDDWXLGX")
            reason = ""
            isSuccess = 1
        }catch{
            case e:Exception => {
                isSuccess = 0
                val message = e.getMessage
                if(message.length>800) reason = message.substring(0,800) else reason = message.substring(0,message.length)
            }
        }

        end = System.currentTimeMillis()
//        spark.sql(s"select getFormatDate(${end}) recordtime,'xl11',${isSuccess} state,'${reason}' reason,${(end-start)/1000} runtime").createOrReplaceTempView("ruleState")
        println(s"规则1.1运行${(end-start)/1000}秒")



    }

    //创建City类，输入参数为地市名称及地市代码
    case class City(name: String, code: String) {
        override def toString: String = {
            s"Name:$name -> Code:$code"
        }
    }
}
