package com.GDNanWangLineLoss.month.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

object Function {

    /**
      * 根据线路线段标识获取线路编号和线路名称
      * @param sparkSession
      * @param all_xlxd
      * @return
      */
    def getXLbhAndXLmc(sparkSession:SparkSession ,all_xlxd:DataFrame)={

        val xlxdList= all_xlxd.collect()
        val xlbhMap = scala.collection.mutable.Map[String, String]()
        for (xlxd <- xlxdList) {
            xlbhMap.put(xlxd.getAs[String]("xlxdbs"), xlxd.getAs[String]("xlbh"))
        }
        val xlbhBroadcast= sparkSession.sparkContext.broadcast(xlbhMap)
        sparkSession.udf.register("getxlbh", (xlxdbs: String) => {
            xlbhBroadcast.value.get(xlxdbs).getOrElse(null)
        })

        val xlmcMap = scala.collection.mutable.Map[String, String]()
        for (xlxd <- xlxdList) {
            xlmcMap.put(xlxd.getAs[String]("xlxdbs"), xlxd.getAs[String]("xlmc"))
        }
        val xlmcBroadcast= sparkSession.sparkContext.broadcast(xlmcMap)
        sparkSession.udf.register("getxlmc", (xlxdbs: String) => {
            xlmcBroadcast.value.get(xlxdbs).getOrElse(null)
        })
    }


    var zzList:Array[Row]= _
    /**
      * 自定义获取组织名称函数
      * @param sparkSession
      * @return
      */
    def getZzmc(sparkSession:SparkSession)={
        zzList = sparkSession.sql("select zzbm,zzmc,zzlxdm from zz").collect()
        val zzMap = scala.collection.mutable.Map[String, String]()
        for (zz <- zzList) {
            zzMap.put(zz.getAs[String]("zzbm"), zz.getAs[String]("zzmc"))
        }
        val zzBroadcast= sparkSession.sparkContext.broadcast(zzMap)
        sparkSession.udf.register("getzzmc", (zzbm: String) => {
            zzBroadcast.value.get(zzbm).getOrElse("-")
        })
    }

    /**
      *获取供电所编码
      * @param sparkSession
      * @param zz
      * @return
      */
    def getGDSbm(sparkSession:SparkSession,zz:DataFrame)={
        val zzlxdmList= zz.collect()
        val zzlxdmMap = scala.collection.mutable.Map[String, String]()
        for (zzlxdm <- zzlxdmList) {
            zzlxdmMap.put(zzlxdm.getAs[String]("zzbm"), zzlxdm.getAs[String]("zzlxdm"))
        }
        val zzlxdmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(zzlxdmMap)
        sparkSession.udf.register("getGdsbm", (gddwbm: String) => {
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
        zzlxdmBroadcast
    }


    /**
      * 获取区县局编码
      * @param sparkSession
      * @param zzlxdmBroadcast
      * @return
      */
    def getXGbm(sparkSession:SparkSession,zzlxdmBroadcast:Broadcast[mutable.Map[String, String]])={
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
    }

    /**
      * 获取地市局编码
      * @param sparkSession
      * @param zzlxdmBroadcast
      * @return
      */
    def getDSGbm(sparkSession:SparkSession,zzlxdmBroadcast:Broadcast[mutable.Map[String, String]])={
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
    }

    /**
      * 自定义获取代码编码名称
      * @param sparkSession
      * @return
      */
    def getDMbm(sparkSession:SparkSession)={
        val dmbmList= sparkSession.sql("select dmbm,dmbmmc,dmfl from dmbm").collect()
        val dmbmMap = scala.collection.mutable.Map[String, String]()
        for (dmbm <- dmbmList) {
            dmbmMap.put(dmbm.getAs[String]("dmfl").concat("_").concat(dmbm.getAs[String]("dmbm")), dmbm.getAs[String]("dmbmmc"))
        }
        val dmbmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(dmbmMap)
        sparkSession.udf.register("getdmbmmc", (dmfl: String, dmbm: String) => {
            dmbmBroadcast.value.get(dmfl+"_"+dmbm).getOrElse(null)
        })
    }



    /**
      * 自定义获取变电站标识
      * @param sparkSession
      * @return
      */
    def getBDZbs(sparkSession:SparkSession)={
        val bdzList: Array[Row] = sparkSession.sql("select * from bdzglb").collect()  //有必要拉数据到driver
        val bdzbsMap = scala.collection.mutable.Map[String, String]()
        for (bdzglb <- bdzList) {
            bdzbsMap.put(bdzglb.getAs[String]("xlxdbs"), bdzglb.getAs[String]("bdzbs"))
        }
        val bdzbsBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(bdzbsMap)
        sparkSession.udf.register("getbdzbs", (xlxdbs: String) => {
            bdzbsBroadcast.value.get(xlxdbs).getOrElse(null)
        })
        bdzList

    }


    /**
      * 自定义获取变电站编号
      * @param sparkSession
      * @param bdzList
      * @return
      */
    def getBDZbm(sparkSession:SparkSession,bdzList:Array[Row])={
        val bdzbhMap = scala.collection.mutable.Map[String, String]()
        for (bdzglb <- bdzList) {
            bdzbhMap.put(bdzglb.getAs[String]("xlxdbs"), bdzglb.getAs[String]("bdzbh"))
        }
        val bdzbhBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(bdzbhMap)
        sparkSession.udf.register("getbdzbh", (xlxdbs: String) => {
            bdzbhBroadcast.value.get(xlxdbs).getOrElse(null)
        })

    }


    /**
      * 自定义获取变电站名称
      * @param sparkSession
      * @param bdzList
      * @return
      */
    def getBDZmc(sparkSession:SparkSession,bdzList:Array[Row])={
        val bdzmcMap = scala.collection.mutable.Map[String, String]()
        for (bdzglb <- bdzList) {
            bdzmcMap.put(bdzglb.getAs[String]("xlxdbs"), bdzglb.getAs[String]("bdzmc"))
        }
        val bdzmcBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(bdzmcMap)
        sparkSession.udf.register("getbdzmc", (xlxdbs: String) => {
            bdzmcBroadcast.value.get(xlxdbs).getOrElse(null)
        })
    }

    /**
      * 自定义获取变电站考核表户号
      * @param sparkSession
      * @return
      */
    def getBDZkhbhh(sparkSession:SparkSession)={
        val bdzkhbList: Array[Row] = sparkSession.sql("select xlxdbs,yhbh from bdzkhb").collect()
        val bdzkhbMap = scala.collection.mutable.Map[String, String]()
        for (bdzkhb <- bdzkhbList) {
            val xlxdbs = bdzkhb.getAs[String]("xlxdbs")
            var yhbh = bdzkhb.getAs[String]("yhbh")
            bdzkhbMap.put(xlxdbs, yhbh)
        }
        val bdzkhbBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(bdzkhbMap)
        sparkSession.udf.register("getbdzkhb", (xlxdbs: String) => {
            bdzkhbBroadcast.value.get(xlxdbs).getOrElse(null)
        })
    }

    /**
      * 自定义获取台区考核表户号
      * @param sparkSession
      * @return
      */
    def getTQkhbhh(sparkSession:SparkSession)={
        val tqkhbList: Array[Row] = sparkSession.sql("select tqbs,yhbh from tqkhb").collect()
        val tqkhbMap = scala.collection.mutable.Map[String, String]()
        for (tqkhb <- tqkhbList) {
            val tqbs = tqkhb.getAs[String]("tqbs")
            var yhbh = tqkhb.getAs[String]("yhbh")
            tqkhbMap.put(tqbs, yhbh)
        }
        val tqkhbBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(tqkhbMap)
        sparkSession.udf.register("gettqkhb", (xlxdbs: String) => {
            tqkhbBroadcast.value.get(xlxdbs).getOrElse(null)
        })
    }

    /**
      * 获取地区编码函数
      * @param sparkSession
      * @param zz
      * @return
      */
    def getDQbm(sparkSession:SparkSession,zz:DataFrame)={
        val dqbmList: Array[Row] = zz.collect()
        val dqbmMap = scala.collection.mutable.Map[String, String]()
        for (dqbm <- dqbmList) {
            dqbmMap.put(dqbm.getAs[String]("zzbm"), dqbm.getAs[String]("dqbm"))
        }
        val dqbmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(dqbmMap)
        sparkSession.udf.register("getdqbm", (zzbm: String) => {
            dqbmBroadcast.value.get(zzbm).getOrElse("031200")
        })
    }

    /**
      * 获取规则名称函数
      * @param sparkSession
      * @param xsycgz
      * @return
      */
    def getGZmc(sparkSession:SparkSession,xsycgz:DataFrame)={
        val xsycgzList: Array[Row] = xsycgz.collect()
        val xsycgzMap = scala.collection.mutable.Map[String, String]()
        for (xsycgz <- xsycgzList) {
            xsycgzMap.put(xsycgz.getAs[String]("ycgzbh"), xsycgz.getAs[String]("ycgzmc"))
        }
        val xsycgzBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(xsycgzMap)
        sparkSession.udf.register("getycgzmc", (ycgzbh: String) => {
            xsycgzBroadcast.value.get(ycgzbh).getOrElse(null)
        })
    }



    /**
      * 获取异常规则编号
      * @param sparkSession
      * @param xsycgz
      * @return
      */
    def getYcgzbh(sparkSession:SparkSession,xsycgz:DataFrame)={
        val xsycgzList: Array[Row] = xsycgz.collect()
        val ycgzbhMap = scala.collection.mutable.Map[String, String]()
        for (xsycgz <- xsycgzList) {
            if(xsycgz.getAs[String]("ycgzbh")!=null){
                ycgzbhMap.put(xsycgz.getAs[String]("ycgzbh").substring(0,xsycgz.getAs[String]("ycgzbh").lastIndexOf("-")), xsycgz.getAs[String]("ycgzbh"))
            }
        }
        val ycgzbhBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(ycgzbhMap)
        sparkSession.udf.register("getycgzbh", (ycgz: String) => {
            ycgzbhBroadcast.value.get(ycgz).getOrElse(null)
        })
    }



    /**
      * 获取异常规则分类
      * @param sparkSession
      * @param xsycgz
      * @return
      */
    def getYcgzfl(sparkSession:SparkSession,xsycgz:DataFrame)={
        val xsycgzList: Array[Row] = xsycgz.collect()
        val ycgzflMap = scala.collection.mutable.Map[String, String]()
        for (xsycgz <- xsycgzList) {
            if(xsycgz.getAs[String]("ycgzbh")!=null){
                ycgzflMap.put(xsycgz.getAs[String]("ycgzbh"),xsycgz.getAs[String]("ycyyyjfl"))
            }
        }
        val ycgzflBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(ycgzflMap)
        sparkSession.udf.register("getycgzfl", (ycgzbh: String) => {
            ycgzbh match {
                case null => null
                case _ =>{
                    ycgzflBroadcast.value.get(ycgzbh).getOrElse(null)
                }
            }
        })
    }

    /**
      * 判断是否是一级分类中的信息系统问题  1-是 0-否
      * @param sparkSession
      * @param xsycgz
      * @return
      */
    def decideInfoSystem(sparkSession:SparkSession,xsycgz:DataFrame)={
        val xsycgzList: Array[Row] = xsycgz.collect()
        val ycyyyjflbmMap = scala.collection.mutable.Map[String, String]()
        for (xsycgz <- xsycgzList) {
            ycyyyjflbmMap.put(xsycgz.getAs[String]("ycgzbh"), xsycgz.getAs[String]("ycyyyjflbm"))
        }
        val ycyyyjflbmBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(ycyyyjflbmMap)
        sparkSession.udf.register("isXxxtwt", (ycgzbh: String) => {
            ycgzbh match {
                case null => 0
                case _ =>{
                    val ycyyyjflbm = ycyyyjflbmBroadcast.value.get(ycgzbh)
                    if("Constant.XXXTBH".equals(ycyyyjflbm)){
                        1
                    }else{
                        0
                    }
                }
            }
        })

    }

    /**
      * 获取规则参数函数
      * @param sparkSession
      * @param xsycgz
      * @return
      */
    def getGzParameter(sparkSession:SparkSession,xsycgz:DataFrame)={
        val xsycgzList: Array[Row] = xsycgz.collect()
        val ycgzbhMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String,String]]()

        for (xsycgz <- xsycgzList) {
            val ycgzbh = xsycgz.getAs[String]("ycgzbh")
            if(ycgzbhMap.get(ycgzbh).getOrElse(null) == null){
                val gzcsMap = scala.collection.mutable.Map[String, String]()
                ycgzbhMap.put(ycgzbh,gzcsMap)
            }
            if(xsycgz.getAs[String]("ycyyyjfl") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyyyjfl", xsycgz.getAs[String]("ycyyyjfl"))
            }
            if(xsycgz.getAs[String]("ycyyyjflbm") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyyyjflbm", xsycgz.getAs[String]("ycyyyjflbm"))
            }
            if(xsycgz.getAs[String]("ycyyejfl") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyyejfl", xsycgz.getAs[String]("ycyyejfl"))
            }
            if(xsycgz.getAs[String]("ycyyejflbm") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyyejflbm", xsycgz.getAs[String]("ycyyejflbm"))
            }
            if(xsycgz.getAs[String]("ycyysjfl") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyysjfl", xsycgz.getAs[String]("ycyysjfl"))
            }
            if(xsycgz.getAs[String]("ycyysjflbm") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyysjflbm", xsycgz.getAs[String]("ycyysjflbm"))
            }
            if(xsycgz.getAs[String]("ycyyyjfl_tree") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyyyjfl_tree", xsycgz.getAs[String]("ycyyyjfl_tree"))
            }
            if(xsycgz.getAs[String]("ycyyyjflbm_tree") != null){
                ycgzbhMap.get(ycgzbh).get.put("ycyyyjflbm_tree", xsycgz.getAs[String]("ycyyyjflbm_tree"))
            }
        }
        val ycgzbhBroadcast: Broadcast[mutable.Map[String, scala.collection.mutable.Map[String,String]]] = sparkSession.sparkContext.broadcast(ycgzbhMap)
        sparkSession.udf.register("getycgzcs", (ycgzbh: String,ycgzcs:String) => {
            if(ycgzbh == null){
                if("ycyyyjfl".equals(ycgzcs) || "ycyyejfl".equals(ycgzcs) || "ycyysjfl".equals(ycgzcs)){
                    "原因不明"
                }else{
                    "700"
                }
            }else{
                ycgzbhBroadcast.value.get(ycgzbh).get.get(ycgzcs).getOrElse(null)
            }

        })

    }

    /**
      * 2020.02.21  获取业务类别代码
      * @param sparkSession
      * @return
      */
    def getYwlb(sparkSession:SparkSession)={
        val ywlbList: Array[Row] = sparkSession.sql("select ywbm,ywbmmc from ywjl").collect()
        val ywlbMap = scala.collection.mutable.Map[String, String]()
        for (ywlb <- ywlbList) {
            ywlbMap.put(ywlb.getAs[String]("ywbm"), ywlb.getAs[String]("ywbmmc"))
        }
        val ywlbBroadcast: Broadcast[mutable.Map[String, String]] = sparkSession.sparkContext.broadcast(ywlbMap)
        sparkSession.udf.register("getywlbmc", (ywlb: String) => {
            ywlbBroadcast.value.get(ywlb).getOrElse(null)
        })

    }


}
