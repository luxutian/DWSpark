package com.GDNanWangLineLoss.month.util


import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map   //todo 可变的集合需要导包  默认为不可变集合

/**
  * 依赖统计
  */
object DependentStatistics {

    import com.GDNanWangLineLoss.month.util.Function.zzList
    def dependentStatistics(sparkSession:SparkSession)={

        /*
        var map4 =Map( ("A", 1), ("B", "北京"), ("C", 3) )
        map4.put("A",3)
        */




/*-----------------------------------------------------------------------*/
        val zzlxdmList= ListBuffer[String]()
        for(row <- zzList){  //zzList 在类Function
            val zzlxdm = row.getAs[String]("zzlxdm")  //组织类型代码
            val zzbm = row.getAs[String]("zzbm")  //组织编码
            if("3".equals(zzlxdm) || "4".equals(zzlxdm) || "5".equals(zzlxdm)){
                zzlxdmList.append(zzbm)
            }
        }


        val structType1 = StructType(List[StructField](
            StructField("gddwbm", StringType, true),  //供电单位编码
            StructField("sl", LongType, true)  //
        ))

        /*---------下面的有限变量出现多次，后添加 _1\_2 加以区别----------------------------------------------------------*/
        //线路总数
        val xlzs_1 = s"select gddwbm,count(1) sl from xlxstjxx where ny = ${nowMonth} group by gddwbm"
        val xlzsArray_1= sparkSession.sql(xlzs_1).collect()


        val xlzsMap_1 = Map[String,Map[String,Long]]()
        updateSpeMap(xlzsArray_1,xlzsMap_1,"gddwbm","sl")

        val xlzsIterable_1= xlzsMap_1.map(data=>(data._1,data._2.get("sl").getOrElse(0L)))
        val xlzsRDD_1= sparkSession.sparkContext.parallelize(xlzsIterable_1.toSeq).map(data=>(Row(data._1,data._2)))

        sparkSession.createDataFrame(xlzsRDD_1,structType1).createOrReplaceTempView("xlzs") //线路总数
        // spark.sql("select * from xlzs").show(100)


        //台区总数
        val tqzs_1 = s"select gddwbm,count(1) sl from tqxstjxx where ny = ${nowMonth} group by gddwbm"
        val tqzsArray_1:Array[Row] = sparkSession.sql(tqzs_1).collect()
        val tqzsMap_1 = Map[String,Map[String,Long]]()
        updateSpeMap(tqzsArray_1,tqzsMap_1 ,"gddwbm","sl")

        val tqzsIterable_1= tqzsMap_1.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val tqzsRDD_1= sparkSession.sparkContext.parallelize(tqzsIterable_1.toSeq).map(data=>(Row(data._1,data._2)))

        sparkSession.createDataFrame(tqzsRDD_1,structType1).createOrReplaceTempView("tqzs")   //台区总数
        // spark.sql("select * from tqzs").show(100)

        /*-------------------------------------------------------------------*/

        //线路异常总数
        val xlyczs_1 = s"select gddwbm,count(1) sl from xsycxlhtqmx where ny = ${nowMonth} and xltqbz = ${xlbz} group by gddwbm"
        val xlyczsArray_1:Array[Row] = sparkSession.sql(xlyczs_1).collect()
        val xlyczsMap_1 = Map[String,Map[String,Long]]()
        updateSpeMap(xlyczsArray_1,xlyczsMap_1 ,"gddwbm","sl")

        val xlyczsIterable_1= xlyczsMap_1.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val xlyczsRDD_1= sparkSession.sparkContext.parallelize(xlyczsIterable_1.toSeq).map(data=>(Row(data._1,data._2)))

        sparkSession.createDataFrame(xlyczsRDD_1,structType1).createOrReplaceTempView("xlgddwyczs")//线路异常总数
        // spark.sql("select * from xlyczs").show(100)


        //台区异常总数
        val tqyczs_1 = s"select gddwbm,count(1) sl from xsycxlhtqmx where ny = ${nowMonth} and xltqbz = ${tqbz} group by gddwbm"
        val tqyczsArray_1:Array[Row] = sparkSession.sql(tqyczs_1).collect()
        val tqyczsMap_1 = Map[String,Map[String,Long]]()
        updateSpeMap(tqyczsArray_1,tqyczsMap_1 ,"gddwbm","sl")

        val tqyczsIterable_1 = tqyczsMap_1.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val tqyczsRDD_1= sparkSession.sparkContext.parallelize(tqyczsIterable_1.toSeq).map(data=>(Row(data._1,data._2)))

        sparkSession.createDataFrame(tqyczsRDD_1,structType1).createOrReplaceTempView("tqgddwyczs")//台区异常总数
        // spark.sql("select * from tqyczs").show(100)



        //总数，异常总数，异常率
        sparkSession.sql(
            s"""
               |select
               |    z.gddwbm,y.sl yczs,z.sl zs,
               |    (case when (z.sl=0 or z.sl is null) then 0 else ifnull(y.sl,0)/z.sl end)*100 ycl,
               |    ${xlbz} xltqbz
               |from xlgddwyczs y  --线路异常总数
               |join xlzs z on y.gddwbm = z.gddwbm  --线路总数
               |union
               |select
               |    z.gddwbm,y.sl yczs,z.sl zs,
               |    (case when (z.sl=0 or z.sl is null) then 0 else ifnull(y.sl,0)/z.sl end)*100 ycl,
               |    ${tqbz} xltqbz
               |from tqgddwyczs y  --台区异常总数
               |join tqzs z on y.gddwbm = z.gddwbm  --台区总数
               |
             """.stripMargin).createOrReplaceTempView("zs_yczs_ycl")

        /*-------下面是添加_2 ------------------------------------------------------------*/

        val structType2 = StructType(List[StructField](
            StructField("gddwbm", StringType, true),
            StructField("ny", StringType, true),
            StructField("sl", LongType, true)
        ))
        //包含年月的线路总数，台区总数，线路异常总数，台区异常总数
        //线路总数
        val xlzs_2 = s"select concat(gddwbm,'_',ny) gddwbm,count(1) sl from xlxstjxx group by gddwbm,ny"
        val xlzsArray_2:Array[Row] = sparkSession.sql(xlzs_2).collect()
        val xlzsMap_2 = Map[String,Map[String,Long]]()
        updateSpeMap(xlzsArray_2,xlzsMap_2,"gddwbm","sl")

        val xlzsIterable_2 = xlzsMap_2.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val xlzsRDD_2= sparkSession.sparkContext.parallelize(xlzsIterable_2.toSeq).map(data=>(Row(data._1.split("_")(0),data._1.split("_")(1),data._2)))
        sparkSession.createDataFrame(xlzsRDD_2,structType2).createOrReplaceTempView("ny_xlzs")  //线路总数(年月)
        // spark.sql("select * from xlzs").show(100)

        //台区总数
        val tqzs_2 = s"select concat(gddwbm,'_',ny) gddwbm,count(1) sl from tqxstjxx group by gddwbm,ny"
        val tqzsArray_2:Array[Row] = sparkSession.sql(tqzs_2).collect()
        val tqzsMap_2 = Map[String,Map[String,Long]]()
        updateSpeMap(tqzsArray_2,tqzsMap_2 ,"gddwbm","sl")

        val tqzsIterable_2= tqzsMap_2.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val tqzsRDD_2= sparkSession.sparkContext.parallelize(tqzsIterable_2.toSeq).map(data=>(Row(data._1.split("_")(0),data._1.split("_")(1),data._2)))
        sparkSession.createDataFrame(tqzsRDD_2,structType2).createOrReplaceTempView("ny_tqzs")  //台区总数(年月)
        // spark.sql("select * from tqzs").show(100)


        //线路异常总数
        val xlyczs_2 = s"select concat(gddwbm,'_',ny) gddwbm,count(1) sl from xsycxlhtqmx where xltqbz = ${xlbz} group by gddwbm,ny"
        val xlyczsArray_2:Array[Row] = sparkSession.sql(xlyczs_2).collect()
        val xlyczsMap_2 = Map[String,Map[String,Long]]()
        updateSpeMap(xlyczsArray_2,xlyczsMap_2 ,"gddwbm","sl")

        val xlyczsIterable_2= xlyczsMap_2.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val xlyczsRDD_2 = sparkSession.sparkContext.parallelize(xlyczsIterable_2.toSeq).map(data=>(Row(data._1.split("_")(0),data._1.split("_")(1),data._2)))
        sparkSession.createDataFrame(xlyczsRDD_2,structType2).createOrReplaceTempView("ny_xlgddwyczs")  //线路异常总数(年月)
        // spark.sql("select * from xlyczs").show(100)



        //台区异常总数
        val tqyczs_2 = s"select concat(gddwbm,'_',ny) gddwbm,count(1) sl from xsycxlhtqmx where xltqbz = ${tqbz} group by gddwbm,ny"
        val tqyczsArray_2:Array[Row] = sparkSession.sql(tqyczs_2).collect()
        val tqyczsMap_2 = Map[String,Map[String,Long]]()
        updateSpeMap(tqyczsArray_2,tqyczsMap_2 ,"gddwbm","sl")

        val tqyczsIterable_2 = tqyczsMap_2.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val tqyczsRDD_2= sparkSession.sparkContext.parallelize(tqyczsIterable_2.toSeq).map(data=>(Row(data._1.split("_")(0),data._1.split("_")(1),data._2)))
        sparkSession.createDataFrame(tqyczsRDD_2,structType2).createOrReplaceTempView("ny_tqgddwyczs")  //台区异常总数(年月)


        //工单归档数 gdgds
        val gdgds =
            s"""
               |select
               |    y.gddwbm gddwbm,count(1) sl
               |from gzdxxls y
               |where substring(y.sqsj,0,7) = '${_nowMonth}' and y.gzdzt = '2'
               |    and y.ywlb in (select ywbm from ywjl where sjywbm = 'MK082')
               |group by y.gddwbm
               |
             """.stripMargin
        val gdgdsArray:Array[Row] = sparkSession.sql(gdgds).collect()
        val gdgdsMap = Map[String,Map[String,Long]]()
        updateSpeMap(gdgdsArray,gdgdsMap ,"gddwbm","sl")

        val gdgdsIterable = gdgdsMap.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val gdgdsRDD = sparkSession.sparkContext.parallelize(gdgdsIterable.toSeq).map(data=>(Row(data._1,data._2)))
        sparkSession.createDataFrame(gdgdsRDD,structType1).createOrReplaceTempView("ycclwcs")
        // spark.sql("select * from ycclwcs").show(100)

        //工单发起数 gdfqs
        val gdfqs =
            s"""
               |select
               |    y.gddwbm gddwbm,count(1) sl
               |from gzdxxls y
               |where substring(y.sqsj,0,7) = '${_nowMonth}'
               |    and y.ywlb in (select ywbm from ywjl where sjywbm = 'MK082')
               |group by y.gddwbm
               |
             """.stripMargin
        val gdfqsArray:Array[Row] = sparkSession.sql(gdfqs).collect()  //sql查询的结构集，数组每个元素就是表格一行元素
        val gdfqsMap = Map[String,Map[String,Long]]()
        updateSpeMap(gdfqsArray,gdfqsMap ,"gddwbm","sl")

        val gdfqsIterable = gdfqsMap.map(data=>(data._1,data._2.get("sl").getOrElse(0l)))
        val gdfqsRDD= sparkSession.sparkContext.parallelize(gdfqsIterable.toSeq).map(data=>(Row(data._1,data._2)))
        sparkSession.createDataFrame(gdfqsRDD,structType1).createOrReplaceTempView("gdfqs")
        // spark.sql("select * from gdfqs").show(100)
        //异常处理完成率   if(y.sl/g.sl is null,1,y.sl/g.sl)  处理发起数和归档数都为0的情况
        sparkSession.sql(
            s"""
               |select
               |    y.gddwbm,case when y.sl/g.sl is null then 1 else y.sl/g.sl end  ycclwcl
               |from ycclwcs y
               |join gdfqs g on y.gddwbm = g.gddwbm
               |
             """.stripMargin).cache().createOrReplaceTempView("ycclwcl")
    }

    /*--------下面为可调用方法-----------------------------------------------------------*/

    /**
      * 获取供电单位编码
      * @param gddwbm
      * @return
      */
    def getSjGddwbm(gddwbm:String):String = {
        if(gddwbm.size<=2){
            "03"
        }else{
            gddwbm.substring(0,gddwbm.size - 2)
        }
    }


    /**
      * 没有数据，不知道想实现什么
      * @param array
      * @param map
      * @param keys
      */
    def updateSpeMap(array:Array[Row],map:Map[String,Map[String,Long]],keys:String*): Unit = { // TODO: 没有样例，不知道这是干什么的
        for(row <- array){
            var first = row.getAs[String](keys(0))  //key(0)字段 gddwbm
            var gddwbm = first.split("_")(0)  //供电单位编码
            var flag = first.equals(gddwbm)
            while(gddwbm != "03"){
                if(map.get(first) == None){
                    map.put(first,Map[String,Long]())

                }
                val flMap = map.get(first).get
                for(i <- 1 until keys.size){  //前闭合后开的范围
                    flMap.put(keys(i),flMap.get(keys(i)).getOrElse(0l) + row.getAs[Long](keys(i)))
                }
                gddwbm = getSjGddwbm(gddwbm)
                first = if(flag) gddwbm else gddwbm + "_" + first.substring(first.indexOf("_") + 1,first.length)
            }

        }
    }

}
