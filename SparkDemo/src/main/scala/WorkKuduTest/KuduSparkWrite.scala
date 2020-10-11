package WorkKuduTest

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object KuduSparkWrite {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")

        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext
        val kuduContext = new KuduContext("cdh112:7051",ssc)
        //1 先读取kudu数据

            //1.1 先判断表是否存在
            val flag1 = kuduContext.tableExists("spark_kudu")
//        if(flag1){
            val kuduMap = Map("kudu.master" ->"cdh112:7051","kudu.table"->"spark_kudu")

            val df = sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()
            df.show()
//        }

        import sparkSession.implicits._
        //2 往表里追加数据
        val flag2 = kuduContext.tableExists("spark_kudu")
         if(flag2){

            val rows = Seq(("1","lu",12,"1"),("2","zhang",15,"1"),("3","wang",18,"1"),("4","li",20,"0"))
            val dff = ssc.makeRDD(rows).toDF("id","name","age","sex")
            kuduContext.insertIgnoreRows(dff,"spark_kudu")
        }

//        //3 修改表数据
//
//        val seq= Seq(("4","li",30,"0"))
//        val alterData = ssc.makeRDD(seq).toDF("id","name","age","sex")
//        kuduContext.updateRows(alterData,"spark_kudu")
//        df.show() //age 已发生改变

        //4 删除数据 (好像不给删数据)
        // 删除数据只需指定主键
        val df2 = df.where("id=2").select("id")
//        val seq2= Seq(("2"))
//        val alterData2 = ssc.makeRDD(df2).toDF("id","name","age","sex")
        kuduContext.deleteRows(df2,"spark_kudu")
        df.show()
    }

    //往表里追加数据
    def insertData(sparkSession:SparkSession,ssc: SparkContext, kuduContext: KuduContext, tableName: String)={
        import sparkSession.implicits._
        val rows = Seq(("1","lu",12,"1"),("2","zhang",15,"1"),("3","wang",18,"1"),("4","li",20,"0"))
        val rows2 = ssc.makeRDD(rows).toDF("id","name","age","sex")
        kuduContext.insertIgnoreRows(rows2,tableName)

    }

    //删除表格
    def deleteTable(kuduContext: KuduContext, tableName: String)={
        kuduContext.deleteTable(tableName)
        println("删除成功："+ tableName)

    }

    //删除数据
    def deleteData(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String,
                   kuduContext: KuduContext, tableName: String)={
        import sparkSession.implicits._

        val kuduMap = Map("kudu.master" ->"cdh112:7051","kudu.table"->"spark_kudu")
        val data = List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40))
        val dataFrame = sc.parallelize(data).toDF("id","name","age")
        val deleteRow = dataFrame.where("id=2").select("id")
        kuduContext.deleteRows(deleteRow,tableName)

    }


}
