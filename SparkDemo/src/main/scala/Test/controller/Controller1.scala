package Test.controller

import java.util.Properties

import Test.service.Service
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Controller1 {


    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext
//        val kuduContext = new KuduContext("cdh102:7051",ssc)

//        val df = sparkSession.read.options(Map("kudu.master" -> "hadoop102:7051,hadoop103:7051", "kudu.table" -> "test_table"))
//          .format("org.apache.kudu.spark.kudu").load()

        val url = "jdbc:mysql://localhost:3306/0722"
        val table = "tbl_employee"
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "root")

        Service.receiveData(sparkSession,url,table,props)




    }


}
