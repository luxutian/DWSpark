package Test.util

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadMysql {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext

        val url = "jdbc:mysql://localhost:3306/0722"
        val table = "tbl_employee"
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "root")

        val df = sparkSession.read.jdbc(url,table,props)
        df.show()




    }

}
