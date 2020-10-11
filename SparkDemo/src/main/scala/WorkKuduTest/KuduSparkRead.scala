package WorkKuduTest

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object KuduSparkRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext
       // val kuduContext = new KuduContext("cdh102:7051",ssc)

        // TODO: kudu表名有大小写的区别
        val df = sparkSession.read.options(Map("kudu.master" -> "cdh112:7051", "kudu.table" -> "impala::ods.KUDU_USER"))  //ctrl+shift+u  大写小写转换
          .format("org.apache.kudu.spark.kudu").load()
        df.show()


    }



}
