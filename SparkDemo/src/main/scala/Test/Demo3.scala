package Test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: panzibin
  */
object Demo3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().set("spark.sql.shuffle.partitions", "100").set("spark.dynamicAllocation.enabled", "false")
          .setAppName("Demo").setMaster("local[4]")
        val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
        import org.apache.kudu.client._
        import org.apache.kudu.spark.kudu.KuduContext
        import collection.JavaConverters._
        val url = "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"//变电站

        // TODO: spark读 kudu 数据
        val bdz = sparkSession.read
          .options(Map("kudu.master" -> url, "kudu.table" -> "ods_yx.dw_bdz"))
          .format("org.apache.kudu.spark.kudu").load
        bdz.createOrReplaceTempView("_bdz")
        bdz.show()
    }
}
