package Demo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkImpala {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SparkImpalaDemo").setMaster("local[2]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()

        val prop = new Properties()
        prop.put("user", "username")
        prop.put("password", "password")
        prop.setProperty("driver", "com.cloudera.impala.jdbc41.Driver")

        val url="jdbc:impala://10.92.208.217:25004;RowsFetchedPerBlock=50000;BATCH_SIZE=50000"
        val sql="select * from t1"
        val df = sparkSession.read.jdbc(url,"table1",prop)
        df.show()


    }

}
