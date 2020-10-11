package dw_BM_get.controller

import dw_BM_get.service.ProcessDatasService
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StartController {
    def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setAppName("StartController")
                  .set("spark.sql.shuffle.partitions", "100")
                  .set("spark.dynamicAllocation.enabled","false")
          .setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val sparkContext = sparkSession.sparkContext

        val url = "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"
        val kuduContext = new KuduContext(url, sparkContext)

        ProcessDatasService.etlDateService(sparkSession,url)
        //kuduContext.updateRows()

    }

}
/*
spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--driver-cores 1 \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 2g \
--class xxx    yyyyy





  */