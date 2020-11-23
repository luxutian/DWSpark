package dw_BM_get.controller

import dw_BM_get.service.ProcessDatasService
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StartController {
    def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setAppName("StartController")
          .set("spark.dynamicAllocation.enabled","false")
//          .set("spark.sql.shuffle.partitions", "100")
        //          .setMaster("local[*]")
        val sparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
        val sparkContext = sparkSession.sparkContext

        val url = "10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"
        val kuduContext = new KuduContext(url, sparkContext)

        ProcessDatasService.etlDateService(sparkSession,url)
        //kuduContext.updateRows()

    }

}
/*
spark2-submit \
--master yarn \
--deploy-mode client \
--driver-memory 2g \
--driver-cores 1 \
--num-executors 4 \
--executor-cores 2 \
--executor-memory 4g \
--class dw_BM_get.controller.StartController /home/xiansun/jars/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar >> \
/home/xiansun/logs/test.log 2>&1

--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/xiansun/kudu/log4j.properties" \

  */