package WorkKuduTest

import java.util

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//这个跑通了
object KuduSparkCreate {

    def main(args: Array[String]): Unit = {
        //构建 sparkConf 对象
        val sparkConf: SparkConf = new SparkConf().setAppName("SparkKuduTest").setMaster("local[2]")
        //构建 SparkSession 对象
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //获取 sparkContext 对象
        val sc: SparkContext = sparkSession.sparkContext
        sc.setLogLevel("warn")

        //构建 KuduContext 对象
        val kuduContext = new KuduContext("cdh112:7051", sc)
        //1.创建表操作
        createTable(kuduContext)


        //2 删除表
       // kuduContext.deleteTable("spark_kudu")

        }

    /**
      * 创建表
      *
      * @param kuduContext
      * @return
      */
    private def createTable(kuduContext: KuduContext)={
        //1.1 定义表名
        val tableName = "spark_kudu"
        //1.2 定义表的 schema
        val schema = types.StructType(
            StructField("id", StringType, false) ::
              StructField("name", StringType, false) ::
              StructField("age", IntegerType, false) ::
              StructField("sex", StringType, false) :: Nil)
        //1.3 定义表的主键
        val primaryKey = Seq("id")
        //1.4 定义分区的 schema
        val options = new CreateTableOptions
        //设置分区
        val  parcols = new util.LinkedList[String]() //链表 插入快，检索慢
        parcols.add("id") ;
        options.setRangePartitionColumns(parcols)
        //设置副本
        options.setNumReplicas(1)
        //1.5 创建表
        if (!kuduContext.tableExists(tableName)) {
            kuduContext.createTable(tableName, schema, primaryKey, options) // TODO: 表名，元数据，主键，属性
        }

    }
}
/*
bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--driver-cores 1 \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 2g \
--class WorkKuduTest.KuduSparkTest1 /home/luxutian/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar
 */