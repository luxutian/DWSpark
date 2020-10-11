package Demo

import java.util

import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{SparkSession, types}

object CreateTable {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("createTable").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val ssc = sparkSession.sparkContext

        //1 创建kudu数据库连接
        val kudumasteraddr="hadoop102,hadoop103,hadoop104"
        val kuduContext: KuduContext = new KuduContext(kudumasteraddr,ssc)

        //2 调用方法操作
        createTable(kuduContext)

    }
    private def createTable(kuduContext:KuduContext)={
        //1 定义表名
        val tablename ="test_kudu"
        //2 定义表的schema
        val schema = types.StructType(
            StructField("id", StringType, false) ::
          StructField("name", StringType, false) ::
          StructField("age", StringType, false) :: Nil //Nil 空List   id,name,age
        )
        //3 定义表的主键
        val key = Seq("id")

        //4 设置表的属性
        val options = new CreateTableOptions
                //主键，分区，schema 副本数
        val partititon = new util.LinkedList[String]()
        partititon.add("id")
        partititon.add("name")
        options.addHashPartitions(partititon,3)

        try{
            if (!kuduContext.tableExists(tablename)){
                kuduContext.createTable(tablename,schema,key,options)
            }
        } catch {
            case e:Exception => e.printStackTrace()
        } finally {

        }
    }

}
