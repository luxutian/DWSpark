package Test.dao

import java.util.Properties

import org.apache.spark.sql.SparkSession

object Dao {

    import Test.bean.Variable._
    def readmysql(sparkSession:SparkSession,url:String,table:String,props:Properties)={

        val result1 = sparkSession.read.jdbc(url, table, props)
        result1.createOrReplaceTempView("_re")
        result1.show()
        val re2 =sparkSession.sql(
            s"""
               |select * from _re where id= ${id}
             """.stripMargin)
        re2.createOrReplaceTempView("_re")
        //sparkSession.catalog.dropTempView("_re")

    }

}
