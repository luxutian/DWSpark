package Test.service

import java.util.Properties

import Test.dao.Dao
import org.apache.spark.sql.SparkSession

object Service {

    def receiveData(sparkSession :SparkSession, url:String , table:String, props:Properties)={

         Dao.readmysql(sparkSession,url,table,props)
//        result1.show()

        val result2 =sparkSession.sql(
            s"""
               |select *
               |from _re
             """.stripMargin)
        result2.show()





    }


}
