package com.GDNanWangLineLoss.month.service

import com.GDNanWangLineLoss.month.dao.OdsMonthDao
import org.apache.spark.sql.SparkSession

object DataSourceTest {
    def receiverData(sparkSession:SparkSession,url:String)={

        //1 调用Dao层形成临时表
        /*---------------1到10个数据源-------------------------------------------------------*/
        OdsMonthDao.getCbxxData(sparkSession,url)
        OdsMonthDao.getYdkhData(sparkSession,url)
        OdsMonthDao.getJldData(sparkSession,url)
        OdsMonthDao.getAll_xlxdData(sparkSession,url)
        OdsMonthDao.getAll_tpData(sparkSession,url)
        OdsMonthDao.getXltqgxData(sparkSession,url)
        OdsMonthDao.getYhdyxxData(sparkSession,url)
        OdsMonthDao.getYxbyqData(sparkSession,url)
        OdsMonthDao.getJlddnbgxData(sparkSession,url)
        OdsMonthDao.getYxdnbData(sparkSession,url)
        println("无忧助手提示：1到10个数据源连接ok")

        /*---------------11到20个数据源-------------------------------------------------------*/

        OdsMonthDao.getDnbcsData(sparkSession,url)
        OdsMonthDao.getJldxx_allData(sparkSession,url)
        OdsMonthDao.getJldxxData(sparkSession,url)
        OdsMonthDao.getTbdlxxData(sparkSession,url)
        OdsMonthDao.getCztbData(sparkSession,url)
        OdsMonthDao.getGfyhdaxxData(sparkSession,url)
        OdsMonthDao.getGfyhglxxData(sparkSession,url)
        OdsMonthDao.getXlbzxxData(sparkSession,url)
        OdsMonthDao.getTqbzxxData(sparkSession,url)
        OdsMonthDao.getWqxxData(sparkSession,url)
        println("无忧助手提示：11到20个数据源连接ok")












        /*-------------------------------------------------------------------*/



    }


}
