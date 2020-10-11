package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.SparkSession

/**
  * 查数
  */
object ShowDatas { // TODO: 查询语句全部要改
    def showDatas(sparkSession: SparkSession)={
        sparkSession.sql(
            s"""
               |select *
               |from (
               |    select '线损异常线路和台区异常明细',fqrq,count(*)
               |    from gpsx_sxxm.gk_xsycxlhtqycmx
               |    where length(fqrq)=6
               |    group by fqrq
               |    order by fqrq desc
               |    limit 30
               |    )
               |
               |union all
               |select *
               |from (
               |    select
               |        '线损异常线路和台区明细',fqrq,count(*)
               |    from gpsx_sxxm.hz_xsycxlhtqmx
               |    where length(fqrq)=6
               |    group by fqrq
               |    order by fqrq desc
               |    limit 30 )
               |
               |union all
               |select *
               |from (
               |    select '线损异常原因分布情况统计',fqrq,count(*)
               |    from gpsx_sxxm.GK_XSYCYYFBQKTJ
               |    where length(fqrq)=6
               |    group by fqrq
               |    order by fqrq desc
               |    limit 30
               |    )
               |
               |union all
               |select *
               |from (
               |    select '线损异常原因分类统计',fqrq,count(*)
               |    from gpsx_sxxm.GK_XSYCYYFLTJ
               |    where length(fqrq)=6
               |    group by fqrq
               |    order by fqrq desc
               |    limit 30
               |    )
               |
               |union all
               |select *
               |from (
               |    select '线损线路和台区统计',fqrq,count(*)
               |    from gpsx_sxxm.GK_XSXLHTQTJ
               |    where length(fqrq)=6
               |    group by fqrq
               |    order by fqrq desc
               |    limit 30
               |    )
               |
               |union all
               |select * from (select '线损供电所统计',fqrq,count(*) from gpsx_sxxm.GK_XSGDSTJ where length(fqrq)=6 group by fqrq order by fqrq desc limit 30   )
               |union all
               |select * from (select '线路指标管理',fqrq,count(*) from gpsx_sxxm.gk_xlzbgl where length(fqrq)=6 group by fqrq order by fqrq desc limit 30 )
               |union all
               |select * from (select '台区指标管理',fqrq,count(*) from gpsx_sxxm.gk_tqzbgl where length(fqrq)=6 group by fqrq order by fqrq desc limit 30 )
               |union all
               |select * from (select '各单位异常消缺',fqrq,count(*) from gpsx_sxxm.hz_gdwycxqqk where length(fqrq)=6 group by fqrq order by fqrq desc limit 30)
               |
               |
             """.stripMargin).show(300)
    }

}
