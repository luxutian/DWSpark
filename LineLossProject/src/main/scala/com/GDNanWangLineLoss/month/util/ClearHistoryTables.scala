package com.GDNanWangLineLoss.month.util

import org.apache.spark.sql.SparkSession
import com.GDNanWangLineLoss.month.bean.Variables._

/**
  * 清除历史表当月数据
  */
object ClearHistoryTables {

    private val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    private val  url ="10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"

    def dropHistoryTable()={

        //线路1.1
        dropfq("gk_yxlwcjyhqd_his")
        //线路1.2
        dropfq("gk_bmpyyhqd_his")
        //线路1.3
        dropfq("gk_sgcbyhqd_his")
        //线路1.4
        dropfq("gk_yxyjlbmbyzqd_his")
        //线路1.5
        dropfq("gk_dlycbdyhqd_his")
        //线路1.6
        dropfq("gk_dlzbyclsdlqd_his")
        //线路1.7
        dropfq("gk_dltbyhqd_his")
        //线路1.8
        dropfq("gk_wtjdfdchdlqd_his")
        //线路1.9
        dropfq("gk_xsycdgbtqqd_his")
        //线路1.10
        dropfq("gk_fsxlbszbdlpdzbkhqd_his")
        //线路1.11
        dropfq("gk_fzqxjfdlyhqd_his")
        //台区1.1
        dropfq("gk_ytqwcjyhqd_his")
        //台区1.2
        dropfq("gk_bmpyyhqd_his")
        //台区1.3
        dropfq("gk_sgcbyhqd_his")
        //台区1.4
        dropfq("gk_yxyjlbmbyzqd_his")
        //台区1.5
        dropfq("gk_dlycbdyhqd_his")
        //台区1.6
        dropfq("gk_dlzbyclsdlqd_his")
        //台区1.7
        dropfq("gk_dltbyhqd_his")
        //台区1.8
        dropfq("gk_wtjdfdchdlqd_his")
        //台区1.9
        dropfq("gk_fzqxjfdlyhqd_his")
        //线路2.1
        dropfq("gk_cshgddwbgqd_his")
        //线路2.2
        dropfq("gk_gddwwkhbgqd_his")
        //线路2.3
        dropfq("gk_dfdchdydjbgqd_his")
        //线路2.4
        dropfq("gk_jldxxdassxlwkqd_his")
        //线路2.5
        dropfq("gk_yxljlddayyhdabyzqd_his")
        //线路2.6
        dropfq("gk_gisxthyxxtdydbyzqd_his")
        //线路2.7
        dropfq("gk_gfshssxlbyzqd_his")
        //线路2.8
        dropfq("gk_zgdyxfsqzsjbyzqd_his")
        //线路2.9
        dropfq("gk_dabgdzhyjbjsqd_his")
        //线路2.10
        dropfq("gk_khbfxygdkjkdqsqd_his")
        //线路2.11
        dropfq("gk_yxztydlbppqd_his")
        //线路2.12
        dropfq("gk_jlfzpdyhlbycqd_his")
        //线路2.13
        dropfq("gk_jlykgdfyqd_his")
        //台区2.1
        dropfq("gk_cshgddwbgqd_his")
        //台区2.2
        dropfq("gk_gddwwkhbgqd_his")
        //台区2.3
        dropfq("gk_dfdchdydjbgqd_his")
        //台区2.4
        dropfq("gk_gfyhlbywqd_his")
        //台区2.5
        dropfq("gk_cbzqydlbppyhqd_his")
        //台区2.6
        dropfq("gk_jldxxdassxlwkqd_his")
        //台区2.7
        dropfq("gk_yxljlddayyhdabyzqd_his")
        //台区2.8
        dropfq("gk_gisxthyxxtdydbyzqd_his")
        //台区2.9
        dropfq("gk_cbqdbyzyhqd_his")
        //台区2.10
        dropfq("gk_sstqyjzqzdgxbyzyhqd_his")
        //台区2.11
        dropfq("gk_khbfxygdkjkdqsqd_his")
        //台区2.12
        dropfq("gk_yxztydlbppqd_his")
        //台区2.13
        dropfq("gk_dabgdzhyjbjsqd_his")
        //台区2.14
        dropfq("gk_dabgdzhyjbjsqd_his")
        //台区2.15
        dropfq("gk_jlykgdfyqd_his")
        //线路3.1
        dropfq("gk_yqzxlgsdjljdwcqd_his")
        //线路3.2
        dropfq("gk_gdgdbjsyhdlbdycqd_his")
        //线路3.3
        dropfq("gk_bmlrcwgdqd_his")
        //线路3.4
        dropfq("gk_jldzgdyhdlbdycqd_his")
        //线路3.5
        dropfq("gk_jlfzgdyhdlbdycqd_his")
        //线路3.6
        dropfq("gk_bdlycyhdlqd_his")
        //台区3.1
        dropfq("gk_yqzxlgsdjljdwcqd_his")
        //台区3.2
        dropfq("gk_gdgdbjsyhdlbdycqd_his")
        //台区3.3
        dropfq("gk_bmlrcwgdqd_his")
        //台区3.4
        dropfq("gk_bdlycyhdlqd_his")
        //台区3.5
        dropfq("gk_bdlycyhdlqd_his")
        //线路4.1
        dropfq("gk_glyspdzgbqd_his")
        //线路4.2
        //dropfq("gk_zgzqzqd_his")
        //线路4.3
        //dropfq("gk_zgzqzqd_his")
        //线路4.4
        //dropfq("gk_fdcbgdqd_his")
        //线路4.5
        dropfq("gk_gsgybyqqd_his")
        //线路4.6
        dropfq("gk_fhqggzdqd_his")
        //线路4.7
        dropfq("gk_gbbsqd_his")
        //线路4.8
        //dropfq("gk_gdbjqd_his")
        //线路4.9
        dropfq("gk_zhxlfzqd_his")
        //线路4.10
        dropfq("gk_sjhwwzhxlqd_his")
        //台区4.1
        dropfq("gk_sxfhbphqd_his")
        //台区4.2
        //dropfq("gpsx_sxxm.gk_gdbjqd_his")
        //台区4.3
        dropfq("gk_zgzqzqd_his")
        //台区4.4
        dropfq("gk_zgzqzqd_his")
        //台区4.5
        //dropfq("gpsx_sxxm.gk_fdcbgdqd_his")
        //台区4.6
        dropfq("gk_dnbshqd_his")
        //线路5.1
        dropfq("gk_wqgzdqd_his")
        //线路5.2
        dropfq("gk_xbykhdlqd_his")
        //线路5.3
        dropfq("gk_glyspdzgbqd_his")
        //台区5.1
        dropfq("gk_wqgzdqd_his")
        //台区5.2
        dropfq("gk_glyspdzgbqd_his")
        //线路6.1
        dropfq("gk_xszbwkxlqd_his")
        //台区6.1
        dropfq("gk_xszbwkxlqd_his")

        dropfq("ycgddwxlgx")
        dropfq("gk_xsycxlhtqycmx_his")
        dropfq("hz_xsycxlhtqmx_his")
        dropfq("gk_xsycyyfbqktj_his")
        dropfq("gk_xsycyyfltj_his")
        dropfq("gk_xsxlhtqtj_his")
        dropfq("gk_xsgdstj_his")
        dropfq("gk_xlzbgl_his")
        dropfq("gk_tqzbgl_his")
        dropfq("hz_xslxycxltq_his")
        dropfq("hz_xslxycxltq_xq")

    }

    private def dropfq(tablename:String):Unit={

        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> tablename)
        val size=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load().collect.size

        // val size = sparkSession.sql(s"select * from ${writeSchema}.${tablename} where fqrq=${nowMonth} limit 1")

        // TODO: kudu怎么清空表中的数据，没有看到清除表数据的操作
        if(size>0){
            sparkSession.sql(s"alter table ${writeSchema}.${tablename} drop partition(fqrq=${nowMonth})")
        }
    }


}
