package com.GDNanWangLineLoss.month.util
import com.GDNanWangLineLoss.month.bean.Variables._
import org.apache.spark.sql.SparkSession

/**
  * 清除回流表当月数据
  * 还有些问题没解决，   kudu怎么清空表中的数据，没有看到清除表数据的操作
  */
object ClearBackflowTables {

    private val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    //val table ="impala::csg_ods_yx.hs_bsbz"
    private val  url ="10.92.208.217:7051,10.92.208.218:7051,10.92.208.220:7051"
//    val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> table)
//    val bsbz1=sparkSession.read.options(kuduMap).format("org.apache.kudu.spark.kudu").load()


    /**
      * 为了防止程序写到一半挂了有残留数据，
      */
    def dropBackflowTables()={

        //线路1.1
        dropfq("gk_yxlwcjyhqd")  //gk_yxlwcjyhqd
        //线路1.2
        dropfq("gk_bmpyyhqd")
        //线路1.3
        dropfq("gk_sgcbyhqd")
        //线路1.4
        dropfq("gk_yxyjlbmbyzqd")
        //线路1.5
        dropfq("gk_dlycbdyhqd")
        //线路1.6
        dropfq("gk_dlzbyclsdlqd")
        //线路1.7
        dropfq("gk_dltbyhqd")
        //线路1.8
        dropfq("gk_wtjdfdchdlqd")
        //线路1.9
        dropfq("gk_xsycdgbtqqd")
        //线路1.10
        dropfq("gk_fsxlbszbdlpdzbkhqd")
        //线路1.11
        dropfq("gk_fzqxjfdlyhqd")
        //台区1.1
        dropfq("gk_ytqwcjyhqd")
        //台区1.2
        dropfq("gk_bmpyyhqd")
        //台区1.3
        dropfq("gk_sgcbyhqd")
        //台区1.4
        dropfq("gk_yxyjlbmbyzqd")
        //台区1.5
        dropfq("gk_dlycbdyhqd")
        //台区1.6
        dropfq("gk_dlzbyclsdlqd")
        //台区1.7
        dropfq("gk_dltbyhqd")
        //台区1.8
        dropfq("gk_wtjdfdchdlqd")
        //台区1.9
        dropfq("gk_fzqxjfdlyhqd")
        //线路2.1
        dropfq("gk_cshgddwbgqd")
        //线路2.2
        dropfq("gk_gddwwkhbgqd")
        //线路2.3
        dropfq("gk_dfdchdydjbgqd")
        //线路2.4
        dropfq("gk_jldxxdassxlwkqd")
        //线路2.5
        dropfq("gk_yxljlddayyhdabyzqd")
        //线路2.6
        dropfq("gk_gisxthyxxtdydbyzqd")
        //线路2.7
        dropfq("gk_gfshssxlbyzqd")
        //线路2.8
        dropfq("gk_zgdyxfsqzsjbyzqd")
        //线路2.9
        dropfq("gk_dabgdzhyjbjsqd")
        //线路2.10
        dropfq("gk_khbfxygdkjkdqsqd")
        //线路2.11
        dropfq("gk_yxztydlbppqd")
        //线路2.12
        dropfq("gk_jlfzpdyhlbycqd")
        //线路2.13
        dropfq("gk_jlykgdfyqd")
        //台区2.1
        dropfq("gk_cshgddwbgqd")
        //台区2.2
        dropfq("gk_gddwwkhbgqd")
        //台区2.3
        dropfq("gk_dfdchdydjbgqd")
        //台区2.4
        dropfq("gk_gfyhlbywqd")
        //台区2.5
        dropfq("gk_cbzqydlbppyhqd")
        //台区2.6
        dropfq("gk_jldxxdassxlwkqd")
        //台区2.7
        dropfq("gk_yxljlddayyhdabyzqd")
        //台区2.8
        dropfq("gk_gisxthyxxtdydbyzqd")
        //台区2.9
        dropfq("gk_cbqdbyzyhqd")
        //台区2.10
        dropfq("gk_sstqyjzqzdgxbyzyhqd")
        //台区2.11
        dropfq("gk_khbfxygdkjkdqsqd")
        //台区2.12
        dropfq("gk_yxztydlbppqd")
        //台区2.13
        dropfq("gk_dabgdzhyjbjsqd")
        //台区2.14
        dropfq("gk_dabgdzhyjbjsqd")
        //台区2.15
        dropfq("gk_jlykgdfyqd")
        //线路3.1
        dropfq("gk_yqzxlgsdjljdwcqd")
        //线路3.2
        dropfq("gk_gdgdbjsyhdlbdycqd")
        //线路3.3
        dropfq("gk_bmlrcwgdqd")
        //线路3.4
        dropfq("gk_jldzgdyhdlbdycqd")
        //线路3.5
        dropfq("gk_jlfzgdyhdlbdycqd")
        //线路3.6
        dropfq("gk_bdlycyhdlqd")
        //台区3.1
        dropfq("gk_yqzxlgsdjljdwcqd")
        //台区3.2
        dropfq("gk_gdgdbjsyhdlbdycqd")
        //台区3.3
        dropfq("gk_bmlrcwgdqd")
        //台区3.4
        dropfq("gk_bdlycyhdlqd")
        //台区3.5
        dropfq("gk_bdlycyhdlqd")
        //线路4.1
        dropfq("gk_glyspdzgbqd")
        //线路4.2
        //dropfq("gk_zgzqzqd")
        //线路4.3
        //dropfq("gk_zgzqzqd")
        //线路4.4
        //dropfq("gk_fdcbgdqd")
        //线路4.5
        dropfq("gk_gsgybyqqd")
        //线路4.6
        dropfq("gk_fhqggzdqd")
        //线路4.7
        dropfq("gk_gbbsqd")
        //线路4.8
        //dropfq("gk_gdbjqd")
        //线路4.9
        dropfq("gk_zhxlfzqd")
        //线路4.10
        dropfq("gk_sjhwwzhxlqd")
        //台区4.1
        dropfq("gk_sxfhbphqd")
        //台区4.2
        //dropfq("gpsx_sxxm.gk_gdbjqd")
        //台区4.3
        dropfq("gk_zgzqzqd")
        //台区4.4
        dropfq("gk_zgzqzqd")
        //台区4.5
        //dropfq("gpsx_sxxm.gk_fdcbgdqd")
        //台区4.6
        dropfq("gk_dnbshqd")
        //线路5.1
        dropfq("gk_wqgzdqd")
        //线路5.2
        dropfq("gk_xbykhdlqd")
        //线路5.3
        dropfq("gk_glyspdzgbqd")
        //台区5.1
        dropfq("gk_wqgzdqd")
        //台区5.2
        dropfq("gk_glyspdzgbqd")
        //线路6.1
        dropfq("gk_xszbwkxlqd")
        //台区6.1
        dropfq("gk_xszbwkxlqd")

        dropfq("ycgddwxlgx")
        dropfq("gk_xsycxlhtqycmx")
        dropfq("hz_xsycxlhtqmx")
        dropfq("gk_xsycyyfbqktj")
        dropfq("gk_xsycyyfltj")
        dropfq("gk_xsxlhtqtj")
        dropfq("gk_xsgdstj")
        dropfq("gk_xlzbgl")
        dropfq("gk_tqzbgl")
        dropfq("hz_xslxycxltq_his")
        dropfq("hz_xslxycxltq_xq")

    }


    private def dropfq(tablename:String):Unit={

        val kuduMap: Map[String, String] = Map[String,String]("kudu.master" -> url,"kudu.table" -> tablename)
        val size=sparkSession.read.options(kuduMap)
          .format("org.apache.kudu.spark.kudu")
          .load().collect.size

                // val size = sparkSession.sql(s"select * from ${writeSchema}.${tablename} where fqrq=${nowMonth} limit 1")

        // TODO: kudu怎么清空表中的数据，没有看到清除表数据的操作
        if(size>0){
            sparkSession.sql(s"alter table ${writeSchema}.${tablename} drop partition(fqrq=${nowMonth})")
        }

    }



}
