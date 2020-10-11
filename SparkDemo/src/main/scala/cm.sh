#! /bin/bash
case $1 in
"start"){
    echo " =================== 启动 CM集群 ==================="

    echo " --------------- 启动 CM server---------------"
    ssh cdh112 "systemctl start cloudera-scm-server"

    echo " --------------- 启动 impala 103 ---------------"
    ssh cdh112 "systemctl start cloudera-scm-agent"
    ssh cdh113 "systemctl start cloudera-scm-agent"
    ssh cdh114 "systemctl start cloudera-scm-agent"


};;
"stop"){
    echo " =================== 关闭 CM集群 ==================="
      for i in cdh112 cdh113 cdh114
      do
        ssh $i "systemctl stop cloudera-scm-agent"
      done

        ssh hadoop102 "systemctl stop cloudera-scm-server"

};;
esac