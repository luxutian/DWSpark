#! /bin/bash
case $1 in
"start"){
    echo " =================== 启动 impala集群 ==================="

    echo " --------------- 启动 impala 102 ---------------"
    ssh hadoop102 "service impala-state-store start"
    ssh hadoop102 "service impala-catalog start"
    ssh hadoop102 "service impala-server start"

    echo " --------------- 启动 impala 103 ---------------"
    ssh hadoop103 "service impala-server start"

    echo " --------------- 启动 impala 104 ---------------"
    ssh hadoop104 "service impala-server start"

};;
"stop"){
    echo " =================== 关闭 impala集群 ==================="
      for i in hadoop102 hadoop103 hadoop104
      do
        ssh $i "service impala-server stop"
      done

        ssh hadoop102 "service impala-state-store stop"
        ssh hadoop102 "service impala-catalog stop"

};;
esac