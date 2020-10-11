#! /bin/bash

case $1 in
"start"){
	for i in hadoop102 hadoop103 hadoop104
	do
		ssh $i "service kudu-master start"
		ssh $i "service kudu-tserver start"
	done
};;
"stop"){
	for i in hadoop102 hadoop103 hadoop104
	do
		ssh $i "service kudu-master stop"
		ssh $i "service kudu-tserver stop"
	done
};;
esac