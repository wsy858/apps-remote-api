#!/bin/sh
# author:     evan_wang

# 服务所在目录
SERVICE_DIR=/home/hadoop/apps-remote-api
# 服务名
SERVICE_NAME=spark_launcher
# 服务进程文件
PID=$SERVICE_NAME\.pid

cd $SERVICE_DIR

case "$1" in
    
    #启动	
    start)
          [ -e $SERVICE_DIR/$PID ] && echo "$SERVICE_NAME start failed, because it is running..... please restart it or delete $PID file" && exit 1

          # nohup & 以守护进程启动
          nohup python3 -u ./files/spark_launcher.py --port=[xx] --yarn_rm_host=[xx] --yarn_rm_port=[xx] 1>/dev/null 2>logs/error.log &
          echo $! > $SERVICE_DIR/$PID
          echo "=== start $SERVICE_NAME process success"
          ;;
    #停止
    stop) 
         kill -9 `cat $SERVICE_DIR/$PID`
		 if [ $? == 0 ];then
            echo "=== stop $SERVICE_NAME process success"
		 else 
            # 暂停2s, 在检查服务是否存在
			sleep 2
			P_ID=`ps -ef | grep -w "$SERVICE_NAME" | grep -v "grep" | awk '{print $2}'`
			if [ "$P_ID" != "" ];then
				echo "=== $SERVICE_NAME process stop failed, pid is:$P_ID, please kill it manually!"
			fi		 
         fi
         rm -rf $SERVICE_DIR/$PID         
         ;;
     #重启
      restart)
         $0 stop
         sleep 2
         $0 start
         ;;
     
       *)
          echo "Usage: $0 start|stop|restart" 
esac
exit 0

