#!/bin/bash
project_home=/home/atguigu/gmall_1128
# 起一个nginx, 三个log服务器  log.sh start/stop/
case $1 in
start)
   echo "======在 hadoop102 启动 nginx======"
   sudo /usr/local/webserver/nginx/sbin/nginx
   for host in hadoop102 hadoop103 hadoop104 ; do
       echo "=====$host 启动日志服务器===== "
       ssh $host "source /etc/profile ; nohup java -jar $project_home/gmall-logger-0.0.1-SNAPSHOT.jar 1>$project_home/log.log 2>$project_home/error.log &"
   done

   ;;

stop)
    echo "======在 hadoop102 停止 nginx======"
    sudo /usr/local/webserver/nginx/sbin/nginx -s stop
    for host in hadoop102 hadoop103 hadoop104 ; do
       echo "=====$host 停止日志服务器===== "
       ssh $host "source /etc/profile ; ps -ef | awk '/gmall-logger/ && !/awk/{print \$2}'| xargs kill -9"
   done
;;

*)
    echo "脚本的正确使用方式: "
    echo "   start 启动nginx和日志服务器 "
    echo "   stop  停止nginx和日志服务器 "
;;
esac


#  jps | grep gmall | awk '{print $1}' | xargs kill -9

# ps -ef | grep gmall-logger | grep -v grep | awk '{print $2}' | xargs kill -9

# ps -ef | awk '/gmall-logger/ && !/awk/{print $2}'

#1. java -jar ....jar   jar包内已经打包的主类

#2. 如果打的jar包中没有主类
#   java -cp gmall-logger-0.0.1-SNAPSHOT.jar org.springframework.boot.loader.JarLauncher

#3. 如果打的jar包中没有主类
 #  java -jar gmall-logger-0.0.1-SNAPSHOT.jar org.springframework.boot.loader.JarLauncher


