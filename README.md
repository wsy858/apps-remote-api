#### 一：功能
此python脚本主要用于远程提交spark，查询yarn任务状态，查询日志地址，kill任务。

#### 二：环境
1. python环境版本
   python 3.6
2. 第三方库：
- requests
- tornado

#### 三：部署
(1). 启动脚本
启动脚本为service.sh, 存在如下脚本, 需要修改里面的参数：

```
nohup python3 -u ./files/spark_launcher.py --port=[xx] --yarn_rm_host=[xx] --yarn_rm_port=[xx] 1>/dev/null 2>logs/error.log &
```
- --port           web服务启动时监听的端口, 默认8001
- --yarn_rm_host   yarn resource manager主机名，必填
- --yarn_rm_port   yarn resource manager端口，默认8088

(2). 启停方式，使用脚本方式
-  启动：./service.sh start
-  停止：./service.sh stop
-  重启：./service.sh restart