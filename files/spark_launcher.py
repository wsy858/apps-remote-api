# coding=utf-8
"""
@to  调度操作接口
@author evan wang
"""

import requests
import shlex
import subprocess
import threading
import logging
import logging.config

from tornado import escape
from tornado import httpserver
from tornado import ioloop
from tornado import web
from tornado.options import options, define

# 启动命令参数
define("port", default=8001, help="TCP port to listen on")
define("yarn_rm_host", help="yarn restful host")
define("yarn_rm_port", default=8088, help="yarn restful post")
define("yarn_config_path", default=None, help="yarn配置文件路径，可选参数，集团环境需要指定")

# 作业提交状态，key为应用yarn_tags，value为状态
SUBMIT_RESULT = {}
# 提交失败
JOB_STATUS_SUBMIT_FAILED = 0
# 提交成功
JOB_STATUS_SUBMIT_SUCCESS = 1
# 提交中
JOB_STATUS_SUBMITTING = 2

# 匹配的任务状态
RUNNING_STATUS_LIST = ["NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING"]
# 自定义类型状态, 主要用于细分特殊情况, [正在提交， YARN上面不存在， 获取状态异常]
NOT_EXISTS_AND_ERROR_STATUS_LIST = ["SUBMITTING", "NOT_EXISTS", "GET_STATUS_ERROR"]


# 请求基类
class BaseRequestHandler(web.RequestHandler):
    def data_received(self, chunk):
        pass

    SUPPORTED_METHODS = ("PUT", "POST", "GET", "DELETE")


# 提交任务
class SubmitHandler(BaseRequestHandler):

    def post(self):
        result = {"code": 0, "msg": "OK"}
        data = escape.json_decode(self.request.body)
        logger.info(self.request.method + self.request.uri)
        logger.info("parameters===============================> %s", str(data))
        # 解析参数
        yarn_tags = data.get('yarn_tags')
        if yarn_tags is None:
            self.write({"code": 0, "msg": "yarn_tags can not be empty"})
        command = self.generate_spark_command(data)
        if SUBMIT_RESULT.get(yarn_tags) == JOB_STATUS_SUBMITTING:
            logger.error("%s 正在提交中", yarn_tags)
            result["msg"] = yarn_tags + '正在提交中，稍后再提交'
        else:
            if command is not None:
                logger.info("submit command===============================> %s", command)
                thread = threading.Thread(target=execute_command, args=(command, yarn_tags,))
                thread.start()
            result["code"] = 1
        self.write(result)

    # 解析参数, 生成spark提交命令
    @staticmethod
    def generate_spark_command(data):
        file = data.get('file')
        yarn_tags = data.get('yarn_tags')
        num_executors = data.get('numExecutors', '10')
        driver_memory = data.get('driverMemory', '1g')
        executor_memory = data.get('executorMemory', '2g')
        driver_cores = data.get('driverCores', '2')
        executor_cores = data.get('executorCores', '2')
        # proxyUser = data.get('proxyUser', 'hadoop')
        name = data.get('name')
        class_name = data.get('className')
        queue = data.get('queue', 'root.dw')
        conf = data.get('conf')
        args = data.get('args')
        files = data.get('files')
        spark_command = "spark-submit --deploy-mode cluster --master yarn "
        spark_command += " --name '" + name + "'"
        spark_command += " --queue '" + queue + "'"
        spark_command += " --class '" + class_name + "'"
        spark_command += " --conf spark.submit.deployMode=cluster"
        spark_command += " --conf spark.master=yarn"
        spark_command += " --conf spark.yarn.submit.waitAppCompletion=false"
        spark_command += " --conf spark.executor.instances=" + str(num_executors)
        spark_command += " --conf spark.executor.cores=" + str(executor_cores)
        spark_command += " --conf spark.executor.memory=" + str(executor_memory)
        spark_command += " --conf spark.driver.cores=" + str(driver_cores)
        spark_command += " --conf spark.driver.memory=" + str(driver_memory)
        if yarn_tags is not None:
            spark_command += " --conf spark.yarn.tags='" + yarn_tags + "'"
        if files is not None:
            for ff in files:
                spark_command += " --files " + ff
        if conf is not None:
            for key in conf:
                spark_command += " --conf " + key + "=" + str(conf[key])
        spark_command += " '" + file + "' "
        if args is not None:
            for arg in args:
                spark_command += " '" + arg + "' "
        return spark_command


# 获取任务状态
class StatusHandler(BaseRequestHandler):
    def get(self):
        result = {"code": 1}
        yarn_tags = self.get_argument("yarn_tags")
        if SUBMIT_RESULT.get(yarn_tags) == JOB_STATUS_SUBMITTING:
            # 正在提交，还没有提交到YARN上面去
            result['data'] = "SUBMITTING"
            logger.warn("%s status is submitting", yarn_tags)
        else:
            # 获取YARN上面的状态
            app_info_result = get_application_info(yarn_tags)
            if app_info_result.get('message') not in NOT_EXISTS_AND_ERROR_STATUS_LIST:
                app_info_list = app_info_result['apps']
                state = app_info_list[len(app_info_list) - 1].get("state")
                if len(app_info_list) > 1:
                    for app_info in app_info_list:
                        if app_info.get("state") in RUNNING_STATUS_LIST:
                            state = app_info.get("state")
                            break
                result['data'] = state
            else:
                result['data'] = app_info_result.get('message')
                logger.warn("%s status is %s", yarn_tags, app_info_result.get('message'))
        self.write(result)


# 获取Yarn上面日志地址
class GetLogsHandler(BaseRequestHandler):

    def get(self):
        # logging.info(self.request.method + self.request.uri)
        result = {"code": 0, "msg": ""}
        yarn_tags = self.get_argument("yarn_tags")
        app_info_result = get_application_info(yarn_tags)
        if app_info_result.get('message') not in NOT_EXISTS_AND_ERROR_STATUS_LIST:
            app_info_list = app_info_result.get('apps')
            logs_url = app_info_list[len(app_info_list) - 1].get("trackingUrl")
            result["code"] = 1
            result['data'] = logs_url
        else:
            result["msg"] = "apps info not exists----"
        self.write(result)


# 中断任务
class KillHandler(BaseRequestHandler):

    def post(self):
        logging.info(self.request.method + self.request.uri)
        yarn_tags = self.get_argument("yarn_tags")
        app_info_result = get_application_info(yarn_tags, "NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING")
        if app_info_result.get('message') not in NOT_EXISTS_AND_ERROR_STATUS_LIST:
            application_info = app_info_result.get('apps')
            if application_info is not None:
                kill_app(application_info)
                self.write({"code": 1, "msg": "OK"})
        else:
            logging.info("get application_info is empty")
            self.write({"code": 0, "msg": "error"})


# kill任务
def kill_app(application_info):
    stopped_state = ["FINISHED", "FAILED", "KILLED"]
    for app in application_info:
        app_id = app.get("id")
        state = app.get("state")
        if stopped_state.count(state) == 0:
            command_str = "yarn application -kill " + app_id
            yarn_config_path = options.yarn_config_path
            if yarn_config_path is not None:
                command_str = "yarn --config " + yarn_config_path + " application -kill " + app_id
            logger.info(command_str)
            execute_command(command_str)


# 执行shell等外部命令
def execute_command(cmd_string, yarn_tags=None, shell=False):
    logger.info("========================================execute_command : " + str(cmd_string))
    if shell:
        cmd_string_list = cmd_string
    else:
        cmd_string_list = shlex.split(cmd_string)
    if yarn_tags is not None:
        # 标识作业正在提交状态
        SUBMIT_RESULT[yarn_tags] = JOB_STATUS_SUBMITTING
    try:
        sub = subprocess.Popen(cmd_string_list, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, shell=shell, bufsize=4096)
        # 阻塞等待子进程执行完毕
        for info in sub.communicate():
            logger.info(info.decode())
        # 下面更新内存中提交的状态
        if yarn_tags is not None:
            logger.info("yarnTags: %s, command return code: %s", yarn_tags, str(sub.returncode))
            if sub.returncode == 0:
                # 标识作业提交状态为成功
                SUBMIT_RESULT[yarn_tags] = JOB_STATUS_SUBMIT_SUCCESS
            else:
                # 标识作业提交状态为失败
                SUBMIT_RESULT[yarn_tags] = JOB_STATUS_SUBMIT_FAILED
        else:
            logging.info("command return code: %s", str(sub.returncode))
    except Exception:
        if yarn_tags is not None:
            # 标识作业提交状态为失败
            SUBMIT_RESULT[yarn_tags] = JOB_STATUS_SUBMIT_FAILED
        logger.exception("运行异常: %s", cmd_string)


# 根据tags获取applicationInfo
# @param yarn_tags : 筛选标识
# @param states ： 筛选状态
# @return 正常：{'message': None, 'apps':[{'id':'application_id','state': 'RUNNING','finalStatus': 'UNDEFINED'},{}]}
#         或者任务不存在时: {'message': 'NOT_EXISTS', 'apps' : None}
#         或者获取状态异常: {'message': 'GET_STATUS_ERROR', 'apps' : None}
def get_application_info(yarn_tags, states=None):
    result = {'message': None, 'apps': None}
    yarn_rm_host = options.yarn_rm_host
    yarn_rm_port = options.yarn_rm_port
    app_get_url = "http://" + yarn_rm_host + ":" + str(
        yarn_rm_port) + "/ws/v1/cluster/apps/?limit=100&applicationTags=" + yarn_tags
    if states is not None:
        app_get_url += "&states=" + states
    try:
        http_result = requests.get(app_get_url)
        if http_result.status_code == 200:
            result_json = http_result.json()
            if result_json is not None:
                if result_json["apps"] is not None and len(result_json["apps"]["app"]) > 0:
                    result['apps'] = result_json["apps"]["app"]
                else:
                    result['message'] = "NOT_EXISTS"
                    logger.error("%s apps not exists, %s", app_get_url, str(http_result.content))
            else:
                result['message'] = "GET_STATUS_ERROR"
                logger.error("%s result_json is None, %s", app_get_url, str(http_result.content))
        else:
            result['message'] = "GET_STATUS_ERROR"
            logger.error("%s result code: %s , content: %s", app_get_url, http_result.status_code,
                         str(http_result.content))
    except Exception:
        result['message'] = "GET_STATUS_ERROR"
        logger.exception("%s get status exception: ", app_get_url)
    return result


# 初始化app，配置映射路由
class S3Application(web.Application):
    def __init__(self):
        web.Application.__init__(
            self,
            [
                (r"/remote_api/job_submit/", SubmitHandler),
                (r"/remote_api/job_status/", StatusHandler),
                (r"/remote_api/job_kill/", KillHandler),
                (r"/remote_api/job_logs/", GetLogsHandler),
            ],
        )


# 开启日志
def enable_logging():
    logging_config_path = 'logging.conf'
    logging.config.fileConfig(logging_config_path)
    _logger = logging.getLogger('root')
    return _logger


# 监听端口，启动服务
def start(port):
    if options.yarn_rm_host is None:
        logger.warning("yarn_rm_host启动参数不能为空")
        return
    application = S3Application()
    http_server = httpserver.HTTPServer(application)
    http_server.listen(port)
    logger.info("启动成功, 服务监听于端口: %s", str(port))
    ioloop.IOLoop.current().start()


# main入口
if __name__ == "__main__":
    options.parse_command_line()
    logger = enable_logging()
    start(options.port)