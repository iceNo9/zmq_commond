import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# 获取当前脚本所在的目录
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

# 如果日志目录不存在，创建目录
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 创建日志文件路径
log_filename = os.path.join(log_dir, f"{datetime.today().strftime('%Y-%m-%d')}.log")

# 创建logger
logger = logging.getLogger()

# 设置日志的最低级别
logger.setLevel(logging.DEBUG)

# 创建一个按时间分割日志的处理器（每天一个新的日志文件）
log_handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7, encoding='utf-8')
log_handler.setLevel(logging.DEBUG)  # 设置日志级别

# 日志格式，包含时间、日志级别、文件名、行号和消息（用于文件输出）
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - line %(lineno)d - %(message)s')
log_handler.setFormatter(file_formatter)

# 将 handler 添加到 logger
logger.addHandler(log_handler)

# 如果需要将日志同时输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # 控制台只显示 INFO 及以上的日志

# 控制台输出格式，只输出日志消息内容（没有时间戳、日志级别等前缀）
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

# 将控制台 handler 添加到 logger
logger.addHandler(console_handler)
