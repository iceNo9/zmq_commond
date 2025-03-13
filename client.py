import zmq
import threading
import time
import uuid
from log_config import logger

MAX_QUEUE_SIZE = 100  # 设置最大消息队列长度
HEARTBEAT_INTERVAL = 1  # 心跳间隔 (秒)
HEARTBEAT_TIMEOUT = 60   # 超时 (秒)

COMMAND_CONNECT = "CONNECT"
COMMAND_HEARTBEAT = "HEARTBEAT"

class ZMQClient:
    def __init__(self, recv_port=5556, send_port=5555, server_ip="127.0.0.1"):
        """初始化客户端"""
        self.server_ip = server_ip
        self.recv_port = recv_port
        self.send_port = send_port
        self.context = zmq.Context()
        
        # 生成唯一的客户端 ID
        self.client_id = str(uuid.uuid4()).encode("utf-8")

        # 用于接收服务器消息
        self.recv_socket = self.context.socket(zmq.DEALER)  
        self.recv_socket.setsockopt(zmq.IDENTITY, self.client_id)

        # 用于发送消息到服务器
        self.send_socket = self.context.socket(zmq.DEALER)
        self.send_socket.setsockopt(zmq.IDENTITY, self.client_id)

        self.running = False
        self.recv_thread = None
        self.send_thread = None
        self.recv_queue = []
        self.send_queue = []
        self.heartbeat_thread = None
        self.last_heartbeat_time = time.time()

    def start(self):
        """启动 ZMQ 客户端"""
        if self.running:
            logger.info("客户端已经在运行.")
            return

        self.running = True
        self.recv_socket.connect(f"tcp://{self.server_ip}:{self.recv_port}")  # 连接服务器
        self.send_socket.connect(f"tcp://{self.server_ip}:{self.send_port}")  # 连接服务器

        # 发送连接通知
        self.send_socket.send_multipart([COMMAND_CONNECT.encode("utf-8")])        

        # 启动接收和发送线程
        self.recv_thread = threading.Thread(target=self.receive_messages, daemon=True)
        self.send_thread = threading.Thread(target=self.send_messages, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)

        self.recv_thread.start()
        self.send_thread.start()
        self.heartbeat_thread.start()

        logger.info(f"已连接到 {self.server_ip} 的端口 {self.recv_port} (接收) 和 {self.send_port} (发送).")

    def stop(self):
        """停止 ZMQ 客户端"""
        if not self.running:
            logger.info("客户端未运行.")
            return

        self.running = False
        logger.info("客户端正在停止...")

        if self.recv_thread:
            self.recv_thread.join()
        if self.send_thread:
            self.send_thread.join()
        if self.heartbeat_thread:
            self.heartbeat_thread.join()

        self.recv_socket.close()
        self.send_socket.close()
        self.context.term()

        logger.info("客户端已停止.")

    def receive_messages(self):
        """接收服务器消息"""
        while self.running:
            try:
                message_parts = self.recv_socket.recv_multipart(flags=zmq.NOBLOCK)
                message = message_parts[0]
                message_str = message.decode("utf-8")
                logger.debug(f"收到消息: {message_str}")

                if message_str == COMMAND_HEARTBEAT:
                    self.last_heartbeat_time = time.time()
                    continue  

                self.recv_queue.append(message_str)

            except zmq.Again:
                if time.time() - self.last_heartbeat_time > HEARTBEAT_TIMEOUT:
                    logger.warning("服务器超时，客户端将断开连接.")
                    self.stop()
                time.sleep(0.1)

    def send_messages(self):
        """从发送队列发送消息"""
        while self.running:
            if self.send_queue:                
                message = self.send_queue.pop(0)  

                logger.debug(f"发送消息: {message}")

                try:
                    if isinstance(message, bytes):
                        message_bytes = message
                    else:
                        message_bytes = message.encode("utf-8")

                    self.send_socket.send_multipart([message_bytes], zmq.NOBLOCK)

                except zmq.ZMQError:
                    logger.warning("发送失败，服务器可能未连接.")
                    time.sleep(1)  

            time.sleep(0.1)  

    def send_heartbeat(self):
        """发送心跳"""
        while self.running:
            self.add_message_to_send_queue(COMMAND_HEARTBEAT)
            time.sleep(HEARTBEAT_INTERVAL)

    def add_message_to_send_queue(self, message):
        """将消息添加到发送队列"""
        if not self.running:
            logger.warning("客户端未启动，丢弃消息{message}.")
            return

        if len(self.send_queue) >= MAX_QUEUE_SIZE:
            logger.warning(f"发送队列已满，丢弃最旧的消息: {self.send_queue[0]}")
            self.send_queue.pop(0)  

        self.send_queue.append(message)

    def get_received_messages(self):
        """获取接收到的所有消息"""
        messages = self.recv_queue.copy()
        self.recv_queue.clear()
        return messages


if __name__ == "__main__":
    client = ZMQClient()
    client.start()

    while True:
        try:
            pass
        except KeyboardInterrupt:
            client.stop()
            break