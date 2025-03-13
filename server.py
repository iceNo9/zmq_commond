from log_config import logger
import zmq
import threading
import time

HEARTBEAT_INTERVAL = 1  # 心跳间隔 (秒)
HEARTBEAT_TIMEOUT = 3   # 超时 (秒)

BROADCAST_ID = b"BROADCAST"  # 预设的广播 ID

COMMAND_CONNECT = "CONNECT"
COMMAND_HEARTBEAT = "HEARTBEAT"

class ZMQServer:

    """初始化服务器"""
    def __init__(self, recv_port=5555, send_port=5556):
        
        self.recv_port = recv_port
        self.send_port = send_port
        self.context = zmq.Context()

        self.recv_socket = self.context.socket(zmq.ROUTER)  # ROUTER 处理多客户端
        self.recv_socket.bind(f"tcp://*:{self.recv_port}")

        self.send_socket = self.context.socket(zmq.ROUTER)
        self.send_socket.bind(f"tcp://*:{self.send_port}")

        self.clients = {}  # 存储客户端心跳时间 {client_id: last_heartbeat}
        self.running = False

        self.recv_queue = []
        self.send_queue = []

        self.recv_thread = None
        self.send_thread = None
        self.heartbeat_thread = None

        self.clients_lock = threading.Lock()
        self.recv_queue_lock = threading.Lock()
        self.send_queue_lock = threading.Lock()


    """启动服务器"""
    def start(self):
        
        if self.running:
            logger.info("服务器已经在运行.")
            return

        self.running = True

        self.recv_thread = threading.Thread(target=self.receive_messages, daemon=True)
        self.send_thread = threading.Thread(target=self.send_messages, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self.check_heartbeat, daemon=True)

        self.recv_thread.start()
        self.send_thread.start()
        self.heartbeat_thread.start()

        logger.info(f"服务器已启动，接收端口 {self.recv_port}，发送端口 {self.send_port}")

    """停止服务器"""
    def stop(self):
        if not self.running:
            logger.info("服务器未运行.")
            return

        self.running = False
        logger.info("服务器正在停止...")

        if self.recv_thread:
            self.recv_thread.join()
        if self.send_thread:
            self.send_thread.join()
        if self.heartbeat_thread:
            self.heartbeat_thread.join()

        self.recv_socket.close()
        self.send_socket.close()
        self.context.term()

        logger.info("服务器已停止.")

    """接收消息并存入队列"""
    def receive_messages(self):
        while self.running:
            with self.recv_queue_lock:
                try:
                    message_parts = self.recv_socket.recv_multipart(flags=zmq.NOBLOCK)
                    client_id, message = message_parts

                    message_str = message.decode("utf-8")
                    logger.debug(f"收信 {client_id}: {message_str}")

                    # 更新客户端心跳时间
                    with self.clients_lock:
                        self.clients[client_id] = time.time()

                    if message_str == COMMAND_HEARTBEAT:
                        continue  # 忽略心跳消息

                    if message_str == COMMAND_CONNECT:
                        logger.info(f"客户端 {client_id} 已连接.")
                        continue # 忽略连接消息

                    # 将消息加入接收队列
                    with self.recv_queue_lock:
                        self.recv_queue.append((client_id, message_str))

                except zmq.Again:
                    time.sleep(0.1)

    # 从队列中发送消息
    def send_messages(self):
        """发送消息"""
        while self.running:
            with self.send_queue_lock:
                if self.send_queue:
                    client_id, message = self.send_queue.pop(0)

                    logger.debug(f"发信 {client_id}: {message}")

                    try:                        
                        if isinstance(message, bytes):
                            message_bytes = message
                        else:
                            message_bytes = message.encode("utf-8")
                    except Exception as e:
                        logger.warning(f"转换消息失败 {client_id}: {message}")
                        continue

                    try:
                        if client_id == BROADCAST_ID:
                            with self.clients_lock:
                                if not self.clients:  # 如果没有客户端，直接丢弃
                                    logger.debug("广播消息丢弃：无客户端连接")
                                    continue
                                for client in self.clients.keys():
                                    self.send_socket.send_multipart([client, message_bytes])
                        else:
                            self.send_socket.send_multipart([client_id, message_bytes])
                    except zmq.ZMQError:
                        logger.warning(f"发信失败 {client_id}: {message}")

            time.sleep(0.1)

    # 检查客户端心跳
    def check_heartbeat(self):
        """定期检查客户端心跳"""
        while self.running:
            current_time = time.time()
            disconnected_clients = []

            with self.clients_lock:
                for client_id, last_time in self.clients.items():
                    if current_time - last_time > HEARTBEAT_TIMEOUT:
                        logger.warning(f"客户端 {client_id} 超时.")
                        disconnected_clients.append(client_id)

                for client_id in disconnected_clients:
                    del self.clients[client_id]

            # 服务器向所有客户端发送心跳
            self.add_message_to_send_queue(BROADCAST_ID, COMMAND_HEARTBEAT)

            time.sleep(HEARTBEAT_INTERVAL)

    # 添加消息到发送队列
    def add_message_to_send_queue(self, client_id, message):
        """添加消息到发送队列"""
        if not self.running:
            logger.warning("服务器未运行，消息丢弃")
            return

        if client_id != BROADCAST_ID and client_id not in self.clients:
            logger.debug(f"客户端 {client_id} 未连接，消息丢弃")
            return
        with self.send_queue_lock:
            self.send_queue.append((client_id, message))

    def get_received_messages(self):
        """获取接收的所有消息"""
        with self.recv_queue_lock:
            messages = self.recv_queue.copy()
            self.recv_queue.clear()

        return messages
    
if __name__ == "__main__":
    app = ZMQServer()
    app.start()

    while True:
        try:
            pass
        except KeyboardInterrupt:
            app.stop()