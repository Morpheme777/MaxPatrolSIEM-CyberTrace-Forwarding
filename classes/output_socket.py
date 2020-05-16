import socket
import time
import logging  


class OutputSocket():

    def __init__(self, settings):
        self.log = logging.getLogger("output_socket")
        self.log.info(
            'Settings: host={}, port={}'.format(
                settings.get("host"),
                settings.get("port")))
        self.log.info('Additional settings: {}'.format(', '.join(
            ['{}={}'.format(k, v) for k, v in settings.items()]
        )))
        self.host = settings.get("host")
        self.port = settings.get("port")
        self.initSettings(settings)
        self.socket_status = False
        self.msg_counter = 0
        self.inside_queue = 0
    
    def initSettings(self, settings):
        self.start_flag = settings.get("start_flag", "")
        self.timeout = settings.get("timeout", 30)
    
    def initSocket(self):
        while not self.socket_status:
            self.log.info("Trying to connect..")
            try:
                self.log.info("Socket initializing..")
                self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.send_socket.connect((self.host, self.port))
                if self.start_flag:
                    self.send_socket.send(bytes(self.start_flag, "utf-8"))
                self.log.info("Socket initialized")
                self.socket_status = True
            except Exception as e:
                try:
                    self.send_socket.close()
                except:
                    pass
                self.log.error("Socket initializing is failed: {}. Reconnection in 30 sec..".format(str(e)))
                time.sleep(self.timeout)
                self.socket_status = False

    def send(self, queue):
        self.initSocket()
        while True:
            try:
                self.inside_queue = queue.qsize()
                while self.inside_queue > 0:        
                    msg = queue.get()
                    self.send_socket.send(msg)
                    self.msg_counter += 1
            except Exception as e:
                try:
                    self.send_socket.close()
                except:
                    pass
                self.socket_status = False
                self.log.warning("Socket has lost connection: {}. Reconnection in 30 sec..".format(str(e)))
                time.sleep(self.timeout)
                self.initSocket()

    def send1(self, msg):
        self.send_socket.send(msg)
        self.msg_counter += 1
            