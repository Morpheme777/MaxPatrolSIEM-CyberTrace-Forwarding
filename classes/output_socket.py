import socket
import time
import logging  


class OutputSocket():

    def __init__(self, host, port, **kwargs):
        self.log = logging.getLogger("output_socket")
        self.log.info(
            'Settings: host={}, port={}'.format(
                host,
                port))
        self.log.info('Additional settings: {}'.format(', '.join(
            ['{}={}'.format(k, v) for k, v in kwargs.items()]
        )))
        self.host = host
        self.port = port
        self.initSettings(**kwargs)
        self.socket_status = False
        self.msg_counter = 0
    
    def initSettings(self, **kwargs):
        self.start_flag = kwargs.get("start_flag") or ""
        self.timeout = kwargs.get("timeout") or 30
    
    def initSocket(self):
        while not self.socket_status:
            self.log.info("Trying to connect..")
            try:
                self.log.info("Socket initializing..")
                self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.send_socket.connect((self.host, self.port))
                if start_flag:
                    self.send_socket.send(bytes(self.start_flag, "utf-8"))
                self.log.info("Socket initialized")
                self.socket_status = True
            except Exception as e:
                self.log.error("Socket initializing is failed: {}. Reconnection in 30 sec..".format(str(e)))
                time.sleep(self.timeout)
                self.socket_status = False

    def send(self, queue):
        self.initSocket()
        while True:
            try:
                while queue.qsize() > 0:        
                    msg = queue.get()    
                    send_socket.send(msg)
                    self.msg_counter += 1
            except:
                self.socket_status = False
                self.log.warning("Socket has lost connection. Reconnection in 30 sec..")
                time.sleep(self.timeout)
                self.initSocket()