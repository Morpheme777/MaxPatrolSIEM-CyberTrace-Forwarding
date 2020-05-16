import socket


class OutputSocket():

    def __init__(self, host, port **kwargs):
        self.host = host
        self.port = port
        self.initSettings(**kwargs)
    
    def initSettings(self, **kwargs):
        self.start_flag = kwargs.get("start_flag") or ""
    
    def initSocket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        if self.start_flag:
            self.socket.send(bytes(self.start_flag, "utf-8"))

    def send(self, queue):
        self.initSocket()