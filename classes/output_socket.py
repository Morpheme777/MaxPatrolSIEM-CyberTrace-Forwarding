import socket
import time


class OutputSocket():

    def __init__(self, host, port **kwargs):
        self.host = host
        self.port = port
        self.initSettings(**kwargs)
    
    def initSettings(self, **kwargs):
        self.start_flag = kwargs.get("start_flag") or ""
        self.timeout = kwargs.get("timeout") or 30
    
    def initSocket(self):
        socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.connect((self.host, self.port))
        if start_flag:
            socket.send(bytes(self.start_flag, "utf-8"))
        return socket

    def send(self, queue):
        socket = self.initSocket()
        while True:
            try:
                while queue.qsize() > 0:        
                    msg = queue.get()    
                    socket.send(msg)
            except:
                time.sleep(self.timeout)
                socket = self.initSocket()