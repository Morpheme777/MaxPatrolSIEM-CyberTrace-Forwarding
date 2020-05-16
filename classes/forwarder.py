import threading
import logging
import time
import queue

from classes.mpsiem_queue import MPSiemQueue
from classes.output_socket import OutputSocket


class Forwarder():

    def __init__(self, settings):
        self.log = logging.getLogger("forwarder")
        self.settings = settings
        self.q = queue.Queue()
        self.inside_queue = 0

    def processQueue(self):
        output_socket.initSocket()
        while True:
            try:
                self.inside_queue = queue.qsize()
                while self.inside_queue > 0:        
                    msg = self.q.get()
                    output_socket.send_socket.send(msg)
                    output_socket.msg_counter += 1
            except Exception as e:
                try:
                    output_socket.send_socket.close()
                except:
                    pass
                output_socket.socket_status = False
                output_socket.log.warning("Socket has lost connection: {}. Reconnection in 30 sec..".format(str(e)))
                time.sleep(output_socket.timeout)
                output_socket.initSocket()

    def run(self):
        self.log.info("Forwarder running..")
        self.log.info("MPSiemQueue initializing..")
        self.mpsiem_queue = MPSiemQueue(host = '10.31.120.59', 
                                username = 'mpx_siem', 
                                password = 'P@ssw0rd', 
                                queue_name = 'cybertraceq',
                                port = 5672,
                                rmq_vhost = '/',
                                timeout = 30,
                                ioc_fields = ['src.ip','dst.ip','src.host','dst.host','dst.port','object.path','object.hash','datafield1','recv_ipv4','event_src.host','event_src.title'],
                                filter = [{'field': 'event_src.title', 'operator': 'ne', 'value': 'cybertrace'}])
        self.mpsiem_queue.out = self.q
        self.log.info("MPSiemQueue initialized")

        self.log.info("OutputSocket initializing..")
        self.output_socket = OutputSocket(host = '10.31.120.59',
                                    port = 9999,
                                    timeout = 30,
                                    start_flag = 'X-KF-SendFinishedEvent')
        self.log.info("OutputSocket initialized")

        
        thread_send = threading.Thread(target=self.processQueue)
        thread_send.start()
        
        thread_consumer = threading.Thread(target=self.mpsiem_queue.consume)
        thread_consumer.start()
        
        while True:
            time.sleep(10)
            self.log.info('queue size = {}, processed messages in = {}, processed events in = {}, processed events out = {}'.format(
                str(self.output_socket.inside_queue),
                str(self.mpsiem_queue.msg_counter),
                str(self.mpsiem_queue.event_counter),
                str(self.output_socket.msg_counter)
            ))
            self.mpsiem_queue.msg_counter = 0
            self.mpsiem_queue.event_counter = 0
            self.output_socket.msg_counter = 0