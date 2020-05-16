import threading
import logging
import time
import queue

from classes.mpsiem_queue import MPSiemQueue
from classes.output_socket import OutputSocket


class Forwarder():

    def __init__(self, settings):
        self.log = logging.getLogger("forwarder")
        self.log_mon = logging.getLogger("monitoring")
        self.settings = settings
        self.q = queue.Queue()
        self.inside_queue = 0
        self.monitoring_timeout = self.settings["monitoring"].get("timeout", 10)

    def processQueue(self):
        self.output_socket.initSocket()
        while True:
            try:
                self.inside_queue = self.q.qsize()
                while self.inside_queue > 0:        
                    msg = self.q.get()
                    self.output_socket.send_socket.send(msg)
                    self.output_socket.msg_counter += 1
            except Exception as e:
                try:
                    self.output_socket.send_socket.close()
                except:
                    pass
                self.output_socket.socket_status = False
                self.output_socket.log.warning("Socket has lost connection: {}. Reconnection in 30 sec..".format(str(e)))
                time.sleep(self.output_socket.timeout)
                self.output_socket.initSocket()
    
    def monitoring(self):
        while True:
            time.sleep(self.monitoring_timeout)
            self.log_mon.info('Consumer:{}, Sender:{}, Queue cache:{}, Input: [{} msg, {} evts, {} eps], Output: [{} evts, {} eps]'.format(
                str(self.mpsiem_queue.chanel_status),
                str(self.output_socket.socket_status),
                str(self.output_socket.inside_queue),
                str(self.mpsiem_queue.msg_counter),
                str(self.mpsiem_queue.event_counter),
                str(round(self.mpsiem_queue.event_counter/self.monitoring_timeout, 2)),
                str(self.output_socket.msg_counter),
                str(round(self.output_socket.msg_counter/self.monitoring_timeout, 2))
            ))
            self.mpsiem_queue.msg_counter = 0
            self.mpsiem_queue.event_counter = 0
            self.output_socket.msg_counter = 0

    def run(self):
        self.log.info("""
 ####################################
 # ~~~ MaxPatrol SIEM Forwarder ~~~ #
 ####################################""")
        self.log.info("Forwarder running..")
        self.log.info("MPSiemQueue initializing..")
        self.mpsiem_queue = MPSiemQueue(self.settings["consumer"])
        self.mpsiem_queue.out = self.q
        self.log.info("MPSiemQueue initialized")

        self.log.info("OutputSocket initializing..")
        self.output_socket = OutputSocket(self.settings["sender"])
        self.log.info("OutputSocket initialized")

        
        thread_send = threading.Thread(target=self.processQueue)
        thread_send.start()
        
        thread_consumer = threading.Thread(target=self.mpsiem_queue.consume)
        thread_consumer.start()
    
        thread_monitoring = threading.Thread(target=self.monitoring)
        thread_monitoring.start()
        
        