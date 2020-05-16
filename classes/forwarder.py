import threading
import logging
import time

from classes.mpsiem_queue import MPSiemQueue
from classes.output_socket import OutputSocket


class Forwarder():

    def __init__(self, settings):
        self.log = logging.getLogger("forwarder")
        self.settings = settings

    def run(self):
        self.log.info("Forwarder running..")
        self.log.info("MPSiemQueue initializing..")
        mpsiem_queue = MPSiemQueue(host = '10.31.120.59', 
                                username = 'mpx_siem', 
                                password = 'P@ssw0rd', 
                                queue_name = 'cybertraceq',
                                port = 5672,
                                rmq_vhost = '/',
                                timeout = 30,
                                ioc_fields = ['src.ip','dst.ip','src.host','dst.host','dst.port','object.path','object.hash','datafield1','recv_ipv4','event_src.host','event_src.title'],
                                filter = [{'field': 'event_src.title', 'operator': 'ne', 'value': 'cybertrace'}])
        self.log.info("MPSiemQueue initialized")

        self.log.info("OutputSocket initializing..")
        output_socket = OutputSocket(host = '10.31.120.59',
                                    port = 9999,
                                    timeout = 30,
                                    start_flag = 'X-KF-SendFinishedEvent')
        self.log.info("OutputSocket initialized")

        
        thread_send = threading.Thread(target=output_socket.send,args=(mpsiem_queue.out,))
        thread_send.start()
        
        thread_consumer = threading.Thread(target=mpsiem_queue.consume)
        thread_consumer.start()
        
        while True:
            time.sleep(10)
            self.log.info('queue size = {}, processed events in = {}, processed messages in = {}, processed events out = {}'.format(
                str(output_socket.inside_queue),
                str(mpsiem_queue.msg_counter),
                str(mpsiem_queue.event_counter),
                str(output_socket.msg_counter)
            ))
            mpsiem_queue.msg_counter = 0
            mpsiem_queue.event_counter = 0
            output_socket.msg_counter = 0