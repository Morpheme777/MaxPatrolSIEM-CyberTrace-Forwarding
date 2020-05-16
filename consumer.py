import socket
import json
import threading, queue
import time
import sys

from classes.mpsiem_queue import MPSiemQueue
from classes.output_socket import OutputSocket
#from multiprocessing import Process

# Global Variables
RMQSettings = {
    'rmqUser':'mpx_siem',
    'rmqPassword':'P@ssw0rd',
    'rmqAddress':'172.26.12.197',
    'rmqPort':5672,
    'rmqVhost':'/',
    'rmqQueue':'cybertraceq'
}
FS_HOST = '172.26.12.199'
FS_PORT = 9999  
lookup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
q = queue.Queue()


def sendEvents(q):    
    while True:
        while q.qsize() > 0:        
            ev = q.get()    
            lookup_socket.send(ev)

    
def FSConnect(host, port):
    
    lookup_socket.connect((host, port))
    lookup_socket.send(bytes("X-KF-SendFinishedEvent", "utf-8"))
    return lookup_socket
   
def startConsuming(channel):
    while True:
        try:
            channel.basic_qos(prefetch_count=1)
            channel.queue_declare(RMQSettings['rmqQueue'], durable=True)        
            # Start consuming
            channel.basic_consume(
            queue=RMQSettings['rmqQueue'], on_message_callback=callback, auto_ack=True)
         
            channel.start_consuming()
        except:
            print("RMQ Connection closed, reconnecting")
            channel = rmqConnect(RMQSettings)
            time.sleep(30)
            continue
            #thread.interrupt_main()

def consumer():
    # Connect to FS
    lookup_socket = FSConnect(FS_HOST, FS_PORT)

    output_socket = OutputSocket(host = '172.26.12.199',
                                port = 9999,
                                start_flag = 'X-KF-SendFinishedEvent')
    thread_send = threading.Thread(target=sendEvents,args=(q,))
    thread_send.start()
    
    
    mpsiem_queue = MPSiemQueue(host = '172.26.12.197', 
                            username = 'mpx_siem', 
                            password = 'P@ssw0rd', 
                            queue_name = 'cybertraceq',
                            port = 5672,
                            rmq_vhost = '/',
                            timeout = 30,
                            ioc_fields = ['src.ip','dst.ip','src.host','dst.host','dst.port','object.path','object.hash','datafield1','recv_ipv4','event_src.host','event_src.title'],
                            filter = [{'field': 'event_src.title', 'operator': 'ne', 'value': 'cybertrace'}])
    thread_consumer = threading.Thread(target=mpsiem_queue.consume)
    thread_consumer.start()

def main():
    consumer()
    print(' [*] Waiting for messages. To exit press CTRL+C')
     

if __name__ == "__main__":
    main()
