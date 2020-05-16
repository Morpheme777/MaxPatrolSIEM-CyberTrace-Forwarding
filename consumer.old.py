import pika
import socket
import json
import ipaddress
import threading, queue
import time
import sys
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

def callback(ch, method, properties, body):
    #try:    
    j = json.loads(body.decode())
    for event in j:
        src = event['src.ip'] if "src.ip" in event else ""
        dst = event['dst.ip'] if "dst.ip" in event else ""
        shost = event['src.host'] if "src.host" in event else ""
        dhost = event['dst.host'] if "dst.host" in event else ""
        dport = str(event['dst.port']) if "dst.port" in event else ""
        objp = event['object.path'] if "object.path" in event else ""
        objh = event['object.hash'] if "object.hash" in event else ""
        df1 = event['datafield1'] if "datafield1" in event else ""
        dvc = event['recv_ipv4'] if "recv_ipv4" in event else ""
        dvch = event['event_src.host'] if "event_src.host" in event else "" 
        product = event['event_src.title'] if "event_src.title" in event else ""
        ev_tmp = "src=" + str(src) + " dst=" + str(dst) + " shost=" + str(shost) + " dhost=" + str(dhost) + " dport=" + str(dport) + " objp=" + str(objp) + " objh=" + str(objh) + " df1=" + str(df1) + " product=" + str(product) + " dvc=" + str(dvc) + " dvch=" + str(dvch) + '\n'
        if product != "cybertrace":
            q.put(bytes(ev_tmp, 'utf-8'))


def sendEvents(q):    
    while True:
        while q.qsize() > 0:        
            ev = q.get()    
            lookup_socket.send(ev)
            

def rmqConnect(rmq_set):
    while True:
        credentials = pika.PlainCredentials(rmq_set['rmqUser'], rmq_set['rmqPassword'])
        try:
            connection = pika.BlockingConnection(    
            pika.ConnectionParameters(rmq_set['rmqAddress'], rmq_set['rmqPort'], rmq_set['rmqVhost'], credentials))
        
            channel = connection.channel()
            print("RMQ Connection established")
            return channel
        except:
            print("RMQ Connection was not established")
            time.sleep(30)
            continue
    
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

def consumer1():
    # Connect to FS
    lookup_socket = FSConnect(FS_HOST, FS_PORT)
    # Connecting to RMQ
    channel1 = rmqConnect(RMQSettings)
    
    thread_send = threading.Thread(target=sendEvents,args=(q,))
    thread_send.start()
    thread_consumer = threading.Thread(target=startConsuming, args=(channel1,))
    thread_consumer.start()

def main():
    consumer1()
    print(' [*] Waiting for messages. To exit press CTRL+C')
     

if __name__ == "__main__":
    main()
