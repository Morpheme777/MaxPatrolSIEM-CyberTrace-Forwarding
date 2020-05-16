import time
import json
import queue
import pika

class MPSiemQueue():
    def __init__(self, host, username, password, queue_name, **kwargs):
        self.host = host
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.out = queue.Queue()
        self.initSettings(**kwargs)
        
    def initSettings(self, **kwargs):
        self.rmq_vhost = kwargs.get("rmq_vhost") or "/"
        self.port = kwargs.get("port") or 5672
        self.timeout = kwargs.get("timeout") or 30
        self.ioc_fields = kwargs.get("ioc_fields")
        if self.ioc_fields:
            self.messageProcessing = self.messageProcessingByField
        else:
            self.messageProcessing = self.messageProcessingRawEvent
        filter = kwargs.get("filter") or []
        self.filter = []
        for condition in filter:
            if condition['operator'] == 'eq':
                condition['operator'] = self.operator_eq
                self.filter.append(condition)
            elif condition['operator'] == 'ne':
                condition['operator'] = self.operator_ne
                self.filter.append(condition)
            elif condition['operator'] == 'in':
                condition['operator'] = self.operator_in
                self.filter.append(condition)
            elif condition['operator'] == 'not in':
                condition['operator'] = self.operator_not_in
                self.filter.append(condition)
            else:
                continue
                
    def getChannel(self)
        credentials = pika.PlainCredentials(self.username, self.password)
        connection_parameters = pika.ConnectionParameters(self.host, 
                                                        self.port, 
                                                        self.rmq_vhost, 
                                                        credentials)
        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(self.queue_name, durable=True)
        return channel
    
    def messageProcessingByField(self, ch, method, properties, body):   
    events = json.loads(body.decode())
    for event in events:
        if filterEvent(event):
            event_out = ' '.join([event[field] for field in set(event.keys()) & set(self.ioc_fields)])
            self.out(bytes(event_out, 'utf-8'))
    
    def messageProcessingRawEvent(self, ch, method, properties, body):   
    events = json.loads(body.decode())
    for event in events:
        if filterEvent(event):
            event_out = ' '.join([value for _, value in event.items()])
            self.out(bytes(event_out, 'utf-8'))
    
    def consume(self):
        channel = self.getChannel()
        while True:
            try:
                channel.basic_consume(queue=self.queue_name, on_message_callback=self.messageProcessing, auto_ack=True)
                channel.start_consuming()
            except:
                time.sleep(self.timeout)
                channel = self.getChannel()
    
    def filterEvent(self, event):
        for condition in self.filter:
            if condition['field'] in event:
                field = condition['field']
            else:
                field = None
            if not condition['operator'](field, condition['value']):
                return False
        return True
    
    def operator_eq(self, field, value):
        if field == value:
            return True
        return False
        
    def operator_ne(self, field, value):
        if field != value:
            return True
        return False
    
    def operator_in(self, field, value):
        if field in value:
            return True
        return False
    
    def operator_not_in(self, field, value):
        if field not in value:
            return True
        return False