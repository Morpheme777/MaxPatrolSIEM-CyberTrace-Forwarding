import time
import json
import queue
import pika
import logging

class MPSiemQueue():
    def __init__(self, settings):
        self.log = logging.getLogger("mpsiem_queue")
        self.log.info(
            'Settings: host={}, username={}, password=***, queue_name={}'.format(
                settings.get("host"),
                settings.get("username"),
                settings.get("queue_name")))
        self.log.info('Additional settings: {}'.format(', '.join(
            ['{}={}'.format(k, v) for k, v in settings.items()]
        )))
        self.host = settings.get("host")
        self.username = settings.get("username")
        self.password = settings.get("password")
        self.queue_name = settings.get("queue_name")
        self.out = queue.Queue()
        self.initSettings(settings)
        self.chanel_status = False
        self.msg_counter = 0
        self.event_counter = 0
        
    def initSettings(self, settings):
        self.rmq_vhost = settings.get("rmq_vhost", "/")
        self.port = settings.get("port", 5672)
        self.timeout = settings.get("timeout", 30)
        self.ioc_fields = settings.get("ioc_fields")
        if self.ioc_fields:
            self.messageProcessing = self.messageProcessingByField
        else:
            self.messageProcessing = self.messageProcessingRawEvent
        self.field_mapping = settings.get("field_mapping", {})
        filter = settings.get("filter", [])
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
                log.warning('Condition {} is not valid: operator "{}" is not valid'.format(
                    str(condition),
                    condition['operator']
                ))
                continue
                
    def initChannel(self):
        while not self.chanel_status:
            self.log.info("Trying to connect..")
            try:
                self.log.info("RMQ channel initializing..")
                credentials = pika.PlainCredentials(self.username, self.password)
                connection_parameters = pika.ConnectionParameters(self.host, 
                                                                self.port, 
                                                                self.rmq_vhost, 
                                                                credentials)
                self.connection = pika.BlockingConnection(connection_parameters)
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)
                self.channel.queue_declare(self.queue_name, durable=True)
                self.log.info("RMQ channel initialized")
                self.chanel_status = True
            except Exception as e:
                self.log.error("RMQ channel initializing is failed: {}. Reconnection in 30 sec..".format(str(e)))
                time.sleep(self.timeout)
                self.chanel_status = False
    
    def messageProcessingByField(self, ch, method, properties, body):   
        events = json.loads(body.decode())
        self.msg_counter += 1
        for event in events:
            self.event_counter += 1
            if self.filterEvent(event):
                event_out = ' '.join(['{}={}'.format(self.field_mapping.get(field, field), str(event[field])) for field in set(event.keys()) & set(self.ioc_fields)])
                self.out.put(bytes(event_out+'\n', 'utf-8'))
    
    def messageProcessingRawEvent(self, ch, method, properties, body):   
        events = json.loads(body.decode())
        for event in events:
            if self.filterEvent(event):
                event_out = ' '.join([value for _, value in event.items()])
                self.out.put(bytes(event_out+'\n', 'utf-8'))
    
    def consume(self):
        self.initChannel()
        while True:
            try:
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.messageProcessing, auto_ack=True)
                self.channel.start_consuming()
            except Exception as e:
                self.log.warning("RMQ channel has lost connection: {}. Reconnection in 30 sec..".format(str(e)))
                self.channel.close()
                self.connection.close()
                time.sleep(self.timeout)
                self.initChannel()
    
    def filterEvent(self, event):
        for condition in self.filter:
            field = event.get(condition['field'], None)
            #if condition['field'] in event:
            #    field = event[condition['field']]
            #else:
            #    field = None
            if not condition['operator'](field, condition['value']):
                return False
        return True
    
    def operator_eq(self, field, value):
        if field != value:
            return False
        return True
        
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
