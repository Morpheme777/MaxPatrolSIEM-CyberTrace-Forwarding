settings = {
    
    "consumer": {
        "host": '10.31.120.59',  # required
        "username": 'mpx_siem',  # required
        "password": 'P@ssw0rd',  # required
        "queue_name": 'cybertraceq',  # required
        "port": 5672,  # optional
        "rmq_vhost": '/',  # optional
        "timeout": 30,  # optional
        "ioc_fields": ['src.ip','dst.ip','src.host','dst.host','dst.port','object.path','object.hash','datafield1','recv_ipv4','event_src.host','event_src.title'],  # optional
        "filter": [{'field': 'event_src.title', 'operator': 'ne', 'value': 'cybertrace'}]  # optional
        "field_mapping": {
            "src.host": "src_host",
            "event_src.title": "event_src_title"
        }
    },

    "sender": {
        "host": '10.31.120.59',  # required
        "port": 9999,  # required
        "timeout": 30,  # required
        "start_flag": 'X-KF-SendFinishedEvent'  # required
    },

    "monitoring": {
        "timeout": 60  # optional
    },

# Logging settings:
# level: [DEBUG, INFO, WARNING, ERROR]
# mode: [console, file]
    "logging": {
        "mode": "file",  # optional, default: console
        #"log_path": "/var/log/",  # optional, default: script directory
        "forwarder": {
            "level": "INFO"
        }, 
        "mpsiem_queue": {
            "level": "INFO"
        }, 
        "output_socket": {
            "level": "INFO"
        }, 
        "monitoring": {
            "level": "INFO"
        }
    }
}