import json
import threading
import time
import sys

import logging
import cfg
from classes.forwarder import Forwarder

def iniLogging(settings):
    loggers = ["forwarder", "mpsiem_queue", "output_socket", "monitoring"]
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        if settings[logger_name]['level']=="DEBUG":
            logger.setLevel(logging.DEBUG)
        elif settings[logger_name]['level']=="INFO":
            logger.setLevel(logging.INFO)
        elif settings[logger_name]['level']=="WARNING":
            logger.setLevel(logging.WARNING)
        elif settings[logger_name]['level']=="ERROR":
            logger.setLevel(logging.ERROR)
        console_handler = logging.StreamHandler()
        log_format = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s]: %(message)s')
        console_handler.setFormatter(log_format)
        logger.addHandler(console_handler)
        logger.info('Logger "{}" initialized'.format(logger_name))

def main():
    forwarder = Forwarder(cfg.settings)
    forwarder.run()
    print(' [*] Waiting for messages. To exit press CTRL+C')
     

if __name__ == "__main__":
    print(cfg.settings)
    iniLogging(cfg.settings['logging'])
    main()
