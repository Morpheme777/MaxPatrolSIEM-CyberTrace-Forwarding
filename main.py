import os
import json
import threading
import time
import sys
import logging

import cfg
from classes.forwarder import Forwarder

work_dir = os.path.dirname(os.path.realpath(__file__))

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
        mode = settings.get("mode", "console")
        if mode == "file":
            log_handler = logging.FileHandler('{}/{}'.format(work_dir, "forwarder.log"))
        else:
            log_handler = logging.StreamHandler()
        log_format = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s]: %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)
        logger.debug('Logger "{}" initialized'.format(logger_name))

def main():
    forwarder = Forwarder(cfg.settings)
    forwarder.run()
    print(' [*] Waiting for messages. To exit press CTRL+C')
     

if __name__ == "__main__":
    iniLogging(cfg.settings['logging'])
    main()
