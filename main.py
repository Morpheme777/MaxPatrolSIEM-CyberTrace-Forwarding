import json
import threading
import time
import sys

import logging

from classes.forwarder import Forwarder

def iniLogging():
    loggers = ["forwarder", "mpsiem_queue", "output_socket", "monitoring"]
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        log_format = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s]: %(message)s')
        console_handler.setFormatter(log_format)
        logger.addHandler(console_handler)
        logger.info('Logger "{}" initialized'.format(logger_name))

def main():
    forwarder = Forwarder("settings")
    forwarder.run()
    print(' [*] Waiting for messages. To exit press CTRL+C')
     

if __name__ == "__main__":
    iniLogging()
    main()
