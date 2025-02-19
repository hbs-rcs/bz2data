import logging

class LogHandler(logging.Handler):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    def emit(self, record):
        with open(self.filename, 'a') as f:
            f.write(self.format(record) + '\n')


log_dict = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL}

def get_logger(level = 'INFO', log_file = './bz2data.log'):

    log_level = log_dict[level]
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    handler = LogHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    level_dict = {'DEBUG': logger.debug, 'INFO': logger.info, 'WARNING': logger.warning, 'ERROR': logger.error, 'CRITICAL': logger.critical}
    
    return level_dict[level]

## Log some messages
#logger.debug('This is a debug message')
#logger.info('This is an info message')
#logger.warning('This is a warning message')
#logger.error('This is an error message')
#logger.critical('This is a critical message')
