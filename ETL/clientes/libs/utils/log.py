import logging


def set_logger(name=__name__):
    # create logger
    logger = logging.getLogger(f'com.itau.desafiotecnico.{name}')

    # create console handler and set level to debug
    ch = logging.StreamHandler()

    # create formatter
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%y/%m/%d %H:%M:%S')

    # add formatter to ch
    ch.setFormatter(formatter)

    # set log level
    logger.setLevel(logging.INFO)

    # add ch to logger
    logger.addHandler(ch)
    return logger
