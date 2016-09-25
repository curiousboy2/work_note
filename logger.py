import logging
from datetime import datetime

class myLogger(logging.Logger):
    def __init__(self,name):
        super(myLogger,self).__init__(name)
        self.setLevel(logging.DEBUG)
        today=datetime.now().strftime('%Y-%m-%d')
        fh=logging.FileHandler('d:\\jeff\\log\\{}.log'.format(today))
        fh.setLevel(logging.ERROR)
        ch=logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter=logging.Formatter("%(asctime)s\t\tuser: %(name)s\t\tlevel: %(levelname)s\t\treason: %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.addHandler(fh)
        self.addHandler(ch)

if __name__ == '__main__':
    logger=myLogger('jeff')
    try:
        f=open('001.txt')
    except Exception as e:
        logger.error(e)
