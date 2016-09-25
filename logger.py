import logging
import datetime

class custom_logger(logging.Logger):
    def __init__(self,name):
        super(custom_logger,self).__init__(name)
        today=datetime.datetime.now().strftime('%Y-%m-%d')
        fh = logging.FileHandler('%s.log' %today)
        fh.setLevel(logging.DEBUG)
        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.addHandler(fh)
        self.addHandler(ch)
