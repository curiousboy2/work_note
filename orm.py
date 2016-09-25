from sqlalchemy import Column,String,Integer,create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base=declarative_base()

class myOrm(Base):
    __tablename__='users'
    id=Column(Integer,primary_key=True)
    username=Column(String(20))
    md5_password=Column(String(32))
    def __init__(self,id,username,md5_password):
        self.id=id
        self.username=username
        self.md5_password=md5_password


engine=create_engine('mysql+pymysql://user:passwd@localhost:3306/hello')
DBSession=sessionmaker(engine)
session=DBSession()
#session.add(myOrm(20,'lee','huanghu'))   #插入数据
#session.commit()
user_list=session.query(myOrm).filter(myOrm.id>4).all()#查询数据
print([user.username for user in user_list])
session.close()
