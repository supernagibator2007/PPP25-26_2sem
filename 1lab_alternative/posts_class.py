from sqlalchemy import Column, Integer, Text
from sqlalchemy.orm import declarative_base


Base = declarative_base()

class Post(Base):
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    body = Column(Text)
    author = Column(Text)
    extracted_time = Column(Text)
