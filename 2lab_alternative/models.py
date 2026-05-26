from sqlalchemy import create_engine, Column, Integer, Text
from sqlalchemy.orm import declarative_base, sessionmaker


URL = "sqlite:///../posts.db"

engine = create_engine(URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class Post(Base):
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    body = Column(Text)
    author = Column(Text)
    extracted_time = Column(Text)
