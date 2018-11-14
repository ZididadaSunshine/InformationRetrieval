import sqlalchemy 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, Text, String, Date, create_engine
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class SynonymPostAssociation(Base):
    __tablename__ = 'synonym_post_association'
    synonym_id = Column(Integer, ForeignKey('synonym.id'), primary_key = True)
    post_id  = Column(Integer, ForeignKey('post.id'), primary_key = True)


class Synonym(Base):
    __tablename__ = 'synonym'
    id    = Column(Integer, primary_key = True, autoincrement = True)
    name  = Column(String, unique = True)
    posts = relationship('Post', secondary = SynonymPostAssociation.__tablename__, back_populates = 'synonyms')

    def __repr__(self):
        return f'<Synonym {self.id}>'

class Post(Base): 
    __tablename__ = 'post'
    id = Column(Integer, primary_key = True, autoincrement = True)
    contents = Column(Text)
    synonyms = relationship('Synonym', secondary = SynonymPostAssociation.__tablename__, back_populates = 'posts')
    date     = Column(Date)

engine = create_engine('sqlite:///posts.db')

Base.metadata.bind = engine

DBSession = sessionmaker()
DBSession.bind = engine
session = DBSession()

Base.metadata.create_all(engine)
