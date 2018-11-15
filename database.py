import datetime

from sqlalchemy import Column, ForeignKey, Integer, Text, String, create_engine, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import exists

Base = declarative_base()


class SynonymPostAssociation(Base):
    __tablename__ = 'synonym_post_association'
    synonym_id = Column(Integer, ForeignKey('synonym.id'), primary_key=True)
    post_id = Column(Integer, ForeignKey('post.id'), primary_key=True)


class Synonym(Base):
    __tablename__ = 'synonym'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    posts = relationship('Post', secondary=SynonymPostAssociation.__tablename__, back_populates='synonyms')

    def __repr__(self):
        return f'<Synonym {self.id}>'


class Post(Base):
    __tablename__ = 'post'
    id = Column(String(16), primary_key=True)
    contents = Column(Text)
    synonyms = relationship('Synonym', secondary=SynonymPostAssociation.__tablename__, back_populates='posts')
    date = Column(DateTime, nullable=False)


class RedditPost(Post):
    __tablename__ = 'redditpost'
    id = Column(String(16), ForeignKey('post.id'), primary_key=True)
    subreddit = Column(String(64), nullable=False)

    __mapper_args__ = {
        'polymorphic_identity': 'redditpost',
    }


class TrustpilotPost(Post):
    __tablename__ = 'trustpilotpost'
    id = Column(String(16), ForeignKey('post.id'), primary_key=True)
    user_ratings = Column(Integer, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity': 'trustpilotpost',
    }


engine = create_engine('sqlite:///posts.db')

Base.metadata.bind = engine

DBSession = sessionmaker()
DBSession.bind = engine
session = DBSession()

if __name__ == "__main__":
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    synonym = Synonym(name='test')
    session.add(synonym)
    session.flush()
    session.refresh(synonym)

    sample = RedditPost(id='test', date=datetime.datetime.utcnow(), contents='hej', subreddit='2007scape',
                        synonyms=[synonym])
    session.add(sample)

    session.commit()

    post = session.query(Post).filter_by(id='test').first()
    print(post)

    print(session.query(Post).filter_by(id='test').count())





