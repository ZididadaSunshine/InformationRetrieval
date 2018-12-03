import datetime
from contextlib import contextmanager
from sqlalchemy import Column, ForeignKey, Integer, Text, String, create_engine, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import os

Base = declarative_base()


class SynonymPostAssociation(Base):
    __tablename__ = 'synonym_post_association'
    synonym_id = Column(Integer, ForeignKey('synonym.id'), primary_key=True)
    post_id = Column(String(32), ForeignKey('post.id'), primary_key=True)


class Synonym(Base):
    __tablename__ = 'synonym'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    posts = relationship('Post', secondary=SynonymPostAssociation.__tablename__, back_populates='synonyms')

    def __repr__(self):
        return f'<Synonym {self.id}>'


class Post(Base):
    __tablename__ = 'post'

    id = Column(String(32), primary_key=True)
    contents = Column(Text, nullable=True)
    synonyms = relationship('Synonym', secondary=SynonymPostAssociation.__tablename__, back_populates='posts')
    date = Column(DateTime, nullable=False)
    author_id = Column(String(32), nullable=False)
    source = Column(String(50))
    sentiment = Column(Integer, nullable = True)
    __mapper_args__ = {
        'polymorphic_identity': 'post',
        'polymorphic_on': source
    }


class RedditPost(Post):
    __tablename__ = 'redditpost'

    id = Column(String(32), ForeignKey('post.id'), primary_key=True)
    subreddit = Column(String(64), nullable=False)

    def __repr__(self):
        return f'<RedditPost {self.id}>'

    __mapper_args__ = {
        'polymorphic_identity': 'redditpost',
    }


class TrustpilotPost(Post):
    __tablename__ = 'trustpilotpost'

    id = Column(String(32), ForeignKey('post.id'), primary_key=True)
    user_ratings = Column(Integer, nullable=False)

    def __repr__(self):
        return f'<TrustpilotPost {self.id}>'

    __mapper_args__ = {
        'polymorphic_identity': 'trustpilotpost',
    }


engine = create_engine(f'postgresql://{os.environ["DB_USERNAME"]}:{os.environ["DB_PASSWORD"]}@{os.environ["DB_HOST"]}/{os.environ["DB_DATABASE"]}')
Base.metadata.bind = engine

Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    print("Database re-created")

