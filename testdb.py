import database
from database import Synonym, Post, SynonymPostAssociation, session
from datetime import datetime

#entity = database.Synonym(name='test')
#database.session.add(entity)
#database.session.commit()
#database.session.refresh(entity)


# syn = Synonym(name = 'theis')
# post = Post(date = datetime.now(), contents = 'ASDJKHASDKJSD')
# syn.posts.append(post)
# session.add(syn)

# session.commit()

syns = session.query(Synonym)
posts = session.query(Post)
rels = session.query(SynonymPostAssociation)

session.commit()

print(syns.count())
print(posts.count())
print(rels.count())
