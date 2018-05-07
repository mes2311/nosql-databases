# Put the use case you chose here. Then justify your database choice:
''' 
I built a photo sharing app, and chose neo4j, a graph database for my backend.
Since graphs are a natural way to model many user actions in a network
(eg users and posts can be nodes and likes, comments etc can be edges)
Using a graph database made a lot of sense. It also makes querying intuitive
and efficient since our the queries we want all search for subgraphs. I am never
expecting to query for all photos of all users but I expect to query for
subgraphs like all photos of all people user X follows.
'''

# Explain what will happen if coffee is spilled on one of the servers in your cluster, causing it to go down.
'''
Neo4j has a two later clustering system with a core cluster handling reads and 
and outer cluster of replicas for more efficient read operations. Regardless of 
what kind of server goes down, there are multiple computers in each cluster so
that in the case of a hardware malfunction the system can keep on operating.

Hence, even if one of the servers goes down, our database will still be intact
if the above scenario happens.
'''

# What data is it not ok to lose in your app? What can you do in your commands to mitigate the risk of lost data?
'''
The most important data points are the users and their posts, which make up the 
foundation of the graph network. Without them, none of the network activities
can be recreated.

However, from a user perhaps I also believe that likes and comments are important
to try to keep safe, because people attach important value to the attention
they receive online and their popularity. Thus, losing one's likes and comments
would cause anger at the platform and users may quit. 

The rest like messages and notifications are less important.

MITIGATING RISKS:

Backups should be made regularly and 'fire drills' in restoring data can be made.
Additionally, having servers in different locations can mitigate the risk
of a physical accident hitting all of the servers 
'''


##### CODE BEGINS HERE #####
from neo4j.v1 import GraphDatabase
import datetime

# Connect to database
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=('neo4j','test'))

# Clear existing nodes and relationships
def clear_db():
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (n)"
             "DETACH DELETE n")

# Datetime to int
def date_to_int(date):
  date = str(date)
  date = int(date[0:4]+date[5:7]+date[8:10])
  return date

# Create user
def create_user(user):
  with driver.session() as session:
    session.run("CREATE (n:User {email:{email}, pword:{pword}, name:{name}, username:{username}, bio:{bio}, photo:{photo}})", email = user[0], pword = user[1], name = user[2], username = user[3], bio  = user[4], photo = user[5])  

# Create post
def create_post(post):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("CREATE (n:Post {pid: {pid}, image_URL: {image}, caption: {caption}, date:{date}})", pid = post[0], image = post[1], caption = post[2], date = post[3])

# Create tag
def create_tag(tag):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("CREATE (n:Tag {tag: {tag}})", tag = tag)


# Create tag-TAGGED-post relationship 
def tag_post(pid, tag):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:Tag), (b:Post)"
	     "WHERE a.tag = {tag} AND b.pid = {pid}"
	     "CREATE (a)-[r:TAGGED]->(b)", tag = tag, pid = pid)

# Create user-MAKES-post relationship 
def make_post(username, pid):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:Post)"
	     "WHERE a.username = {username} AND b.pid = {pid}"
	     "CREATE (a)-[r:MAKES]->(b)", username = username, pid = pid)

# Create user-LIKES-post relationship 
def like_post(username, pid):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:Post)"
	     "WHERE a.username = {username} AND b.pid = {pid}"
	     "CREATE (a)-[r:LIKES]->(b)", username = username, pid = pid)

# Create user-BOOKMARKS-post relationship 
def bookmark_post(username, pid):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:Post)"
	     "WHERE a.username = {username} AND b.pid = {pid}"
	     "CREATE (a)-[r:BOOKMARKS]->(b)", username = username, pid = pid)

# Create user-COMMENTS-post relationship 
def comment(username, comment, pid):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:Post)"
	     "WHERE a.username = {username} AND b.pid = {pid}"
	     "CREATE (a)-[r:COMMENTS {date: {date}, comment: {comment}}]->(b)", username = username, comment = comment, pid = pid, date = date_to_int(datetime.date.today()))

# Create user-FOLLOWS-user relationship 
def follow(follower, followed):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:User)"
	     "WHERE a.username = {u1} AND b.username = {u2}"
	     "CREATE (a)-[r:FOLLOWS]->(b)", u1 = follower, u2 = followed)

# Create user-MESSAGES-user relationship 
def message(sender, message, receiver):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:User)"
	     "WHERE a.username = {u1} AND b.username = {u2}"
	     "CREATE (a)-[r:MESSAGES {date: {date}, message: {message}}]->(b)", u1 = sender, message = message, u2 = receiver, date = date_to_int(datetime.date.today()))

# Create notification and send it to user
def create_notif(text, target, nid):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("CREATE (n:Notification {nid: {nid}, date: {date}, content: {text}, target_URL: {t}})", text = text, t = target, nid = nid, date = date_to_int(datetime.date.today())) 
	   
def notify(user, nid):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      tx.run("MATCH (a:User), (b:Notification)"
	   "WHERE a.username = {username} AND b.nid = {nid}"
   	   "CREATE (a)-[r:RECEIVED]->(b)", username = user, nid = nid)
     
# See all notifications from last week
def see_notifs_last_week(user):
  today = date_to_int(datetime.date.today())
  with driver.session() as session:
    with session.begin_transaction() as tx:
      for record in tx.run("MATCH (a:User)-[:RECEIVED]->(b) "
	     		   "WHERE a.username = {username} AND ( ({today} - b.date) <= 7 " 
		           "OR (({today} - b.date) >= 70 AND ({today} - b.date) <= 77) )"
 	     		   "RETURN b.content", username = user, today = today):
	print(record["b.content"])

# See all photos from last week of people you follow
def see_photos_last_week(user):
  today = date_to_int(datetime.date.today())
  with driver.session() as session:
    with session.begin_transaction() as tx:
      for record in tx.run("MATCH (a:User)-[:FOLLOWS]->(b)-[:MAKES]->(p) "
	     		   "WHERE a.username = {username} AND (  ({today} - p.date) <= 7 " 
		           "OR (({today} - p.date) >= 70 AND ({today} - p.date) <= 77) )"
 	     		   "RETURN b.username, p.caption, p.date, p.image_URL", username = user, today = today):
	print(record["b.username"] + ', ' + str(record["p.date"]) + ', ' + record["p.caption"] + ', ' + record["p.image_URL"])

# See messages between two users
def see_messages(send, recv):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      for record in tx.run("MATCH (a:User)-[m:MESSAGES]->(b) "
	     		   "WHERE (a.username = {u1} AND b.username = {u2}) " 
		           "OR (a.username = {u2} AND b.username = {u1})"
 	     		   "RETURN m.message", u1 = send, u2 = recv):
	print(record["m.message"])



# See all photos of a user
def see_photos_of(user):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      for record in tx.run("MATCH (a:User)-[:MAKES]->(b) "
	     		   "WHERE a.username = {username}" 
 	     		   "RETURN b.caption, b.date, b.image_URL", username = user):
	print(str(record["b.date"]) + ', ' + record["b.caption"] + ', ' + record["b.image_URL"])


# See all photos with a tag
def see_tagged(tag):
  with driver.session() as session:
    with session.begin_transaction() as tx:
      for record in tx.run("MATCH (a:Tag)-[:TAGGED]->(b) "
	     		   "WHERE a.tag = {tag}" 
 	     		   "RETURN b.caption, b.date, b.image_URL", tag = tag):
	print(str(record["b.date"]) + ', ' + record["b.caption"] + ', ' + record["b.image_URL"])


###### MAIN PROGRAM #######
if __name__ == "__main__": 
  clear_db()  
  # today = datetime.date.today()

  # Create USERS
  mehmet = ('mes2311@columbia.edu', 'qwerty', 'Mehmet Sonmez', 'madmax', None, 'url1')
  gage = ('ggh2111@columbia.edu', 'abcdef', 'Gage Hodgen', 'theTruther', 'I seek nothing but the truth', 'url2')
  katherine = ('ks3310@columbia.edu', '123456', 'Katherine Strong', 'kittykat', 'NYC Livin - Varsity Frisbee', 'url3')

  users = [mehmet, gage, katherine]
  for user in users:
    create_user(user)

  # Create POSTS
  post1 = (1,"https://www.instagram.com/p/v6yv-FJ2bL/?hl=en&taken-by=mehmetsonmez","Pictured in South Africa" , date_to_int(datetime.date(2018, 5, 3)))
  post2 = (2,"https://www.instagram.com/p/u8SyEjp2e5/?hl=en&taken-by=mehmetsonmez","Tides crash against an island home to sea lions in the Atlantic Ocean. Pictured just outside False Bay, South Africa.", date_to_int(datetime.date(2018, 5, 1)))
  post3 = (3,"https://www.instagram.com/p/BA5rmQZhTM3/?hl=en&taken-by=gagehodgen","I didn't believe that it really got cold in New York.", date_to_int(datetime.date(2018, 4, 30)))
  post4 = (4,"https://www.instagram.com/p/BclkypFDRycECL3B_ECrgQ5ZFHsO_HifhL3IqU0/?hl=en&taken-by=kittykat_strong","Smiling, but only softly", date_to_int(datetime.date(2018, 5, 2)))
 
  posts = [post1, post2, post3, post4]
  for post in posts:
    create_post(post)

  # Create TAGS
  tags = ['nature', 'wildlife','NYC', 'portrait']
  for tag in tags:
    create_tag(tag)
 
  # Create tag-TAGGED-post relationships
  tag_post(1, 'nature')
  tag_post(1, 'wildlife')
  tag_post(2, 'nature')
  tag_post(3, 'NYC')
  tag_post(4, 'portrait')

  # Create user-MAKES-post relationships
  make_post('madmax', 1)
  make_post('madmax', 2)
  make_post('theTruther', 3)
  make_post('kittykat', 4)

  # Create user-LIKES-post relationships
  like_post('madmax', 3)
  like_post('madmax', 4)
  like_post('theTruther', 4)
  like_post('kittykat', 1)
  like_post('kittykat', 2)

  # Create user-COMMENTS-post relationships
  comment('kittykat', 'wow what a photo', 1)
  comment('kittykat', 'where was this taken?', 1)
  comment('madmax', 'lookin good my man', 3)
  comment('theTruther', 'derp-a derp derp, lol', 4)

  # Create user-BOOKMARKS-post relationships
  bookmark_post('madmax', 1)
  bookmark_post('kittykat', 1)
  bookmark_post('kittykat', 3)

  # Create user-FOLLOWS-user relationships
  follow('madmax', 'kittykat')
  follow('madmax', 'theTruther')
  follow('theTruther', 'madmax')
  follow('kittykat', 'madmax')
  follow('kittykat', 'theTruther')

  # Create user-MESSAGES-user relationships
  message('madmax', 'Did you see Katherines new photo?', 'theTruther')
  message('theTruther', 'Yea, I even left a comment haha', 'madmax')
  message('madmax', 'Cool cool', 'theTruther')

  # Create NOTIFICATIONS and  user-RECEIVES-notification relationships

  create_notif('madmax liked your photo', 'https://www.instagram.com/p/BA5rmQZhTM3/?hl=en&taken-by=gagehodgen', 1)
  notify('theTruther', 1)

  create_notif('madmax liked your photo', 'https://www.instagram.com/p/BclkypFDRycECL3B_ECrgQ5ZFHsO_HifhL3IqU0/?hl=en&taken-by=kittykat_strong', 2)
  notify('kittykat', 2)
  
  create_notif('theTruther liked your photo', 'https://www.instagram.com/p/BclkypFDRycECL3B_ECrgQ5ZFHsO_HifhL3IqU0/?hl=en&taken-by=kittykat_strong', 3)
  notify('kittykat', 3)
  
  create_notif('kittykat liked your photo', 'https://www.instagram.com/p/v6yv-FJ2bL/?hl=en&taken-by=mehmetsonmez', 4)
  notify('madmax', 4)

  create_notif('kittykat liked your photo', 'https://www.instagram.com/p/u8SyEjp2e5/?hl=en&taken-by=mehmetsonmez', 5)
  notify('madmax', 5)

  create_notif('kittykat commented on your photo', 'https://www.instagram.com/p/v6yv-FJ2bL/?hl=en&taken-by=mehmetsonmez', 6)
  notify('madmax', 6)

  create_notif('kittykat commented on your photo', 'https://www.instagram.com/p/v6yv-FJ2bL/?hl=en&taken-by=mehmetsonmez', 7)
  notify('madmax', 7)

  create_notif('madmax commented on your photo', 'https://www.instagram.com/p/BA5rmQZhTM3/?hl=en&taken-by=gagehodgen', 8) 
  notify('theTruther', 8)

  create_notif('theTruther commented on your photo', 'https://www.instagram.com/p/BclkypFDRycECL3B_ECrgQ5ZFHsO_HifhL3IqU0/?hl=en&taken-by=kittykat_strong', 9)
  notify('kittykat', 9)

  create_notif('madmax started following you', 'https://www.instagram.com/mehmetsonmez/', 10)
  notify('kittykat', 10)

  create_notif('madmax started following you', 'https://www.instagram.com/mehmetsonmez/', 11)
  notify('theTruther', 11)

  create_notif('theTruther started following you', 'https://www.instagram.com/gagehodgen/', 12)
  notify('madmax', 12)

  create_notif('kittykat started following you', 'https://www.instagram.com/kittykat_strong/', 13)
  notify('madmax', 13)

  create_notif('kittykat started following you', 'https://www.instagram.com/kittykat_strong/', 14)
  notify('theTruther', 14)

### APPLICATION ACTIONS ###

# 1: User signs up for an account

  emily = ['emstolfo@gmail.com', 'nosqldb', 'Emily Stolfo', 'estolfo', 'Im a ruby engineer at MongoDB', 'profile_photo_URL']
  create_user(emily)

# 2: A user follows another user 

  follow('estolfo', 'madmax')
  follow('estolfo', 'kittykat')

# 3: A user comments on a photo

  comment('estolfo', 'Do you sell prints?', 1)

# 4: A user sends a message to another user

  message('estolfo', 'Which database is your favorite?', 'madmax')

# 5: A user sees all their notifications from last week
  print('Notifications for madmax from the last week \n') 
  see_notifs_last_week('madmax')

# 6: A user sees all the photos of the people they follow from last week
  print('\nPhotos from last week for people followed by estolfo\n')
  see_photos_last_week('estolfo')

# 7: A user sees all their messages with a person 
  print('\nMessages between users madmax and theTruther\n')
  see_messages('madmax', 'theTruther')

# 8: A user sees all photos of a person they follow
  print('\nAll photos of a particular user (e.g madmax)\n')
  see_photos_of('madmax')

# 9: A user views all photos with a certain tag
  print('\nSee all photos with a particular tag (e.g portrait)\n')
  see_tagged('portrait')

