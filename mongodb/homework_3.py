# Mehmet Emre Sonmez 
# @mes2311

import pymongo
from pymongo import MongoClient
import pprint

# Create a client
client = MongoClient()
db = client.test

# Set up variables
collection = db['movies']


#results = collection.find({"rated": "Pending rating"})
#for movie in results:
#  pprint.pprint(movie)


# A. Update all movies with "NOT RATED" at the "rated" key to be "Pending rating". The operation must be in-place and atomic.

collection.update_many(
  { "rated": "NOT RATED" },
  { "$set": {"rated": "Pending rating"}}
)

# B. Find a movie with your genre (short) in imdb and insert it into your database with the fields listed in the hw description.

'''
collection.insert_one(
  { "title": "The Silent Child",
    "year": 2017,
    "countries": ["USA"],
    "genres": ["Short","Drama"],
    "directors": ["Chris Overton"],
    "imdb": { "id": 6186970, "rating": 7.7, " votes": 1077 }
  }
)
'''

# C. Use the aggregation framework to find the total number of movies in your genre.

results = collection.aggregate([
  {"$match": {"genres": {"$all": ["Short"]}} },
  {"$count": "count"},
  {"$project": {"_id": "Short", "count": "$count"}}
])
for item in results:
  pprint.pprint(item)

# Example result:
#  => [{"_id"=>"Comedy", "count"=>14046}]


# D. Use the aggregation framework to find the number of movies made in the country you were born in with a rating of "Pending rating".

results = collection.aggregate([
  {"$match": {"countries": {"$all": ["Turkey"]}, "rated": "Pending rating" }},
  {"$count": "count"},
  {"$project": {"_id": {"country": "Turkey", "rating": "Pending rating"},      "count": "$count"}}
])

for item in results:
  pprint.pprint(item)

# Example result when country is Hungary:
#  => [{"_id"=>{"country"=>"Hungary", "rating"=>"Pending rating"}, "count"=>9}]


# E. Create an example using the $lookup pipeline operator. See hw description for more info.

# Create courses collection
db.courses.insert_one(
  { 
    "title": "Data Structures",
    "department": "Computer Science",
    "credits": 3,
  }
)

# Create instructors collection
db.instructors.insert_many(
  [
    {
	"name": "Paul Blaer",
	"course": "Data Structures",
	"semester": "Spring 2018"
    },
    {
	"name": "Daniel Bauer",
	"course": "Data Structures",
	"semester": "Fall 2017"
    },
    {
	"name": "Jae Woo Lee",
	"course": "Advanced Programming",
	"semester": "Spring 2018"
    }
  ]
)

# Perform lookup

results = db.courses.aggregate([
{"$lookup":
  {
	"from": "instructors",
	"localField": "title",
	"foreignField": "course",
	"as": "taught_by"
  }
}
])

for item in results:
  pprint.pprint(item)
