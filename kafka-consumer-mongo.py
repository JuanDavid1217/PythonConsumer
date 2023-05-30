# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = "mongodb+srv://JuanDavid1217:JuanDavid#1712@cluster0.m3ei4fv.mongodb.net/?retryWrites=true&w=majority";
#Local = "mongodb://127.0.0.1:27017"
#URL del profe = "mongodb+srv://adsoft:adsoft-sito@cluster0.kzghgph.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri)
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.fisicoculturismo
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")


#Para consumir el topic reactions
consumerReactions = KafkaConsumer('reactions',bootstrap_servers=['my-kafka.kubernetes-kafka-juandavid1217.svc.cluster.local:9092']) 
for msg in consumerReactions:
    record = json.loads(msg.value)
    print(record)

    info = {'name':record['name'],
    'publication':record['publication'],
    'reaction':record['reaction']}

    try:
        info_id = db.reactions_info.insert_one(info)
        print("Data inserted with record ids", info_id)
    except:
        print("Could not insert into MongoDB")

#Para consumir el topic comments
consumerComments = KafkaConsumer('comments',bootstrap_servers=['my-kafka.kubernetes-kafka-juandavid1217.svc.cluster.local:9092']) 
for msg in consumerComments:
    record = json.loads(msg.value)
    print(record)

    info = {'name':record['name'],
    'publication':record['publication'],
    'comment':record['comment']}

    try:
        info_id = db.comments_info.insert_one(info)
        print("Data inserted with record ids", info_id)
    except:
        print("Could not insert into MongoDB")



#Esto es el profe
#consumer = KafkaConsumer('test',bootstrap_servers=['my-kafka.kubernetes-kafka-juandavid1217.svc.cluster.local:9092'])#'my-kafka-0.my-kafka-headless.kafka-adsoftsito.svc.cluster.local:9092'])
# Parse received data from Kafka
#for msg in consumer:
 #   record = json.loads(msg.value)
  #  print(record)
   # name = record['name']

    # Create dictionary and ingest data into MongoDB
    #try:
     #  meme_rec = {'name':name }
      # print (meme_rec)
       #meme_id = db.memes_info.insert_one(meme_rec)
       #print("Data inserted with record ids", meme_id)
    #except:
     #  print("Could not insert into MongoDB")

    #try:
     #   agg_result = db.memes_info.aggregate(
      #      [{
       #         "$group" : 
        #        { "_id" : "$name",
         #         "n" : {"$sum":1}}
          #  }]
        #)
        #db.memes_summary.delete_many({})
        #for i in agg_result:
         #   print(i)
          #  summary_id = db.memes_summary.insert_one(i)
           # print("Summary inserted with record ids", summary_id)
       #meme_rec = {'name':name }
       #print (meme_rec)
       #meme_id = db.memes_info.insert_one(meme_rec)
       #print("Data inserted with record ids", meme_id)
    #except Exception as e:
     #   print(f'group by caught {type(e)}: ')
      #  print(e)
       #print("Could not insert into MongoDB")
