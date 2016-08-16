#-*-encoding: utf-8 -*-
# ipython notebook --ip 0.0.0.0 --port 9999
#import networkx as nx

#db.author.find().limit(-1).skip(2).next()
#limit와 skip만 있으면 나도 이제 원하는 문서를 랜덤하게 찾을 수 있다. 
import numpy as np
import re
import os.path
import csv
from scipy.stats import rankdata
import scipy.stats
import time
from pprint import pprint
from pymongo import MongoClient

#1. h-index구하기

#2. author rank 구하기
#해당연도까지의 총 인용수를 구해서 이를 순위로 만든다. 
#그 연도별 순위를 author에 저장한다. 
#근데 이건 h-index랑 거의 비슷 

def collection_cited_count(ip,port,db,collection):
	#collection의 연도별 인용수를 저장해주는 프로그램 
	collection_client = MongoClient('lamda.ml', 27017)[db][collection]
	item_client = MongoClient('lamda.ml', 27017)[db]["paper"]
	#1. 저자 정보를 가져오고 그의 출판기록을 본다. collection_id
	#그 출판기록에서 각 아이디들의 연도별 인용수 기록을 읽어서 리스트로 만들어둔다. item_id
	past_time = time.time()
	iterator = 0
	for collection_id in collection_client.find():
		published = collection_id["published"]
		collection_cited = {}

		if (iterator%10000 == 0):
			print collection + " cited_count uploading : " + str(iterator) + "/" + str(1900000) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()
		iterator += 1
		for year in published:
			item_list= published[year]
		
			for item_id in item_list:
				cited_count = item_client.find_one({"_id":str(item_id)})["cited_count"]
				#print str(item_id)+"'s cited_count: " + str(cited_count)
				#p=""
				for year in range(1950,2016):
					if (str(year) not in collection_cited):
						collection_cited[str(year)] = 0
					collection_cited[str(year)] += int(cited_count[str(year)])
					#p += ",\t" + str(collection_cited[str(year)])

				#print collection+"_cited " + str(collection_cited)
				#input()
		collection_client.update({"_id":collection_id},{"$set":{"cited_count":collection_cited,"last_modified":time.time()}})
  
def author_rank_H_index(ip,port,db):
	author_client = MongoClient('lamda.ml', 27017)[db]["author"]
	item_client = MongoClient('lamda.ml', 27017)[db]["paper"]
	
	past_time = time.time()
	iterator = 0
	cited_counts_per_years = []
	for author_id in author_client.find():
		published = author_id["published"]		
		collection_cited = {}

		if (iterator%10000 == 0 ):
			print collection + " cited_count uploading : " + str(iterator) + "/" + str(1900000) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()
		iterator += 1

	#저자가 발행한 논문들의 연도별 인용수 리스트가 필요하다 
		for year in published:
			item_list= published[year]
		
			for item_id in item_list:
				cited_count = item_client.find_one({"_id":str(item_id)})["cited_count"]
				#print str(item_id)+"'s cited_count: " + str(cited_count)
				#p=""
				for year in range(1950,2016):
					if (str(year) not in collection_cited):
						collection_cited[str(year)] = 0
					collection_cited[str(year)] += int(cited_count[str(year)])

def test(ip,port,db):
	#시간넣기 
	collection_client = MongoClient('lamda.ml', 27017)[db]["test"]
	#collection_client.save({"_id":"1", "time":time.time(),"temp":"2312312"})
	collection_client.update({"_id":"1"},{"$set":{"cited_count":"111112","last_modified":time.time()}})	



#최근 업데이트된 정보를 볼 수 있도록 시간을 추가하였다. 
#이거 다 되면 author한번 더 돌릴 수 있도록 한다. 

ip = "127.0.0.1"
port= 27017
db = "DBLP_Citation_network_V8"
#각 collection들의 연도별 cited count를 만들어 저장 -> ranking용 
collection_cited_count(ip,port,db,"author")
#collection_cited_count(ip,port,db,"venue")


author_rank_H_index(ip,port,db)



#test(ip,port,db)
#3. venue rank 구하기
