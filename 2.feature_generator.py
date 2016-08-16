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

def collection_cited_count_maker(ip,port,db,collection):
	#collection의 연도별 인용수를 저장해주는 프로그램 
	collection_client = MongoClient(ip, port)[db][collection]
	item_client = MongoClient(ip, port)[db]["paper"]
	#1. 저자 정보를 가져오고 그의 출판기록을 본다. collection_id
	#그 출판기록에서 각 아이디들의 연도별 인용수 기록을 읽어서 리스트로 만들어둔다. item_id
	#랭킹을 만들어 추가한다. 

	collection_cited_count = {}

	past_time = time.time()
	iterator = 0
	for collection_id in collection_client.find():
		published = collection_id["published"]
		collection_cited = {}

		if (iterator%10000 == 0):
			print collection + " cited_count uploading : " + str(iterator) + "/" + str(collection_client.count()) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()
		iterator += 1
		
		if collection_id["_id"] not in collection_cited_count:
			collection_cited_count[collection_id["_id"]] = [0]*66

		for year in published:
			item_list= published[year]
			
			for item_id in item_list:
				#최종인용수를 만들고 그 사이의 랭킹을 구한다. 
				item_cited_count = item_client.find_one({"_id":str(item_id)})["cited_count_sum"]
				for i in range(0,66):
					collection_cited_count[collection_id["_id"]][i] += item_cited_count[i]

		collection_client.update({"_id":collection_id["_id"]},{"$set":{"cited_count":collection_cited_count[collection_id["_id"]],"last_modified":time.time()}})

def collection_rank(ip,port,db,collection):
	collection_client = MongoClient(ip, port)[db][collection]
	item_client = MongoClient(ip, port)[db]["paper"]
	
	past_time = time.time()
	iterator = 0
	collection_cited_count = [[]]*collection_client.count()
	collection_rank_percentile = {}
	id_iter_map = {}
	iter_id_map = {}
	#rank는 percentile로 구해야 의미가 있을 것 같은데 아닌가?

	for collection_id in collection_client.find():
		collection_cited_count[iterator] = collection_id["cited_count"]		

		#collection rank에 이름 채워넣을라고 집어넣은거 
		collection_rank[collection_id["_id"]] = [0]*66
		collection_rank_percentile[collection_id["_id"]] = [0]*66

		id_iter_map[collection_id["_id"]] = iterator
		iter_id_map[iterator] = collection_id["_id"]
		#print collection_cited_count[iterator]
		iterator+=1


	#일반 rank와 percentile rank를 같이 구한다. 
	for year in range(0,66):
		cited_count_for_rank = [-item[year] for item in collection_cited_count]
		cited_count_for_percentile = [item[year] for item in collection_cited_count]

		cited_rank = rankdata(cited_count_for_rank)
		
		#순위와 백분위를 구한다. 
		for item_id in id_iter_map:
			index = id_iter_map[item_id]
			collection_rank[item_id][year] = cited_rank[index]
			collection_rank_percentile[item_id][year] = percentileofscore(cited_count_for_percentile, index)

	print collection_rank
	input()
	print collection_rank_percentile
	input()

		"""
		collection_cited = {}

		if (iterator%10000 == 0 ):
			print collection + " cited_count uploading : " + str(iterator) + "/" + str(collection_client.count()) +" takes: " + str((time.time() - past_time)) + " seconds"
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
		"""

#최근 업데이트된 정보를 볼 수 있도록 시간을 추가하였다. 
#이거 다 되면 author한번 더 돌릴 수 있도록 한다. 

ip = "127.0.0.1"
port= 27017
db = "DBLP_Citation_network_V8"
#각 collection들의 연도별 cited count를 만들어 저장 -> ranking용 
#collection_cited_count_maker(ip,port,db,"author")
#collection_cited_count_maker(ip,port,db,"venue")

#collection_rank(ip,port,db,"author")
collection_rank(ip,port,db,"venue")


#test(ip,port,db)
#3. venue rank 구하기



def test(ip,port,db):
	#시간넣기 
	collection_client = MongoClient('lamda.ml', 27017)[db]["test"]
	#collection_client.save({"_id":"1", "time":time.time(),"temp":"2312312"})
	collection_client.update({"_id":"1"},{"$set":{"cited_count":"111112","last_modified":time.time()}})	
	collection_rank = {}
