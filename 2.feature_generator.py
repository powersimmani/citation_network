#-*-encoding: utf-8 -*-
# ipython notebook --ip 0.0.0.0 --port 9999
#import networkx as nx

#db.author.find().limit(-1).skip(2).next()
#limit와 skip만 있으면 나도 이제 원하는 문서를 랜덤하게 찾을 수 있다. 
import numpy as np
import re
import os.path
import csv
from scipy.stats import rankdata,percentileofscore

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

	past_time = time.time()
	iterator = 0
	for collection_id in collection_client.find():
		collection_cited_count = [0]*66

		published = collection_id["published"]
		collection_cited = {}

		if (iterator%10000 == 0):
			print collection + " cited_count uploading : " + str(iterator) + "/" + str(collection_client.count()) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()
		iterator += 1


		for year in published:
			item_list= published[year]
			
			for item_id in item_list:
				#최종인용수를 만들고 그 사이의 랭킹을 구한다. 
				item_cited_count = item_client.find_one({"_id":str(item_id)})["cited_count_sum"]
				for i in range(0,66):
					collection_cited_count[i] += item_cited_count[i]
				#print item_cited_count

		"""
		#for test
		print "-"*50
		print collection_id["_id"]
		print collection_cited_count
		print "="*50
		print input()
		"""
		collection_client.update({"_id":collection_id["_id"]},{"$set":{"cited_count":collection_cited_count,"last_modified":time.time()}})

def collection_rank_maker(ip,port,db,collection):
	collection_client = MongoClient(ip, port)[db][collection]
	#각 collection의 인용수를 기준으로 순위와 백분위를 정한다. 
	past_time = time.time()
	iterator = 0
	collection_cited_count = [[]]*collection_client.count()
	collection_rank = {}
	id_iter_map = {}
	iter_id_map = {}
	#rank는 percentile로 구해야 의미가 있을 것 같은데 아닌가?

	for collection_id in collection_client.find():
		if (iterator%10000 == 0):
			print collection + " cited_count downloading : " + str(iterator) + "/" + str(collection_client.count()) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		collection_cited_count[iterator] = collection_id["cited_count"]		

		#collection rank에 이름 채워넣을라고 집어넣은거 
		collection_rank[collection_id["_id"]] = [0]*66

		id_iter_map[collection_id["_id"]] = iterator
		iter_id_map[iterator] = collection_id["_id"]
		#print collection_cited_count[iterator]
		iterator+=1



	#일반 rank를 구한다. 
	for year in range(0,66):
		print collection + " calculate ranking : "+ str(year) + "/" + str(66) +" takes: " + str((time.time() - past_time)) + " seconds"

		past_time = time.time()		

		cited_count_for_rank = [-item[year] for item in collection_cited_count]
		cited_count_for_percentile = [item[year] for item in collection_cited_count]

		cited_rank = rankdata(cited_count_for_rank)
		
		#순위를 구한다. 
		for item_id in id_iter_map:
			index = id_iter_map[item_id]
			collection_rank[item_id][year] = cited_rank[index]

	for item_id in id_iter_map:
		index = id_iter_map[item_id]
		collection_client.update({"_id":item_id},{"$set":{"rank":collection_rank[item_id],"last_modified":time.time()}})


def author_h_index_maker(ip,port,db,collection):
	collection_client = MongoClient(ip, port)[db][collection]
	item_client = MongoClient(ip, port)[db]["paper"]

	author_h_index = {}
	cited_counts_years = [[] for i in range(0,66)]
	past_time = time.time()
	iterator = 0

	for collection_id in collection_client.find():
		if (iterator%10000 == 0):
			print collection + " cited_count downloading : " + str(iterator) + "/" + str(collection_client.count()) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		if (collection_id["_id"] not in author_h_index):
			author_h_index[collection_id["_id"]] = [0]*66
		published = collection_id["published"]

		for year in published:
			item_list= published[year]
			
			for item_id in item_list:
				#최종인용수를 만들고 그 사이의 랭킹을 구한다. 
				item = item_client.find_one({"_id":str(item_id)})
				item_cited_count = item["cited_count_sum"]
				start_year = int(year) -1950
				for i in range(start_year,66):
					cited_counts_years[i].append(item_cited_count[i])


		for year in range(50,66):
			sort = sorted(map(int,cited_counts_years[year]),reverse=True)
			for i in range(0,len(sort)):
				print sort
				print "i 	: " + str(i)
				print "sort[i]	: " + str(sort[i])
				if (i > sort[i]):
					author_h_index[collection_id["_id"]][year]= i
					continue
			print "="*100
			print author_h_index[collection_id["_id"]]
			input()


def test(ip,port,db):
	#시간넣기 
	collection_client = MongoClient(ip, port)[db]["test"]
	#collection_client.save({"_id":"1", "time":time.time(),"temp":"2312312"})


	collection_rank = {}

	collection_cited_count = [[8],[9],[1],[1],[1],[7],[7],[8],[9]]
	#일반 rank와 percentile rank를 같이 구한다. 
	for year in range(0,1):

		past_time = time.time()		

		cited_count_for_rank = [-item[year] for item in collection_cited_count]
		cited_count_for_percentile = [item[year] for item in collection_cited_count]
		print cited_count_for_rank
		print cited_count_for_percentile

		cited_rank = rankdata(cited_count_for_rank)
		#순위와 백분위를 구한다. 

		print cited_rank




	for item_id in id_iter_map:
		index = id_iter_map[item_id]
		collection_client.update({"_id":item_id},{"$set":{"rank":collection_rank[item_id],"rank_percent":collection_rank_percentile[item_id],"last_modified":time.time()}})




#최근 업데이트된 정보를 볼 수 있도록 시간을 추가하였다. 
#이거 다 되면 author한번 더 돌릴 수 있도록 한다. 

ip = "127.0.0.1"
port= 27017
db = "DBLP_Citation_network_V8"
#각 collection들의 연도별 cited count를 만들어 저장 -> ranking용 
#collection_cited_count_maker(ip,port,db,"author")
#collection_cited_count_maker(ip,port,db,"venue")

#collection_rank_maker(ip,port,db,"author")
#collection_rank_maker(ip,port,db,"venue")


author_h_index_maker(ip,port,db,"author")
#test(ip,port,db)
#3. venue rank 구하기


