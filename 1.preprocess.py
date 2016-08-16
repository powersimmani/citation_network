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

class Item:
	def __init__(self):
		self.item_id = ''
		self.title = 'NULL'
		self.year = -1
		self.publisher = 'NULL'
		self.author = []
		self.cite = []
		
		self.abstract = ""

def processed_data(in_path,ip,port,db):

	paper_send = MongoClient(ip, port)[db]["paper"]

	start_time = time.time()
	temp_start_time = time.time()

	#파일을 입력받는다. 2016년 4월 최신버전 기준 2GB정도 된다.
	print "=============	Reading raw data start 	=============="

	f_in = open(in_path,"r")
	lines = f_in.readlines()
	f_in.close()

	print "=============	Reading raw data end 	=============="
	print("Reading raw data takes %s seconds" % (time.time() - temp_start_time))
	temp_start_time = time.time()

	#한줄씩 보며 데이터를 읽어들이고 저장한다. 
	print "=============	Parsing raw data start 	=============="
	#출력할 경로를 지정하여준다. 

	item_list = []
	item = Item()

	print len(lines)
	i = 0
	past_time = time.time()
	tot_i = 0
	for line in lines:

		if (i%1000000 == 0 ):
			print "send_processed_data_abstract: " + str(i) + "/" + str(len(lines)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		if len(line) > 1:
			flag = line[1]
			if (flag == "*"):
				item.title = line[2:].strip().replace("\t"," ").rstrip("\n")
			elif(flag == "@"):
				item.author = line[2:].split(",")
				item.author[-1] = item.author[-1].rstrip("\n")
				item.author = [author.strip() for author in item.author]
			elif(flag == "t"):
				item.year = int(line[2:].strip())
			elif(flag == "c"):
				item.publisher = line[2:].strip().replace("\t"," ").rstrip("\n")
			elif(flag == "i"):
				item.item_id = line[6:].strip().rstrip("\n")
			elif(flag == "%"):
				item.cite.append(line[2:].rstrip("\n"))
			elif(flag == "!"):
				item.abstract = line[2:].replace("\t"," ").rstrip("\n")
				item_list.append(item)
				item = Item()
				tot_i +=1

				#임시로 막아놓음
				#save_processed_data(item,out_path)

		i = i +1

	i = 0
	past_time = time.time()

	for i in range(0,tot_i):

		if (i%100000 == 0 ):
			print "sending_to_db: " + str(i) + "/" + str(tot_i) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		item = item_list[i]
		year = int(item.year)
		if (year > 2016 or year < 1950):
			continue
		paper_send.save({	"_id" : item.item_id,
				"title": item.title,
				"year": item.year,
				"authors": item.author,
				"cite": item.cite,
				"abstract": item.abstract,
				"venue": item.publisher,
				"last_modified":time.time()
				})

def author_collection(ip,port,db):
	item_id = MongoClient('lamda.ml', 27017)[db]["paper"]

	past_time = time.time()

	author_list = {}
	data = item_id.find()
	iterator = 0
	for i in item_id.find():
		if (iterator%100000 == 0 ):
			print "getting_item_info and make author : " + str(iterator) + "/" + str((1600000)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		for author in i["authors"]:
			if author not in author_list:
				author_list[author] = {}

			if (str(i["year"]) not in author_list[author]):
				author_list[author][str(i["year"])] = []

			author_list[author][str(i["year"])].append(i["_id"])
		iterator += 1

	author_id = MongoClient('lamda.ml', 27017)[db]["author"]

	iterator = 0

	for author in author_list:
		if (iterator%100000 == 0 ):
			print "uploading author : " + str(iterator) + "/" + str(len(author_list)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		author_id.save({"_id": author, "published": author_list[author],"last_modified":time.time()})
		iterator +=1

	#데이터를 가져와보자 
def venue_collection(ip,port,db):
	item_id = MongoClient('lamda.ml', 27017)[db]["paper"]
	past_time = time.time()
	venue_list = {}
	iterator = 0

	for i in item_id.find():
		if (iterator%100000 == 0 ):
			print "getting_item_info and make venue : " + str(iterator) + "/" + str((1600000)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()


		if i["venue"] not in venue_list:
			venue_list[i["venue"]] = {}

		if (str(i["year"]) not in venue_list[ i["venue"]]):
			venue_list[ i["venue"]][str(i["year"])] = []

		venue_list[i["venue"]][str(i["year"])].append(i["_id"])
		iterator += 1


	venue_id = MongoClient('lamda.ml', 27017)[db]["venue"]

	iterator = 0
	
	for venue in venue_list:
		if (iterator%100000 == 0 ):
			print "uploading venue : " + str(iterator) + "/" + str(len(venue_list)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		venue_id.save({"_id": venue, "published": venue_list[venue],"last_modified":time.time()})
		iterator +=1

def citation_count_per_year(ip,port,db):
	item_id = MongoClient('lamda.ml', 27017)[db]["paper"]
	past_time = time.time()
	iterator = 0

	cited_count = {}
	cited_list = {}
	for i in item_id.find():
		if (iterator%100000 == 0 ):
			print "getting_item_info and make cited_count : " + str(iterator) + "/" + str((1600000)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()


		#인용 수 구하기
		if (i["_id"] not in cited_count):
			#그냥 나온 paper를 cited_count에 등록
			cited_count[i["_id"]] = [0]*66

		for cited in i["cite"]:
			if (cited not in cited_count):
				#그 paper가 인용하는 아이들을 cited_count에 등록
				cited_count[cited] = [0]*66			
			#해당연도에 인용되었음을 +1로 표시한다. 
			cited_count[cited][(i["year"])-1950] += 1

		"""
		#인용 하는 아이들 리스트 구하기 -> 나중에 필요해지면 추가할 것 

		if (i["_id"] not in cited_list):
			#그냥 나온 paper를 cited_count에 등록
			cited_list[i["_id"]] = {}

		for cited in i["cite"]:
			if (cited not in cited_count):
				#그 paper가 인용하는 아이들을 cited_count에 등록
				cited_list[cited] = {}			
			if str(i["year"]) not in cited_list[cited]:
				cited_count[cited][str(i["year"])]= {}
			cited_count 		
		"""


		iterator += 1

	iterator = 0
	
	for item in cited_count:
		if (iterator%100000 == 0 ):
			print "uploading cited_count : " + str(iterator) + "/" + str(len(cited_count)) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()

		#연도별 인용수를 누적한다. 누적 인용수 구하기
		tot_cnt = 0
		cited_count_sum = [0]*66
		for i in range(0,66):
			tot_cnt += cited_count[item][i]
			cited_count_sum[i] = tot_cnt

		item_id.update({"_id":item},{"$set":{"cited_count":cited_count[item],"cited_count_sum":cited_count_sum,"last_modified":time.time()}})

		iterator +=1


#데이터 덤프를 데이터베이스에 전송
#데이터베이스 정보를 기반으로 새로운 collection인 author, venue를 생성
#데이터베이스 정보를 기반으로 cited_count생성 및 추가
#processed_data("../1.raw_data/acm.txt","lamda.ml",27017,"DBLP_Citation_network_V8")
#author_collection("lamda.ml",27017,"DBLP_Citation_network_V8")
#venue_collection("lamda.ml",27017,"DBLP_Citation_network_V8")
citation_count_per_year("lamda.ml",27017,"DBLP_Citation_network_V8")
