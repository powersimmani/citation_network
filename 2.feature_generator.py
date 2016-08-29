#-*-encoding: utf-8 -*-
# ipython notebook --ip 0.0.0.0 --port 9999
#import networkx as nx

#db.author.find().limit(-1).skip(2).next()
#limit와 skip만 있으면 나도 이제 원하는 문서를 랜덤하게 찾을 수 있다. 
#db connection은 10분 지나면 꺼진다. 
import numpy as np
import re
import os.path
import csv
from scipy.stats import rankdata,percentileofscore

import networkx as nx

import scipy.stats
import time
from pprint import pprint
from pymongo import MongoClient

from multiprocessing import Process, Queue
import os 
from pymongo.errors import BulkWriteError
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
	
	past_time = time.time()
	iterator = 0

	for collection_id in collection_client.find():
		if (iterator%100000 == 0):
			print collection + " cited_count downloading : " + str(iterator) + "/" + str(collection_client.count()) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()


		cited_counts_years = [[] for i in range(0,66)]

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


		for year in range(0,66):
			sort = sorted(map(int,cited_counts_years[year]),reverse=True)
			if (len(sort) >1):
				for i in range(0,len(sort)):
					if (i > sort[i]):
						author_h_index[collection_id["_id"]][year]= i
						break
			elif len(sort)==1:
				if sort[0] > 0:
					author_h_index[collection_id["_id"]][year]= 1
		iterator += 1

	for author_id in author_h_index:
		collection_client.update({"_id":author_id},{"$set":{"h_index":author_h_index[author_id],"last_modified":time.time()}})


#================ network feature ====================
#cascading network(가중치가 인용수인 네트워크)를 그려 보는것은 어떨까?
#network이 제대로 구성되어있는지도 확인해야 한다. 
#여러가지를 고려해서 다시 만들어보자 
#일단 연도별로 나누어서 올리는 작업을 해야 하나?

def make_network_feature_array_on_db(ip,port,db,collection_load,collection_save):
	#멀티 프로세싱을 위해 사전에 db에 저장공간을 마련하고 
	#네트워크 값들을 계산할 때 마다 0이 아닌 경우를 업데이트 해준다. 
	#이 경우 db의 부담이 줄어들고 멀티프로세싱 리턴값을 받지 않아도 되기 떄문에 편리하다. 
	#꽤 오래 걸린다. 
	con_load = MongoClient(ip, port)[db][collection_load]
	con_save = MongoClient(ip, port)[db][collection_save]

	cen_name_list = ["in_degree","degree","eigenvector","pagerank"]
		#collection_client.update({},{"$set":{cen_type:[0.0]*66}},False,True)
	
	for document in con_load.find():
		for cen_type in cen_name_list:
			con_load.update({"_id":document['_id']}, {'$unset': {cen_type:1}}, multi=True)


	"""
	for document in con_load.find():
		con_save.save({"_id":document['_id']})
		for cen_type in cen_name_list:
			con_save.update({"_id":document['_id']},{"$set":{cen_type:[0.0]*66}})
	"""

def cal_network_value_multiprocessor(ip,port,db,con_save,cen_type, SG,year):
	#어느정도 비율로 이게 있는지 다 0 인 애들이 있는데 이것들은 어쩔건지
	#똑같이 db에 들어간 애들중에 자료 없는 애들이 얼마나 있는지 대략적인 분포를 알고싶다. 
	#이후에 centrality별로 feature들 계산해서 새로 올리는 그거 만들면 될 듯 어차피 db에 저장되니 확실히 편하긴 하다. 
	#
	print "worker on "+str(cen_type) + " "+str(year) + " start"


	for ed_tuple in SG.edges():
		#여기서 weight를 설정할 수 있다. 
		weight = 1.0
		weight = max(SG.node[ed_tuple[0]]['cited_count_sum'][year-1950],0.5)
		SG.edge[ed_tuple[0]][ed_tuple[1]]['weight'] = weight


	print "worker on "+str(cen_type) + " "+str(year) + " centrality start"
	cen_list = "error"
	if (cen_type == "in_degree"):
		cen_list = nx.in_degree_centrality(SG)
	elif (cen_type == "degree"):
		cen_list = nx.degree_centrality(SG)
	elif (cen_type == "eigenvector"):
		cen_list = nx.eigenvector_centrality(SG)
	elif (cen_type == "pagerank"):
		cen_list = nx.pagerank(SG)

	collection_client = MongoClient(ip, port)[db][con_save]

	bulkop = collection_client.initialize_ordered_bulk_op()

	print "worker on "+str(cen_type) + " "+str(year) + " bulk and send start"
	
	for paper_id in cen_list:
		value = cen_list[paper_id]
		if value == 0.0:
			continue
		#bulkop.find({"_id":paper_id}).update({"$set":{ cen_type + "." + str(year-1950) :value}})
		collection_client.update({"_id":paper_id},{"$set":{ cen_type + "." + str(year-1950) :value}})


def network_uploader(ip,port,db,con_load,con_save,cen_type):
	collection_client = MongoClient(ip, port)[db][con_load]
	# paper에서 network부분만 떼어내어서 폴더를 만들고 만들어낸다. 
	# 이를 이용헤 네트워크를 만들어서 올려둔다? 아니면 해당연도에 속한 자료만 
	#그냥 db에서 연도가 n년 이하인 경우만 끌어오면 되는거 아닌가?
	#설마 1980으로 해놔서 문제가 있을수도 있나? 아닌가?...일단 해보고 결정하자 
	#network이 80년대꺼니까 그럴 수 있다....없는 데이터가 더 많을 수 있지...

	G = nx.DiGraph()
	cursor = collection_client.find({"year":{"$gte":1950,"$lt":2016}},{"year":1, "cite":1,"cited_count_sum":1})
	count = cursor.count()
	iterator = 0
	past_time = time.time()
	
	for collection_id in cursor:#--------------
		#이걸로 한시름 놓았군 좋아좋아  네트워크 만들어서 들이대면 될 듯 
		#그럼 만들어진 것들은 연도별로 모아서 다시 데이터별로 올리는 방법을 쓰자 
		#매번 포문 돌리면서 계속해서 구하면 반복해서 뭐 더할 필요도 없고 연도마다 추가되는 네트워크에 대해서만 계산 때리면 되니 엄청 효율적이다.
		if (iterator%100000 == 0):
			print cen_type + " graph making : " + str(iterator) + "/" + str(count) +" takes: " + str((time.time() - past_time)) + " seconds"
			past_time = time.time()
		iterator += 1

		source = collection_id["_id"]

		#이미 있는 노드의 경우? ->알아서 갱신해줌 따라서 연도정보가 없는 target을 알아내기 위해 target의 데이터에 -1을 추가하였다. 
		G.add_node(source,{"year":collection_id["year"],
							cen_type:[0.0]*66,
							"cited_count_sum" : collection_id["cited_count_sum"]
							})

		for target in collection_id["cite"]:
			if target not in G:
				G.add_node(target,{"year":-1})
			G.add_edge(source,target,{"year":collection_id["year"]})

	year_divide = [[1950,1951,1957,1963,1969,1975,1981,1987,1993,1999,2005],
					[1952,1958,1964,1970,1976,1982,1988,1994,2000,2006,2011],
					[1953,1959,1965,1971,1977,1983,1989,1995,2001,2007,2012],
					[1954,1960,1966,1972,1978,1990,1984,1996,2002,2008,2013],
					[1955,1961,1967,1973,1979,1985,1991,1997,2003,2009,2014],
					[1956,1962,1968,1974,1980,1986,1992,1998,2004,2010,2015]]

	#year_divide = [[1980]]
	if __name__=='__main__':

		jobs = []
		#multiprocessing을 쓰려 하였으나 메모리를 너무 많이 소모해 문제가 많아서 프로세스 2개만 쓴느걸로 해결...
		#for year in range(2000,2016):
		for year in range(1950,2000):
			SG=G.subgraph( [n for n,attrdict in G.node.items() if attrdict['year'] <= year ] )
			cal_network_value_multiprocessor(ip,port,db,con_save,cen_type, SG,year)
			"""
		 	for year_array in year_divide:
		 		p = Process(target=cal_network_value_multiprocessor, args = (ip,port,db,con_save,cen_type, SG,year))
				jobs.append(p)

			for process in jobs:
				process.start()

			for process in jobs:
				process.join()
			"""

def network_feature_extractor(ip,port,db,con,cen_type_list):
	collection_client = MongoClient(ip, port)[db][con]
	#일단 feature저장하는 부분을 만들었는데 이거 나중에 한번 더 돌려야 한다. 
	#지금은 저장공간만 확보하다는 개념으로 돌리는거 

	bulkop = collection_client.initialize_ordered_bulk_op()

	for document in collection_client.find():#--------------
		add_docu = {}
		for cen_type in cen_type_list:
			document[cen_type]

			add_docu[cen_type +"max"] = [0.0]*66
			add_docu[cen_type +"sum"] = [0.0]*66
			add_docu[cen_type +"top"] = [0.0]*66
			add_docu[cen_type +"slope"] = [0.0]*66

			for year in range(1950-1950,2016-1950):
				#4대 요소 계산 
				add_docu[cen_type +"max"][year] = max(document[cen_type][:year+1])
				add_docu[cen_type +"sum"][year]  = sum(document[cen_type][:year+1])

				cnt = 0
				start = 0
				for i in document[cen_type]:
					if i != 0.0 and start == 0:
						start = cnt
					if max(document[cen_type][:year+1]) == i:
						break
					cnt += 1

				add_docu[cen_type +"top"][year]  = cnt-start

				#여기를 좀 손많이 봐야할 듯 
				val_slope = 0
				for i in range(start+1, year):
					val_slope = max(val_slope,document[cen_type][i]-document[cen_type][i-1])
				add_docu[cen_type +"slope"][year]  = val_slope

				retval = bulkop.update({"_id":document["_id"]},{"$set":{ "features" : add_docu}})
				#이상하면 add_docu에 ["_id"]붙여서 전역으로 써야 한다. 

	retval = bulkop.execute()

def test(ip,port,db):
	pass


#최근 업데이트된 정보를 볼 수 있도록 시간을 추가하였다. 
#이거 다 되면 author한번 더 돌릴 수 있도록 한다. 

ip = "127.0.0.1"
ip = "lamda.ml"
port= 27017
db = "DBLP_Citation_network_V8"
#각 collection들의 연도별 cited count를 만들어 저장 -> ranking용 
#collection_cited_count_maker(ip,port,db,"author")
#collection_cited_count_maker(ip,port,db,"venue")

#collection_rank_maker(ip,port,db,"author")
#collection_rank_maker(ip,port,db,"venue")

#author_h_index_maker(ip,port,db,"author")

#"in_degree"
#"degree"
#"eigenvector"
#"pagerank"

centrality_list = ["in_degree","degree","eigenvector","pagerank"]
#make_network_feature_array_on_db(ip,port,db,"paper","network")
for centrality in centrality_list:
	network_uploader(ip,port,db,"paper","network",centrality)


#network_feature_extractor(ip,port,db,"network",centrality_list)
#test(ip,port,db)
#3. venue rank 구하기


