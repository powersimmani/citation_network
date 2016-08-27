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

import networkx as nx

import scipy.stats
import time
from pprint import pprint
from pymongo import MongoClient

from multiprocessing import Process, Queue
import os 
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
		if (iterator%10000 == 0):
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

def make_network_feature_array_on_db(ip,port,db,collection):
	#멀티 프로세싱을 위해 사전에 db에 저장공간을 마련하고 
	#네트워크 값들을 계산할 때 마다 0이 아닌 경우를 업데이트 해준다. 
	#이 경우 db의 부담이 줄어들고 멀티프로세싱 리턴값을 받지 않아도 되기 떄문에 편리하다. 
	#꽤 오래 걸린다. 
	collection_client = MongoClient(ip, port)[db][collection]

	cen_name_list = ["in_degree","degree","eigenvector","pagerank"]
		#collection_client.update({},{"$set":{cen_type:[0.0]*66}},False,True)
	
	for document in collection_client.find():
		for cen_type in cen_name_list:
			collection_client.update({"_id":document['_id']},{"$set":{cen_type:[0.0]*66}})
		

def cal_network_value_multiprocessor(ip,port,db,collection,cen_type, G,year_array):
	print "worker on"
	for year in year_array:
		SG=G.subgraph( [n for n,attrdict in G.node.items() if attrdict['year'] <= year ] )

		print "-"*60
		for ed_tuple in SG.edges():
			#여기서 weight를 설정할 수 있다. 
			weight = 1.0
			weight = max(SG.node[ed_tuple[0]]['cited_count_sum'][year-1950],0.5)
			SG.edge[ed_tuple[0]][ed_tuple[1]]['weight'] = weight
		
		printer = [edge for edge in SG.edges(data=True) if int(edge[2]['weight']) >1 ]

		cen_list = "error"
		try:
			if (cen_type == "in_degree"):
				cen_list = nx.in_degree_centrality(SG)
			elif (cen_type == "degree"):
				cen_list = nx.degree_centrality(SG)
			elif (cen_type == "eigenvector"):
				cen_list = nx.eigenvector_centrality(SG)
			elif (cen_type == "pagerank"):
				cen_list = nx.pagerank(SG)
		except:
			print "error in calculate centrality"


		collection_client = MongoClient(ip, port)[db][collection]

		for paper_id in cen_list:
			value = cen_list[paper_id]
			if value == 0.0:
				continue
			collection_client.update({"_id":paper_id},{"$set":{ cen_type + "." + str(year-1950) :value}})


def network_uploader(ip,port,db,collection,cen_type):
	collection_client = MongoClient(ip, port)[db][collection]
	# paper에서 network부분만 떼어내어서 폴더를 만들고 만들어낸다. 
	# 이를 이용헤 네트워크를 만들어서 올려둔다? 아니면 해당연도에 속한 자료만 
	#그냥 db에서 연도가 n년 이하인 경우만 끌어오면 되는거 아닌가?

	G = nx.DiGraph()
	
	for collection_id in collection_client.find({"year":{"$gte":1950,"$lt":1980}},{"year":1, "cite":1,"cited_count_sum":1}):#--------------
		#이걸로 한시름 놓았군 좋아좋아  네트워크 만들어서 들이대면 될 듯 
		#그럼 만들어진 것들은 연도별로 모아서 다시 데이터별로 올리는 방법을 쓰자 
		#매번 포문 돌리면서 계속해서 구하면 반복해서 뭐 더할 필요도 없고 연도마다 추가되는 네트워크에 대해서만 계산 때리면 되니 엄청 효율적이다.
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

	year_divide = [[1958]]
	if __name__=='__main__':

		jobs = []
   
	 	for year_array in year_divide:
	 		p = Process(target=cal_network_value_multiprocessor, args = (ip,port,db,collection,cen_type, G,year_array))
			jobs.append(p)

		for process in jobs:
			process.start()

		for process in jobs:
			process.join()




def test(ip,port,db):
	import sys
	#시간넣기 
	#필터로 노드를 걸러내고(연도별로)
	#네트워크를 만들었을 떄 당연히 인디그리에서 연도가 아닌것들은 사리지겠지.
	#필터가 되는지 그래서 인디그리가 사라지는지 
	enter = 0
	for i in range(1950,2016):
		sys.stdout.write(str(i) + "\t")
		if (i%6 == 0):
			sys.stdout.write("\n")

	pass



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

#author_h_index_maker(ip,port,db,"author")

#"in_degree"
#"degree"
#"eigenvector"
#"pagerank"

centrality = "in_degree"
#make_network_feature_array_on_db(ip,port,db,"paper")
network_uploader(ip,port,db,"paper",centrality)

#test(ip,port,db)
#3. venue rank 구하기


