from __future__ import print_function
from flask import Flask, jsonify, request
from twitter import *
import sys
import json
from pymongo import MongoClient
from bson.json_util import dumps
import csv
import os
import time
import datetime


# token_secret=raw_input("Enter token_secret: ")
# consumer_secret=raw_input("Enter consumer_secret: ")

app = Flask(__name__)
client = MongoClient()
db = client['Twitter_Data']


# Please enter/hardcode your Twitter token,token_secret,consumer_key,consumer_secret here
token=''
token_secret=''
consumer_key=''
consumer_secret=''




@app.route('/',methods=['GET'])
def test():
	return jsonify({'message':'It Works'})
#--------------------------------------------------------------------------------------------------------------

# API 1: Search for a keyword sent in request
# Example used: http://localhost:8056/insert/messi
@app.route('/insert/<string:keyword>',methods=['GET'])
def addMongo(keyword):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	result=t.search.tweets(q=keyword,count='15',result_type='popular')
	status=result['statuses']
	# These are the fields I have decided to include
	for tweet in status:
		info = {}
		info['query']=keyword
		ts= time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
		info['creation']= ts
		info['id'] = tweet['id']
		info['text'] = tweet['text']
		user = tweet['user']
		info['user_url'] = user['url']
		info['user_id'] = user['id']
		info['user_name']=user['name']
		info['user_followers_count'] = user['followers_count']
		info['user_friends_count'] = user['friends_count']
		info['lang'] = user['lang']
		info['retweet_count'] = tweet['retweet_count']
		info['favorite_count'] = tweet['favorite_count']
		db.information.insert(info)
	return jsonify({'message':'Success! Inserted in <information> collection'})


#--------------------------------------------------------------------------------------------------------------
# API 2: Return stored tweets and their metadata 
# Simple get request- Enter the fieldname-eg-user_name, and keyword eg-mayank
# Example used: Example used: http://localhost:8056/get/query/messi
@app.route('/get/<string:field>/<string:keyword>',methods=['GET'])
def getMongo1(field,keyword):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	result=db.information.find({field:keyword})
	m = dumps(result)
	return m

# Sorting by date, tweet text, user name
# Example used: http://localhost:8056/get/query/messi/sort_by_date
@app.route('/get/<string:field>/<string:keyword>/sort_by_date',methods=['GET'])
def getMongo2(field,keyword):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	result=db.information.find({field:keyword}).sort('creation')
	m = dumps(result)
	return m	
# Example used: http://localhost:8056/get/query/messi/sort_by_text
@app.route('/get/<string:field>/<string:keyword>/sort_by_text',methods=['GET'])
def getMongo3(field,keyword):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	result=db.information.find({field:keyword}).sort('text')
	m = dumps(result)
	return m
# Example used: http://localhost:8056/get/query/messi/sort_by_name
@app.route('/get/<string:field>/<string:keyword>/sort_by_name',methods=['GET'])
def getMongo4(field,keyword):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	result=db.information.find({field:keyword}).sort('user_name')
	m = dumps(result)
	return m	


# Filter by date range, integer columns comparison or string matching
# Example used: http://localhost:8056/filter/date/2018/01/19
@app.route('/filter/date/<string:year>/<string:month>/<string:day>',methods=['GET'])
def getMongo5(year,month,day):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	d = datetime.datetime(int(year), int(month), int(day))
	result=db.information.find({"creation": {"$gt": d}})
	m = dumps(result)
	return m

# Use only for integer columns
# Example used: http://localhost:8056/filter/int/retweet_count/lt/500
# comp_operator: lt for less than, gt for greater than, eq for equal
@app.route('/filter/int/<string:field>/<string:comp_operator>/<string:rangedata>',methods=['GET'])
def getMongo6(field,rangedata,comp_operator):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	rangeofdata=int(rangedata)
	pp = []
	if (comp_operator=='gt'):
		result=db.information.find({field: {"$gt": rangeofdata}})
		for r in result:
			if 'creation' in r:
				pp.append(r)
	elif (comp_operator=='lt'):
		result=db.information.find({field: {"$lt": rangeofdata}})
		for r in result:
			if 'creation' in r:
				pp.append(r)
	else:
		result=db.information.find({field: {"$eq": rangeofdata}})
		for r in result:
			if 'creation' in r:
				pp.append(r)
	m = dumps(pp)
	return m

#  Example used: http://localhost:8056/filter/string/text/ronaldo/contains
#  typeofcomp: startswith, endswith,contains, exact
@app.route('/filter/string/<string:field>/<string:keyword>/<string:typeofcomp>',methods=['GET'])
def getMongo7(field,keyword,typeofcomp):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	if (typeofcomp=='startswith'):
		result=db.information.find({field: {'$regex' :'^'+keyword,'$options':'i'}})
	elif (typeofcomp=='endswith'):
		result=db.information.find({field: {'$regex' :keyword+'$','$options':'i'}})
	elif (typeofcomp=='contains'):
		result=db.information.find({field: {'$regex' :keyword,'$options':'i'}})
	else: #exact match default
		result=db.information.find({field:keyword})
	m = dumps(result)
	return m

#--------------------------------------------------------------------------------------------------------------

# API 3: Export 
# Filter by date range and export
# Example used: http://localhost:8056/filter/date/2018/01/19/export/daterange
@app.route('/filter/date/<string:year>/<string:month>/<string:day>/export/<string:filename>',methods=['GET'])
def Export1(year,month,day,filename):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	d = datetime.datetime(int(year), int(month), int(day))
	result=db.information.find({"creation": {"$gt": d}})
	keys = result[0].keys()
	k = [keys[8],keys[1],keys[2],keys[3],keys[6],keys[7],keys[10],keys[11],keys[12]]
	d = []
	x = []
	d.append(k)
	for r in result:
	 	x=[r[k[0]],r[k[1]],r[k[2]],r[k[3]],r[k[4]],r[k[5]],r[k[6]],r[k[7]],r[k[8]]]
	 	d.append(x)
	fileis=filename+'.csv' 	
	with open(fileis,'w') as fp:
		a=csv.writer(fp,delimiter=',')
		a.writerows(d)
	m = dumps(result)	
	return jsonify({'message':'Exported to CSV file'})

# Filter by integer columns and export
# Example used: http://localhost:8056/filter/int/retweet_count/lt/500/intcomp
@app.route('/filter/string/<string:field>/<string:keyword>/<string:typeofcomp>/export/<string:filename>',methods=['GET'])
def Export2(field,keyword,typeofcomp,filename):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	if (typeofcomp=='startswith'):
		result=db.information.find({field: {'$regex' :'^'+keyword,'$options':'i'}})
	elif (typeofcomp=='endswith'):
		result=db.information.find({field: {'$regex' :keyword+'$','$options':'i'}})
	elif (typeofcomp=='contains'):
		result=db.information.find({field: {'$regex' :keyword,'$options':'i'}})
	else: #exact match default
		result=db.information.find({field:keyword})
	
	keys = result[0].keys()
	k = [keys[8],keys[1],keys[2],keys[3],keys[6],keys[7],keys[10],keys[11],keys[12]]
	d = []
	x = []
	d.append(k)
	for r in result:
	 	x=[r[k[0]],r[k[1]],r[k[2]],r[k[3]],r[k[4]],r[k[5]],r[k[6]],r[k[7]],r[k[8]]]
	 	d.append(x)
	fileis=filename+'.csv' 	
	with open(fileis,'w') as fp:
		a=csv.writer(fp,delimiter=',')
		a.writerows(d)
	m = dumps(result)
	return jsonify({'message':'Exported to CSV file'})

# Filter by integer columns and export
#  Example used: http://localhost:8056/filter/string/text/ronaldo/contains/stringsearch
@app.route('/filter/int/<string:field>/<string:comp_operator>/<string:rangedata>/export/<string:filename>',methods=['GET'])
def export3(field,rangedata,comp_operator,filename):
	t = Twitter(auth=OAuth(token, token_secret, consumer_key, consumer_secret))
	rangeofdata=int(rangedata)
	pp = []
	if (comp_operator=='gt'):
		result=db.information.find({field: {"$gt": rangeofdata}})
		for r in result:
			if 'creation' in r:
				pp.append(r)
	elif (comp_operator=='lt'):
		result=db.information.find({field: {"$lt": rangeofdata}})
		for r in result:
			if 'creation' in r:
				pp.append(r)
	else:
		result=db.information.find({field: {"$eq": rangeofdata}})
		for r in result:
			if 'creation' in r:
				pp.append(r)

	keys = pp[0].keys()
	k = [keys[8],keys[1],keys[2],keys[3],keys[6],keys[7],keys[10],keys[11],keys[12]]
	d = []
	x = []
	d.append(k)
	for r in pp:
	 	x=[r[k[0]],r[k[1]],r[k[2]],r[k[3]],r[k[4]],r[k[5]],r[k[6]],r[k[7]],r[k[8]]]
	 	d.append(x)
	fileis=filename+'.csv' 	
	with open(fileis,'w') as fp:
		a=csv.writer(fp,delimiter=',')
		a.writerows(d)
	m = dumps(result)	
	return jsonify({'message':'Exported to CSV file'})


#--------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
     app.run(debug=True,port=8056)	


#--------------------------------------------------------------------------------------------------------------









