#!/usr/bin/python

import sys
import pafy
import logging
import requests
import json
from sets import Set
from datetime import datetime, timedelta
import time
import MySQLdb
from bs4 import BeautifulSoup

api_key     = "INSERT YOUR YOUTUBE API KEY HERE"
DB_ADDRESS  = "INSERT YOUR DATABASE ADDRESS HERE"
DB_USERNAME = "INSERT YOUR DATABASE USERNAME HERE"
DB_PASSWORD = "INSERT YOUR DATABASE PASSWORD HERE"
DB_DATABASE = "INSERT YOUR DATABASE NAME HERE"

pafy.set_api_key(api_key)

def youtube_search_by_time(keyword, start_date, period):
	db = MySQLdb.connect(DB_ADDRESS, DB_USERNAME, DB_PASSWORD, DB_DATABASE)
	cursor = db.cursor()
	cursor.execute("SET NAMES utf8mb4;") #or utf8 or any other charset you want to handle
	cursor.execute("SET CHARACTER SET utf8mb4;") #same as above
	cursor.execute("SET character_set_connection=utf8mb4;") #same as above	
	
	init_url = "https://www.googleapis.com/youtube/v3/search"
	params = {
		"part": "snippet",
		"maxResults": 50,
		"order": "date",
		"q": keyword,
		"relevanceLanguage": "en",
		"topicId": "/m/0bzvm2",		#Gaming
		"type": "video",
		"key": api_key
	}
	
	vid_id_set = Set([])
	
	for i in range(0, period):
		base_date = datetime.strptime(start_date, "%Y-%m-%d")
		current_date = base_date + timedelta(days = i)
		for hour in range(0, 24):
			start_timestamp = current_date.strftime("%Y-%m-%dT") + str(hour) + ":00:00Z"
			end_timestamp = current_date.strftime("%Y-%m-%dT") + str(hour) + ":59:59Z"
			params["publishedAfter"] = start_timestamp
			params["publishedBefore"] = end_timestamp
			page = 1
			
			#Query this hour
			while True:
				actual_vid_count = 0
				
				response = requests.get(init_url, params = params)
				while (response.status_code != requests.codes.ok):
					err_code = str(response.status_code)
					err_msg = json.loads(response.text)["error"]["message"]
					logging.error('[ERROR]\t request error code: ' + err_code + ' error msg: ' + err_msg + '. Waiting 10 mins...')
					time.sleep(600)
					response = requests.get(init_url, params = params)
				response_json = json.loads(response.text)
				
				for item in response_json["items"]:
					# parse each result
					html_doc = requests.get("https://www.youtube.com/watch?v=" + item["id"]["videoId"])
					video_page = BeautifulSoup(html_doc.text, "lxml")
					try:
						game_title = video_page.find(class_="watch-info-tag-list").li.a.get_text()
						if game_title != keyword:
							continue
					except:
						continue
					
					actual_vid_count += 1
					vid_id_set.add(item["id"]["videoId"])
					sql = "INSERT INTO non_steam_vid (youtube_id, game_title) VALUES ('" + item["id"]["videoId"] + "', '" + keyword + "')"
					try:
						cursor.execute(sql)
						db.commit()
					except MySQLdb.Error, e:
						if e.args[0] == 1062:
							continue
						else:
							logging.error("[ERROR] SQL: " + sql + " , error: " + str(e))
					
				info_str = "Timeperiod: " + start_timestamp + " - " + end_timestamp + "\tPage: " + str(page) + \
								"\tVidCount: " + str(len(response_json["items"])) + "\tGameVidCount: " + str(actual_vid_count) + "\tTotalCount: " + str(len(vid_id_set))
				logging.info(info_str)
				print(info_str)
				
				if len(response_json["items"]) < 50 or "nextPageToken" not in response_json:
					break
				else:
					params["pageToken"] = response_json["nextPageToken"]
					page += 1

def youtube_search_all(keyword):
	init_url = "https://www.googleapis.com/youtube/v3/search"
	params = {
		"part": "snippet",
		"maxResults": 50,
		"order": "date",
		"publishedBefore": "2017-12-14T00:00:00Z",
		"q": keyword,
		"relevanceLanguage": "en",
		"topicId": "/m/0bzvm2",		#Gaming
		"type": "video",
		"key": api_key
	}
	
	response = requests.get(init_url, params = params)
	while (response.status_code != requests.codes.ok):
		err_code = str(response.status_code)
		logging.error('[ERROR]\t request error code: ' + err_code + '. Waiting 60s...')
		time.sleep(60)
		response = requests.get(init_url, params = params)
	response_json = json.loads(response.text)

	last_timestamp = None
	last_id = "PLACEHOLDER"
	vid_id = Set([])

	while True:
		if len(response_json["items"]) > 0:
			if (len(response_json["items"]) == 1 and last_id == response_json["items"][0]["id"]["videoId"]):
				logging.info("Task finished. Total: " + str(len(vid_id)))
				return
			for item in response_json["items"]:
				# parse each result
				vid_id.add(item["id"]["videoId"])
				last_timestamp = item["snippet"]["publishedAt"][:-5] + 'Z'
				last_id = item["id"]["videoId"]
		
			if "nextPageToken" in response_json:
				params["pageToken"] = response_json["nextPageToken"]
				response = requests.get(init_url, params = params)

		else:
			if last_timestamp is None:
				logging.info("Task finished. Total: " + str(len(vid_id)))
				return
			params["publishedBefore"] = last_timestamp
			last_timestamp = None
			params.pop('pageToken', None)
			
		response = requests.get(init_url, params = params)
		while (response.status_code != requests.codes.ok):
			err_code = str(response.status_code)
			logging.error('[ERROR]\t request error code: ' + err_code + '. Waiting 60s...')
			time.sleep(60)
			response = requests.get(init_url, params = params)
		response_json = json.loads(response.text)

def main():
	keyword = sys.argv[1]
	start_date = sys.argv[2]
	period = sys.argv[3]
	logging.basicConfig(filename="youtube_" + keyword.replace(" ", "_") + ".log", level=logging.INFO)
	youtube_search_by_time(keyword, start_date, int(period))

if __name__ == '__main__':
	main()
