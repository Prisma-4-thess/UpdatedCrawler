#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import opendata
import json
import sys

query_size = 500;
page = 0;


def printAllTypes (response):
	'''Print all the types' labels

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	allDictionaries = response["decisionTypes"]
	for i in allDictionaries:
		print i["label"]


def printAllDictionaries (response):
	'''Print all the dictionaries' labels

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	allDictionaries = response["dictionaries"]
	for i in allDictionaries:
		print i["label"]


def printAllDecisionsPDF (response):
	'''Print the urls of all decisions

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	file = open("out", "a")
	decisions = response["decisions"]
	for decision in decisions:
		for i in decision:
			if (i==u'url'):
				# print i,":",decision[i]
				file.write(decision[i].encode('utf-8')+"/document"+"\n")
	file.close()

def printInfo (response):
	'''Return the total size of decisions

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	info = response["info"]
	for i in info:
		# print i,":", info[i]
		if (i==u'total'):
			return info[i]

def main(argv=None):
	client = opendata.OpendataClient("https://diavgeia.gov.gr/luminapi/opendata")
	print "***TYPES***"
	response = client.get_decision_types()
	printAllTypes(response)
	print "***DICTIONARIES***"
	response = client.get_dictionaries()
	printAllDictionaries(response)
	# q = "submissionTimestamp:[DT(2006-03-01T00:00:00) TO DT(2014-11-11T23:59:59)] AND (organizationUid:\"6114\")"
	# response = client.get_advanced_search_results(q,page,query_size)
	# total = printInfo (response)
	# print (total)
	# steps = total/query_size
	# print (steps)
	# print ("***ACTUAL DECISIONS***")
	# printAllDecisionsPDF(response)
	# for x in range(1,steps+1):
		# print("Page ",x)
		# response = client.get_advanced_search_results(q,x,query_size)
		# printAllDecisionsPDF(response)
	return 0

if __name__ == "__main__":
	sys.exit(main())