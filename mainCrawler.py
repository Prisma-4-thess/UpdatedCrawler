#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import opendata
import json
import sys


query_size = 500;
page = 0;

# def findAllOrgForPerifereia (response):
# 	'''Find all organizations that have as supervisor Perifereia Kentrikis Makedonias

# 	Arguments:
# 	response: a json response returned from OpendataClient 
# 	'''
# 	file = open("organizationsPeriferia", "w")
# 	organizations = response["organizations"]
# 	file.write("22898"+"\n")
# 	file.write("50205"+"\n")
# 	for org in organizations:
# 		if (org["supervisorId"]=="5009"):
# 			file.write(org["uid"]+"\n")
# 			print org["uid"]+":"+org["latinName"]
# 	file.close()

def printTypes (response,client):
	'''Print all the types' labels

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	d_types = response["decisionTypes"]
	for d_type in d_types:
		print "Type: "+d_type['uid']
		for key in d_type:
			if (isinstance(d_type[key], (bool))):
				print "\t"+key+": "+str(d_type[key])
			elif (d_type[key]!=None and key!='uid'):
				print "\t"+key+": "+d_type[key]
		printTypesDetails(client,d_type['uid'].encode('utf-8'))

def printTypesDetails(client,uid):
	'''Print details for a specific dictionary

	Arguments:
	client: OpendataClient instance
	uid: The Dictionary's uid
	'''
	response = client.get_decision_type_details(uid)
	# print(response)
	print '\t\tExtraFields:'
	wait = raw_input("PRESS TO CONTINUE...")
	for extrafield in response['extraFields']:
		print '\t\t\t'+extrafield['uid']
		for details in extrafield:
			print '\t\t\t\t'+details+': ',
			if (details=='fixedValueList' and extrafield[details]!=None):
				print (' ')
				for field in extrafield[details]:
					print '\t\t\t\t\t'+field
			elif(details=='nestedFields' and extrafield[details]!=None):
				print (' ')
				for field in extrafield[details]:
					print '\t\t\t\t\t'+field['uid']
					for nested_detail in field:
						if (nested_detail!='uid' and nested_detail!='nestedFields'):
							print '\t\t\t\t\t\t'+nested_detail+': ',
							print field[nested_detail]
						if(nested_detail=='nestedFields' and field[nested_detail]!=None):
							print (' ')
							for field2 in field[nested_detail]:
								print '\t\t\t\t\t\t\t'+field2['uid']
								for nested_detail2 in field2:
									if (nested_detail2!='uid'):
										print '\t\t\t\t\t\t\t\t'+nested_detail2+': ',
										print field2[nested_detail2]
			else: 
				print extrafield[details]
	if (response['parent']!=None): print '\t\tParent: '+response['parent']
	# print (allDictionaries)

def printDictionaryDetails(client,uid):
	'''Print details for a specific dictionary

	Arguments:
	client: OpendataClient instance
	uid: The Dictionary's uid
	'''
	response = client.get_dictionary(uid)
	# print(response)
	items = response["items"]
	for detail in items:
		for key in detail:
			if (detail[key]!=None):
				print '\t\t'+key+": "+detail[key]
	# print (allDictionaries)

def printAllDictionaries (response,client):
	'''Print all the dictionaries' labels

	Arguments:
	response: a json response returned from OpendataClient 
	client: OpendataClient instance
	'''
	allDictionaries = response["dictionaries"]
	for i in allDictionaries:
		print "Dictionary: "+i['uid']
		print '\t'+i["label"]
		printDictionaryDetails(client,i['uid'])



# def printAllDecisionsPDF (response,filename):
# 	'''Print the urls of all decisions

# 	Arguments:
# 	response: a json response returned from OpendataClient 
# 	'''
# 	file = open(filename, "a")
# 	decisions = response["decisions"]
# 	for decision in decisions:
# 		for i in decision:
# 			if (i==u'url'):
# 				# print i,":",decision[i]
# 				file.write(decision[i].encode('utf-8')+"/document"+"\n")
# 	file.close()

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

def printOrganizations (response):
	'''Return all organizations from diavgeia

	Arguments
	response: a json response returned from OpendataClient
	'''
	organizations = response["organizations"]
	for organization in organizations:
		print "Organization "+organization["uid"]
		for key in organization:
			# print organization[key]!=None
			# print key
			if (organization[key]!=None and key!='uid' and key!='organizationDomains'):
				print "\t"+key+": "+organization[key]

def printPositions (response):
	'''Return all positions from diavgeia

	Arguments
	response: a json response returned from OpendataClient
	'''
	positions = response["positions"]
	for position in positions:
		print "Position "+position["uid"]
		print "\tLabel: "+position["label"]
		

def main(argv=None):
	client = opendata.OpendataClient("https://diavgeia.gov.gr/luminapi/opendata")
	# print "***TYPES***"
	# print "***DICTIONARIES***"
	response = client.get_decision_types()
	# response = client.get_dictionaries()
	# response = client.get_organizations()
	# response = client.get_positions()
	printTypes(response,client)
	# printPositions(response)
	# printOrganizations(response)
	# printAllDictionaries(response,client)
	# print (response);
	
	# printAllDictionaries(response)
	# content = [line.strip() for line in open('organizationsPeriferia')]
	# for organization in content:
	# 	q = "submissionTimestamp:[DT(2006-03-01T00:00:00) TO DT(2014-11-11T23:59:59)] AND (organizationUid:"+organization+")"
	# 	print q;
	# 	response = client.get_advanced_search_results(q,page,query_size)
	# 	total = printInfo (response)
	# 	# print (total)
	# 	steps = total/query_size
	# 	# print (steps)
	# 	# print ("***ACTUAL DECISIONS***")
	# 	printAllDecisionsPDF(response,organization)
	# 	for x in range(1,steps+1):
	# 		print("Page ",x)
	# 		response = client.get_advanced_search_results(q,x,query_size)
	# 		printAllDecisionsPDF(response,organization)
	return 0

if __name__ == "__main__":
	sys.exit(main())