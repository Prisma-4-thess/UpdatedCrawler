#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import opendata
import json
import sys
import MySQLdb
import connection_string as con
import xml.etree.cElementTree as ET


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
	db = con.connectMySQL()
	cur = db.cursor()
	d_types = response["decisionTypes"]
	for d_type in d_types:
		print "Type: "+d_type['uid']
		for key in d_type:
			if (isinstance(d_type[key], (bool))):
				print "\t"+key+": "+str(d_type[key])
			elif (d_type[key]!=None and key!='uid'):
				print "\t"+key+": "+d_type[key]
		insertIntoTypes(db,cur,d_type)
		printTypesDetails(client,d_type['uid'].encode('utf-8'),d_type['uid'],db,cur)
	db.commit()
	db.close()

def insertIntoTypes(db,cursor,value):
	'''Insert types into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	'''
	fields = ['uid','label','parent','allowedInDecisions']
	SQLcommand = "insert into type(type_id,label,parent_id,allowed_in_decision) VALUES (%s,%s,%s,%s)"
	actuallInsertion(fields,SQLcommand,cursor,db,value)
	# cursor.execute(SQLcommand)
	# db.commit()

def printTypesDetails(client,uid,uid_database,db,cur):
	'''Print details for a specific dictionary

	Arguments:
	client: OpendataClient instance
	uid: The Dictionary's uid
	'''
	response = client.get_decision_type_details(uid)
	# print(response)
	print '\t\tExtraFields:'
	# wait = raw_input("PRESS TO CONTINUE...")
	for extrafield in response['extraFields']:
		print '\t\t\t'+extrafield['uid']
		insertIntoTypesDetails(db,cur,extrafield,uid_database)
		for details in extrafield:
			print '\t\t\t\t'+details+': ',
			if (details=='fixedValueList' and extrafield[details]!=None):
				print (' ')
				for field in extrafield[details]:
					print '\t\t\t\t\t'+field
			elif(details=='nestedFields' and extrafield[details]!=None):
				print (' ')
				for field in extrafield[details]:
					insertIntoTypesDetails(db,cur,field,uid_database)
					print '\t\t\t\t\t'+field['uid']
					for nested_detail in field:
						if (nested_detail!='uid' and nested_detail!='nestedFields'):
							print '\t\t\t\t\t\t'+nested_detail+': ',
							print field[nested_detail]
						if(nested_detail=='nestedFields' and field[nested_detail]!=None):
							print (' ')
							for field2 in field[nested_detail]:
								print '\t\t\t\t\t\t\t'+field2['uid']
								insertIntoTypesDetails(db,cur,field2,uid_database)
								for nested_detail2 in field2:
									if (nested_detail2!='uid'):
										print '\t\t\t\t\t\t\t\t'+nested_detail2+': ',
										print field2[nested_detail2]
			else: 
				print extrafield[details]
	if (response['parent']!=None): print '\t\tParent: '+response['parent']
	db.commit()

def insertIntoTypesDetails(db,cursor,value,uid):
	'''Insert extrafields into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	'''
	fields = ['uid','dictionary','help','label','maxLength','relAdaConstrainedInOrganization','searchTerm','type','validation']
	SQLcommand = "insert into extra_field(extraField_id,dictionary,help,label,max_length,rel_ada_constrained_in_organization,search_term,type,validation) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	actuallInsertion(fields,SQLcommand,cursor,db,value)
	# cursor.execute(SQLcommand)
	# db.commit()

def printDictionaryDetails(client,uid,db,cur):
	'''Print details for a specific dictionary

	Arguments:
	client: OpendataClient instance
	uid: The Dictionary's uid
	'''
	response = client.get_dictionary(uid)
	# print(response)
	items = response["items"]
	for detail in items:
		# for key in detail:
		# 	if (detail[key]!=None):
		# 		print '\t\t'+key+": "+detail[key]

		insertIntoDictionariesDetails(db,cur,detail,uid)
	# print (allDictionaries)

def insertIntoDictionariesDetails(db,cursor,value,uid):
	'''Insert dictionary details into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	'''
	fields = ['uid','label','parent','dict']
	SQLcommand = "insert into dictionary_item(dictionaryItem_id,label,parent_id,dictionary_id) VALUES (%s,%s,%s,%s)"
	value['dict'] = uid
	actuallInsertion(fields,SQLcommand,cursor,db,value)
	# cursor.execute(SQLcommand)
	# db.commit()

def printAllDictionaries (response,client):
	'''Print all the dictionaries' labels

	Arguments:
	response: a json response returned from OpendataClient 
	client: OpendataClient instance
	'''
	db = con.connectMySQL()
	cur = db.cursor()
	allDictionaries = response["dictionaries"]
	for i in allDictionaries:
		print "Dictionary: "+i['uid']
		# print '\t'+i["label"]
		insertIntoDictionaries(db,cur,i)
		printDictionaryDetails(client,i['uid'],db,cur)
	db.commit()
	db.close()

def insertIntoDictionaries(db,cursor,value):
	'''Insert dictionaries into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	'''
	fields = ['uid','label']
	SQLcommand = "insert into dictionary(dictionary_id,label) VALUES (%s,%s)"
	actuallInsertion(fields,SQLcommand,cursor,db,value)

def actuallInsertion(fields,SQLcommand,cursor,db,value):
	sql_val = []
	for field in fields:
		try:
			if (value[field]==None):
				sql_val.append(None)
			else:
				sql_val.append(value[field])
		except:
			sql_val.append(None)
	print (SQLcommand,sql_val)
	try:
		cursor.execute(SQLcommand,sql_val)
	except Exception as e:
		print 
		print (SQLcommand,sql_val)
		print e
	# print SQLcommand


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

def printOrganizations (response,client):
	'''Return all organizations from diavgeia

	Arguments
	response: a json response returned from OpendataClient
	client: OpendataClient instance
	'''
	db = con.connectMySQL()
	cur = db.cursor()
	organizations = response["organizations"]
	for organization in organizations:
		print "Organization "+organization["uid"]
		# for key in organization:
			# print organization[key]!=None
			# print key
			# if (organization[key]!=None and key!='uid' and key!='organizationDomains'):
				# print "\t"+key+": "+organization[key]
		insertIntoOrganizations(db,cur,organization)
		printUnits(client,organization["uid"],db,cur)
	db.commit()
	db.close()


def insertIntoOrganizations(db,cursor,value):
	'''Insert organizations into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	'''
	fields = ['uid','abbreviation','category','fekIssue','fekNumber','fekYear','label','latinName','odeManagerEmail','status','supervisorId','vatNumber','website']
	# SQLcommand = "insert into organization('organization_id','abbreviation','category_id','fek_issue_id','fek_number','fek_year','label','latin','ode_manager_email','status','supervisor_id','vat_number','website') VALUES ("+values+")"
	SQLcommand = "insert into organization(organization_id,abbreviation,category_id,fek_issue_id,fek_number,fek_year,label,latin_name,ode_manager_email,status,supervisor_id,vat_number,website) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	# print SQLcommand
	actuallInsertion(fields,SQLcommand,cursor,db,value)
	# cursor.execute(SQLcommand)
	# db.commit()

def printUnits(client,uid,db,cur):
	'''Print units for a specific organization

	Arguments:
	client: OpendataClient instance
	uid: The organization's uid
	'''
	units = client.get_organization_units(uid)['units']
	for unit in units:
		# print "\t\tUnit "+unit['uid']
		#for detail in unit:
		#	print "\t\t\t"+detail+': ',
		#	print unit[detail]
		insertIntoUnits(db,cur,unit,uid)
	db.commit()
		
def insertIntoUnits(db,cursor,value,uid):
	'''Insert units into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	'''
	fields = ['uid','label','category','active','myParentId','abbreviation']
	SQLcommand = "insert into unit(unit_id,label,category_id,active,parent_id,abbreviation) VALUES (%s,%s,%s,%s,%s,%s)"
	value['myParentId']=uid
	actuallInsertion(fields,SQLcommand,cursor,db,value)

def printPositions (response):
	'''Return all positions from diavgeia

	Arguments
	response: a json response returned from OpendataClient
	'''
	db = con.connectMySQL()
	cur = db.cursor()
	positions = response["positions"]
	for position in positions:
		print "Position "+position["uid"]
		print "\tLabel: "+position["label"]		
		insertIntoPositions(db,cur,position)
	db.commit()
	db.close()

def insertIntoPositions(db,cursor,value):
	'''Insert positions into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	'''
	fields = ['uid','label']
	SQLcommand = "insert into org_position(orgPosition_id,label) VALUES (%s,%s)"
	actuallInsertion(fields,SQLcommand,cursor,db,value)


def getGEO(csr):
	query = (
		 "INSERT INTO geo"
		 "(version,address,dimos,latitude,longitude,namegrk,new_cat,new_sub_cat,phone,tk)"
		 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")  

	tree = ET.ElementTree(file='poi_thessalonikis.kml')
	root = tree.getroot()
	child = root[0]
	print child
	folder=child[0]
	for member in folder:
		if member.tag == 'Placemark':
			#Structure if each Placemark Element
			name=member[0]
			ext_data=member[1]
			schemadata=ext_data[0]
			dimos = u'\u0394\u03ae\u03bc\u03bf\u03c2 \u0398\u03b5\u03c3\u03c3\u03b1\u03bb\u03bf\u03bd\u03af\u03ba\u03b7\u03c2'
			address = point_x = point_y = namegrk = newcat = newsubcat = phone = tk = None
			for parts in schemadata:
				if parts.attrib['name'] == 'tk':
					tk=parts.text
				if parts.attrib['name'] == 'newcat':
					newcat=parts.text
				if parts.attrib['name'] == 'phone':
					phone=parts.text
					if phone is "0":
						phone = None
				if parts.attrib['name'] == 'address':
					address=parts.text
				if parts.attrib['name'] == 'newsubcat':
					newsubcat=parts.text
				if parts.attrib['name'] == 'namegrk':
					namegrk=parts.text
			point=member[2]
			coord=point[0]
			coordstr=coord.text.split(',',2)
			coordX=coordstr[1]
			coordY=coordstr[0]
			data = (1,address,dimos,coordX,coordY,namegrk,newcat,newsubcat,phone,tk)
			# for temp in data:
			#   print (temp)
			try:
				csr.execute(query,data)
			except Exception, e:
				print (str(e))

def printSigners(response,uid):
	'''Print signers for a specific organization

	Arguments:
	client: OpendataClient instance
	uid: The organization's uid
	'''
	db = con.connectMySQL()
	cur = db.cursor()
	signers = response['signers']
	for signer in signers:
		# print "\t\tSigner "+signer['uid']
		# for detail in signer:
		# 	print "\t\t\t"+detail+': ',
		# 	print signer[detail]
		insertIntoSigners(db,cur,signer,uid)
	db.commit()
	db.close()
	# db.commit()

def insertIntoSigners(db,cursor,value,uid):
	'''Insert signers into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	uid: The uid of the organization
	'''
	fields = ['uid','active','activeFrom','activeUntil','firstName','hasOrganizationSignRights','lastName','myOrgId']
	SQLcommand = "insert into signer(signer_id,active,active_from,active_until,first_name,has_organization_sign_rights,last_name,org_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
	value['myOrgId'] = uid
	actuallInsertion(fields,SQLcommand,cursor,db,value)

def main(argv=None):
	client = opendata.OpendataClient("https://diavgeia.gov.gr/luminapi/opendata")	
	print "***DICTIONARIES***"
	response = client.get_dictionaries()
	printAllDictionaries(response,client)
	print "***POSITIONS***"
	response = client.get_positions()
	printPositions(response)
	print "***ORGANIZATIONS***"
	response = client.get_organizations(status='all')
	printOrganizations(response,client)
	print "***TYPES***"
	response = client.get_decision_types()
	printTypes(response,client)
	print "***GEO***"
	db = con.connectMySQL()
	cur = db.cursor()
	getGEO(cur)
	db.commit()
	db.close()
	print '***SIGNERS***'
	response = client.get_organization_signers('6114')
	printSigners(response,'6114')
	# printAllDictionaries(response,client)
	# response = client.get_organizations()
	
	# printTypes(response,client)
	# printOrganizations(response,client)
	# print (response);
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
