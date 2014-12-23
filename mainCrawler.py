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

# def printTypesDetails_old(client,uid,uid_database,db,cur):
# 	'''Print details for a specific dictionary

# 	Arguments:
# 	client: OpendataClient instance
# 	uid: The Dictionary's uid
# 	'''
# 	response = client.get_decision_type_details(uid)
# 	# print(response)
# 	print '\t\tExtraFields:'
# 	# wait = raw_input("PRESS TO CONTINUE...")
# 	for extrafield in response['extraFields']:
# 		print '\t\t\t'+extrafield['uid']
# 		enter1 = False
# 		for details in extrafield:
# 			print '\t\t\t\t'+details+': ',
# 			if (details=='fixedValueList' and extrafield[details]!=None):
# 				print (' ')
# 				for field in extrafield[details]:
# 					print '\t\t\t\t\t'+field
# 			elif(details=='nestedFields' and extrafield[details]!=None):
# 				print (' ')
# 				enter1 = True
# 				for field in extrafield[details]:
# 					enter2 = False
# 					print '\t\t\t\t\t'+field['uid']
# 					for nested_detail in field:
# 						if (nested_detail!='uid' and nested_detail!='nestedFields'):
# 							print '\t\t\t\t\t\t'+nested_detail+': ',
# 							print field[nested_detail]
# 						if(nested_detail=='nestedFields' and field[nested_detail]!=None):
# 							enter2 = True
# 							print (' ')
# 							for field2 in field[nested_detail]:
# 								print '\t\t\t\t\t\t\t'+field2['uid']
# 								insertIntoTypesDetails(db,cur,field2,uid_database)
# 								for nested_detail2 in field2:
# 									if (nested_detail2!='uid'):
# 										print '\t\t\t\t\t\t\t\t'+nested_detail2+': ',
# 										print field2[nested_detail2]
# 					if not enter2: insertIntoTypesDetails(db,cur,field,uid_database)
# 			else: 
# 				print extrafield[details]
# 		if not enter1: insertIntoTypesDetails(db,cur,extrafield,uid_database)
# 	if (response['parent']!=None): print '\t\tParent: '+response['parent']
# 	# db.commit()

def printTypesDetails(client,uid_temp,uid,db,cur):
	'''Print details for a specific dictionary

	Arguments:
	client: OpendataClient instance
	uid: The Dictionary's uid
	'''
	response = client.get_decision_type_details(uid_temp)
	extraFields = response['extraFields']
	recursiveExtraFields(db,cur,'',extraFields,uid)
	db.commit()
		

def recursiveExtraFields(db,cursor,newu,extraFields,parent_id):
	for extraField in extraFields:
		# print (extraField['uid'])
		# print (extraField['nestedFields'])
		if not extraField['nestedFields']:
			print(newu+extraField['uid'])
			extraField['uid'] = newu+extraField['uid']
			insertIntoTypesDetails(db,cursor,extraField,parent_id)
		else:
			recursiveExtraFields(db,cursor,newu+extraField['uid']+'-',extraField['nestedFields'],parent_id)

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
			if (value[field]==None or value[field]==''):
				sql_val.append(None)
			else:
				sql_val.append(value[field])
		except:
			sql_val.append(None)
	# print (SQLcommand,sql_val)
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

def printOneOrg (response,client):
	'''Return all organizations from diavgeia

	Arguments
	response: a json response returned from OpendataClient
	client: OpendataClient instance
	'''
	db = con.connectMySQL()
	cur = db.cursor()
	organization = response
	print organization['uid']
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
		 "(address,dimos,latitude,longitude,namegrk,new_cat,new_sub_cat,phone,tk)"
		 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")  

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
			data = (address,dimos,coordX,coordY,namegrk,newcat,newsubcat,phone,tk)
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

def printDecisions (response):
	'''Print the urls of all decisions

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	db = con.connectMySQL()
	cur = db.cursor()
	# db = 1
	# cursor = 1
	decisions = response["decisions"]
	for decision in decisions:
		insertIntoDecisions(db,cur,decision)
		# for i in decision:
		# 	print i,":",decision[i]

	db.commit()
	db.close()

def getDecisionsForRelations (response):
	'''Print the urls of all decisions

	Arguments:
	response: a json response returned from OpendataClient 
	'''
	decisions = response["decisions"]
	for decision in decisions:
		# for i in decision:
			# print i,":",decision[i]
		relationInsertIntoDecisionDictionaryItem(decision)
		relationInsertIntoDecisionSigner(decision)

def getOrganizationsForRelation (response,client):
	# One org
	relationInsertIntoOrganizationDictionaryItem(response)
	getUnitsForRelation(response['uid'],client)
	# Many orgs
	# organizations = response["organizations"]
	# for organization in organizations:
		# print "Organization "+organization["uid"]
		# relationInsertIntoOrganizationDictionaryItem(organization)

		# for key in organization:
			# print organization[key]!=None
			# print key
			# if (organization[key]!=None and key!='uid' and key!='organizationDomains'):
				# print "\t"+key+": "+organization[key]

def getUnitsForRelation(uid,client):
	units = client.get_organization_units(uid)['units']
	for unit in units:
		relationInsertIntoUnitDictionaryItem(unit)

def getTypesForRelation (response):
	d_types = response["decisionTypes"]
	for d_type in d_types:
		getExtraFieldsForRelation(d_type['uid'].encode('utf-8'),d_type['uid'])

def getExtraFieldsForRelation (uid_temp,uid):
	db = con.connectMySQL()
	cur = db.cursor()
	response = client.get_decision_type_details(uid_temp)
	extraFields = response['extraFields']
	recursiveExtraFieldsForRelation(db,cur,'',extraFields,uid)
	db.commit()
	db.close()

def recursiveExtraFieldsForRelation(db,cursor,newu,extraFields,parent_id):
	for extraField in extraFields:
		# print (extraField['uid'])
		# print (extraField['nestedFields'])
		if not extraField['nestedFields']:
			print(newu+extraField['uid'])
			extraField['uid'] = newu+extraField['uid']
			value = []
			value['uid'] = extraField['uid']
			value['type_id'] = parent_id
			value['multiple'] = extraField['multiple']
			value['required'] = extraField['required']
			fields = ['uid','type_id','multiple','required']
			SQLcommand = "insert into type_extra_field(parent_id, extra_field_id, mutliple,required) VALUES (%s,%s,%s,%s)"
			actuallInsertion(fields,SQLcommand,cur,db,value)
			# Extra Field Fixed Value List
			try:
				for fixed in value['fixedValueList']:
					value['fixed_value'] = fixed
					fields = ['uid','fixed_value']
					SQLcommand = "insert into extra_feild_fixed_value_list(extra_field_id, fixed_value_list) VALUES (%s,%s)"
					actuallInsertion(fields,SQLcommand,cur,db,value)
			except:
				print "No fixed value"
			# insertIntoTypesDetails(db,cursor,extraField,parent_id)
		else:
			recursiveExtraFields(db,cursor,newu+extraField['uid']+'-',extraField['nestedFields'],parent_id)

def insertIntoDecisions(db,cursor,value):
	'''Insert signers into MySQL db

	Arguments
	db: Connection to MySQL database
	cursor: Cursor for the db
	value: A dictionary for all values for one entry
	uid: The uid of the organization
	'''
	fields = ['ada','versionId','correctedVersionId','issueDate','organizationId','privateData','protocolNumber','subject','submissionTimestamp','decisionTypeId']
	SQLcommand = "insert into decision(ada, version_id, corrected_version_id, issue_date, org_id, private_data, protocol_number, subject, submission_timestamp, type_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	actuallInsertion(fields,SQLcommand,cursor,db,value)

def relationInsertIntoDecisionDictionaryItem(value):
	db = con.connectMySQL()
	cur = db.cursor()
	for thematicCatergory in value['thematicCategoryIds']:
		# print value['ada'],
		# print value['versionId'],
		# print thematicCatergory
		fields = ['ada','versionId','thematicCategoryId']
		value['thematicCategoryId'] = thematicCatergory
		SQLcommand = "insert into decision_dictionary_item(decision_ada, decision_version, them_cat_id) VALUES (%s,%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,value)
	db.commit()
	db.close()

def relationInsertIntoDecisionSigner(value):
	db = con.connectMySQL()
	cur = db.cursor()
	for signer in value['signerIds']:
		# print value['ada'],
		# print value['versionId'],
		# print thematicCatergory
		fields = ['ada','versionId','signerId']
		value['signerId'] = signer
		SQLcommand = "insert into decision_signer(decision_ada, decision_version, signer_id) VALUES (%s,%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,value)
	db.commit()
	db.close()

def relationInsertIntoDecisionUnit(value):
	db = con.connectMySQL()
	cur = db.cursor()
	for unit in value['unitIds']:
		# print value['ada'],
		# print value['versionId'],
		# print thematicCatergory
		fields = ['ada','versionId','unitId']
		value['unitId'] = unit
		SQLcommand = "insert into decision_unit(decision_ada, decision_version, unit_id) VALUES (%s,%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,value)
	db.commit()
	db.close()

# DICTIONARY RELATION

def relationInsertIntoOrganizationDictionaryItem(value):
	db = con.connectMySQL()
	cur = db.cursor()
	for domain in value['organizationDomains']:
		# print value['ada'],
		# print value['versionId'],
		# print thematicCatergory
		fields = ['uid','domain']
		value['domain'] = domain
		SQLcommand = "insert into organization_dictionary_item(organization_id,item_id) VALUES (%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,value)
	db.commit()
	db.close()

def relationInsertIntoUnitDictionaryItem(value):
	db = con.connectMySQL()
	cur = db.cursor()
	for domain in value['unitDomains']:
		# print value['ada'],
		# print value['versionId'],
		# print thematicCatergory
		fields = ['uid','domain']
		value['domain'] = domain
		SQLcommand = "insert into unit_dictionary_item(unit_id,item_id) VALUES (%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,value)
	db.commit()
	db.close()

def importingDictionaryItems(client):
	response = client.get_dictionary('THK')
	db = con.connectMySQL()
	cur = db.cursor()
	items = response["items"]
	for item in items:
		fields = ['uid','label']
		SQLcommand = "insert into dictionary_item(dictionary_item_id,label) VALUES (%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,item)
	db.commit()
	db.close()

def importingTypes(client):
	response = client.get_decision_types()	
	db = con.connectMySQL()
	cur = db.cursor()
	d_types = response["decisionTypes"]
	for d_type in d_types:
		fields = ['uid','label']
		SQLcommand = "insert into type(type_id,label) VALUES (%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,d_type)
	db.commit()
	db.close()

def importingGeo():
	db = con.connectMySQL()
	cur = db.cursor()
	getGEO(cur)
	db.commit()
	db.close()

def importingOrganization(client):
	db = con.connectMySQL()
	cur = db.cursor()
	response = client.get_organization('6114')
	organization = response
	fields = ['uid','label','odeManagerEmail','status','vatNumber','website']
	# SQLcommand = "insert into organization('organization_id','abbreviation','category_id','fek_issue_id','fek_number','fek_year','label','latin','ode_manager_email','status','supervisor_id','vat_number','website') VALUES ("+values+")"
	SQLcommand = "insert into organization(organization_id,label,ode_manager_email,status,vat_number,website) VALUES (%s,%s,%s,%s,%s,%s)"
	# print SQLcommand
	actuallInsertion(fields,SQLcommand,cur,db,organization)
	db.commit()
	db.close()

def importingUnits(client):
	db = con.connectMySQL()
	cur = db.cursor()
	units = client.get_organization_units('6114','all')['units']
	for unit in units:
		fields = ['uid','label','active','myParentId']
		SQLcommand = "insert into unit(unit_id,label,active,org_id) VALUES (%s,%s,%s,%s)"
		unit['myParentId']='6114'
		actuallInsertion(fields,SQLcommand,cur,db,unit)
	SQLcommand = "insert into unit(unit_id,label,org_id) VALUES ('6114','ΔΗΜΟΣ ΘΕΣΣΑΛΟΝΙΚΗΣ','6114')"
	try:
		cur.execute(SQLcommand)
	except:
		print "Already in"
	db.commit()
	db.close()

def importingSigners(client):
	response = client.get_organization_signers_all('6114')
	db = con.connectMySQL()
	cur = db.cursor()
	signers = response['signers']
	for signer in signers:
		fields = ['uid','active','activeFrom','activeUntil','firstName','lastName','myOrgId']
		SQLcommand = "insert into signer(signer_id,active,active_from,active_until,first_name,last_name,org_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
		signer['myOrgId'] = '6114'
		actuallInsertion(fields,SQLcommand,cur,db,signer)
	db.commit()
	db.close()

def fillingSignerUnitRelation(client):
	db = con.connectMySQL()
	cur = db.cursor()
	cur.execute("SELECT signer_id FROM signer")
	for row in cur.fetchall():
		response = client.get_signer(row[0])
		units = response['units']
		for unit in units:
			fields = ['signerId','uid','positionLabel']
			unit['signerId'] = row[0]
			SQLcommand = "insert into signer_unit(signer_id,unit_id,position) VALUES (%s,%s,%s)"
			actuallInsertion(fields,SQLcommand,cur,db,unit)
	db.commit()
	db.close()

def importingDecisions(client,current_page):
	q = "submissionTimestamp:[DT(2006-03-01T00:00:00) TO DT(2014-11-11T23:59:59)] AND (organizationUid:6114)"
	response = client.get_advanced_search_results(q,current_page,query_size)
	db = con.connectMySQL()
	cur = db.cursor()
	decisions = response["decisions"]
	for decision in decisions:
		fields = ['ada','versionId','correctedVersionId','issueDate','protocolNumber','subject','decisionTypeId']
		SQLcommand = "insert into decision(ada, version_id, corrected_version_id, issue_date, protocol_number, subject, type_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
		actuallInsertion(fields,SQLcommand,cur,db,decision)
		thematicCategoryIds = decision['thematicCategoryIds']
		for thematic in thematicCategoryIds:
			value = {}
			value['decisionAda'] = decision['ada']
			value['versionId'] = decision['versionId']
			value['thematic'] = thematic
			fields = ['decisionAda','versionId','thematic']
			SQLcommand = "insert into decision_dictionary_item(decision_ada,decision_version_id,dictionary_item_id) VALUES (%s,%s,%s)"
			actuallInsertion(fields,SQLcommand,cur,db,value)
		extraFields = decision['extraFieldValues']
		importingRecursiveExtraFields(db,cur,'',extraFields)
		var = raw_input("Click to continue...")
	db.commit()
	db.close()

def fillingDecisionsRelationships(client):
	db = con.connectMySQL()
	cur = db.cursor()
	cur.execute("SELECT ada,version_id FROM decision")
	for row in cur.fetchall():
		response = client.get_decision(row[0].encode('utf-8'))
		thematicCategoryIds = response['thematicCategoryIds']
		for thematic in thematicCategoryIds:
			value = {}
			value['decisionAda'] = row[0]
			value['versionId'] = row[1]
			value['thematic'] = thematic
			fields = ['decisionAda','versionId','thematic']
			SQLcommand = "insert into decision_dictionary_item(decision_ada,decision_version_id,dictionary_item_id) VALUES (%s,%s,%s)"
			actuallInsertion(fields,SQLcommand,cur,db,value)
		extraFields = response['extraFieldValues']
		importingRecursiveExtraFields(db,cur,'',extraFields)
	db.commit()
	db.close()

def importingRecursiveExtraFields(db,cursor,newu,extraFields,parent_id):
	for extraField in extraFields:
		# print (extraField['uid'])
		# print (extraField['nestedFields'])
		if not extraField['nestedFields']:
			print(newu+extraField['uid'])
			extraField['uid'] = newu+extraField['uid']
			# insertIntoTypesDetails(db,cursor,extraField,parent_id)
		else:
			recursiveExtraFields(db,cursor,newu+extraField['uid']+'-',extraField['nestedFields'],parent_id)

def main(argv=None):
	client = opendata.OpendataClient("https://diavgeia.gov.gr/luminapi/opendata")	
	print "***DICTIONARY ITEMS***"
	importingDictionaryItems(client)
	print "***TYPES***"
	importingTypes(client)
	print "***GEO***"
	importingGeo()
	print "***ORGANIZATION***"
	importingOrganization(client)
	print "***UNITS***"
	importingUnits(client)
	print '***SIGNERS***'
	importingSigners(client)
	print "***SIGNER - UNIT***"
	fillingSignerUnitRelation(client)
	print '***DECISIONS***'
	q = "submissionTimestamp:[DT(2006-03-01T00:00:00) TO DT(2014-11-11T23:59:59)] AND (organizationUid:6114)"
	response = client.get_advanced_search_results(q,page,query_size)
	total = printInfo (response)
	steps = total/query_size
	for x in range(0,steps+1):
		importingDecisions(client,x)
	# printDecisions(response)
	# getDecisionsForRelations(response)
	# total = printInfo (response)
	# print total
	# steps = total/query_size
	# for x in range(1,steps+1):
	# 	print("Page ",x)
	# 	response = client.get_advanced_search_results(q,x,query_size)
	# 	printDecisions(response)
	# 	getDecisionsForRelations(response)

	# *** OLD CODE ***

	# print "***POSITIONS***"
	# response = client.get_positions()
	# printPositions(response)
	# print "***ORGANIZATIONS***"
	# # response = client.get_organizations(status='all')
	# response = client.get_organization('30')
	# printOneOrg(response,client)
	# response = client.get_organization('6114')
	# printOneOrg(response,client)
	# print "***TYPES***"
	# response = client.get_decision_types()
	# printTypes(response,client)
	# print "***GEO***"
	# db = con.connectMySQL()
	# cur = db.cursor()
	# getGEO(cur)
	# db.commit()
	# db.close()
	# print '***SIGNERS***'
	# response = client.get_organization_signers('6114')
	# printSigners(response,'6114')
	# # printAllDictionaries(response,client)
	# # response = client.get_organizations()
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
