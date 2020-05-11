import sys, rds_config, logging, pymysql, random, datetime, json, os, urllib
from urllib.parse import urlparse, parse_qs
from hashlib import md5
from mailchimp3 import MailChimp

# rds settings
rds_host = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

# url settings
baseURL = 'https://uais.dev/signup'
successURL = baseURL + '/success'
failURL = baseURL + '/issue'

# mailchimp settings
mailchimpAPI = os.environ['mailchimp_api']
mailchimpID = os.environ['mailchimp_id']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
	conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
	logger.error('ERROR: Unexpected error: Could not connect to MySQL instance.')
	logger.error(e)
	sys.exit()

client = client = MailChimp(mc_api=mailchimpAPI, mc_user='UAISalberta')

logger.info('SUCCESS: Connection to RDS MySQL instance succeeded')

def lambda_handler(event, context):
	formData = get_form_data(event)
	validForm, validItems = validate_form_items(formData)
	if not validForm:
		return {
			'statusCode': 302,
			'headers': {
				'location': failURL
			}
		}
	db_order, db_statement = create_db_statement(validItems)
	## UPDATE MYSQL
	with conn.cursor() as cur:
		cur.execute(f'INSERT INTO registration {db_order} VALUES {db_statement}')
	conn.commit()
	## UPDATE MAILC
	sub_hash = md5(validItems['email'].lower().encode("utf-8")).hexdigest()
	client.lists.members.create_or_update(list_id=mailchimpID, subscriber_hash=sub_hash, data={
		'email_address': validItems['email'],
		'status_if_new': 'subscribed',
		'merge_fields': {
			'FNAME': validItems['firstname'],
			'LNAME': validItems['lastname'],
		},
	})
	conn.close()
	return {
		'statusCode': 301,
		'headers': {
			'location': successURL
		}
	}

def get_form_data(event):
	body = urllib.parse.parse_qs(event['body'])
	bodyItems = {key : body[key][0] for key in body}
	return bodyItems

def validate_form_items(formItems):
	validItems = {}
	validItems['etc'] = {}
	validMembership = ['regular','associate']

	if 'membership' in formItems and formItems['membership'] in validMembership:
		if formItems['membership'] == 'regular':
			validItems['memberStatus'] = True
		if formItems['membership'] == 'associate':
			validItems['memberStatus'] = False
	else:
		return False, validItems

	if 'email' in formItems and '@' in formItems['email']:
		validItems['email'] = formItems['email'].lower()
	else:
		return False, validItems

	if 'firstname' in formItems:
		validItems['firstname'] = formItems['firstname']

	if 'lastname' in formItems:
		validItems['lastname'] = formItems['lastname']

	if 'program' in formItems:
		p = formItems['program']
		switch={
			'cs': 'Computing Science',
			'ce': 'Computer Engineering',
			'math': 'Mathematics',
			'stat': 'Statistics',
			'psych': 'Psychology',
			'other': 'Other'
		}
		validItems['etc']['program'] = switch.get(p, 'None')

	validYears = ['0','1','2','3','4','5','5+']
	if 'year' in formItems and formItems['year'] in validYears:
		validItems['etc']['year'] = formItems['year']

	validOrgOptions = ['university', 'company', 'other']
	if 'org' in formItems and formItems['org'] in validOrgOptions:
		o = formItems['org']
		switch={
			'university': 'University',
			'company': 'Company',
			'other': 'Other'
		}
		validItems['etc']['organizationType'] = switch.get(o, 'None')

	if 'orgname' in formItems:
		validItems['etc']['organizationName'] = formItems['orgname']

	if 'position' in formItems:
		validItems['etc']['organizationPosition'] = formItems['position']

	return True, validItems

def create_db_statement(formData):
	email = formData['email']
	firstname = formData['firstname']
	lastname = formData['lastname']
	memberStatus = formData['memberStatus']
	etc = str(formData['etc']).replace("'", '"')

	db_order = '(email, firstname, lastname, memberStatus, etc)'
	db_statement = str(f"('{email}', '{firstname}', '{lastname}', {memberStatus}, '{etc}')")

	return db_order, db_statement