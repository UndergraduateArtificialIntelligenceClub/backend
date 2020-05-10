import sys, rds_config, logging, pymysql, uuid, random, datetime

#rds settings
rds_host = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
	conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
	logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
	logger.error(e)
	sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded") 

def lambda_handler(event, context):

	formData = get_form_data(event)
	db_order, db_statement = create_db_statement(formData)

	with conn.cursor() as cur:
		cur.execute("INSERT INTO agents {} VALUES {}".format(db_order, db_statement))
	conn.commit()

	return "All Done"

def get_form_data(event):
	formValues = {'email': 'test1@magnumpus.com','firstname': 'tester1', 'lastname': 'testy', 'referrer': 'NULL'}
	return formValues

def create_id():
	userId = uuid.uuid4().hex
	return userId

def create_db_statement(formData):
	userId = createId()
	email = formData['email']
	firstname = formData['firstname']
	lastname = formData['lastname']
	startDate = str(datetime.date.today())

	db_order = "(id, email, firstname, lastname)"
	db_statement = str((agentId, email, firstname, lastname))
	
	return db_order, db_statement
