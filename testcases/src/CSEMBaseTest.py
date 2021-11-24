from datetime import date, datetime
from threading import Event, Lock
from bson import timestamp
from pymongo import MongoClient
from random import randint, random

from pysys.basetest import BaseTest

class CSEMBaseTest(BaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)

		# Batching
		self.resultsToWrite = []
		self.resultsToWriteLock = Lock()
		self.resultsToWriteEvent = Event()
		self.total_written = 0
		self.total_parsed = 0
		self.threads = []
		self.test_start = datetime.now()

	def get_db_connection(self, conn_str=None):
		if not conn_str:
			conn_str = self.project.MONGODB_CONNECTION_STRING_ATLAS.replace("~", "=")
		client = MongoClient(conn_str)
		db = client.get_database()
		return db

	def clear_data(self, db):
		db.src_data.delete_many({})
		db.audit.delete_many({})
		db.resume_token.delete_many({})

	def done(self):
		self.log.info('Stopping threads')
		for thread in self.threads:
			thread.stop()
			thread.join()
		self.threads = []

	def start_src_data_listen_thread(self, db):
		resume_token = None
		start_after = None
		change_stream = None
		resume_doc = db.resume_token.find_one()
		if resume_doc:
			resume_token = resume_doc['resume_token']
			self.log.info(f"Resuming from {resume_token}")
		else:
			start_after = timestamp.Timestamp(self.test_start, 0)
			self.log.info(f"Start after {start_after}")

		change_stream = db.src_data.watch([
			{'$match': { 'operationType': 'insert'} }], resume_after=resume_token, start_at_operation_time=start_after)

		args = {}
		args['change_stream'] = change_stream
		args['db'] = db
		self.threads.append(self.startBackgroundThread( f"Audit Listen", self.listenToCS, args))
		return change_stream

	# Change stream thread
	def listenToCS(self, stopping, **kwargs):
		change_stream = kwargs['change_stream']
		db = kwargs['db']

		# self.log.info("listenToCS start")
		try:
			for change in change_stream:
				doc = change['fullDocument']
				# insert into audit + resume token
				doc = self.create_audit_entry(doc["object_id"])
				self.log.info(f"Audit entry {doc}")
				self.insert_audit_entry(db, doc, change["_id"])
		except Exception as ex:
			self.log.error(ex)

		self.log.info("listenToCS done")

	def create_src_data(self, index, max_perms):
		doc = {}
		doc["object_id"] = self.create_object_id(index)
		doc["ts"] = datetime.now()
		self.create_perms(doc, randint(1, max_perms))
		return doc

	def create_audit_entry(self, obj_id):
		doc = {}
		doc["object_id"] = obj_id
		doc["ts"] = datetime.now()
		return doc

	def create_object_id(self, index):
		return f'object_id_{index}'

	def create_perms(self, doc, count):
		perms = []
		for i in range(count):
			perm = { f'perm_{i}' : f'value_{i}'}
			perms.append(perm)
		doc['perms'] = perms

	def insert_src_data(self, db, doc):
		db.src_data.insert_one(doc)

	def insert_audit_entry(self, db, doc, resume_token):
		res_doc = {}
		res_doc['_id'] = 1
		res_doc['resume_token'] = resume_token
		
		with db.client.start_session() as session:
			with session.start_transaction():
				db.audit.insert_one(doc, session=session)
				db.resume_token.replace_one({'_id' : res_doc['_id']}, res_doc, upsert=True, session=session)

	def have_audit_index(self, db, index):
		obj_id = self.create_object_id(index)
		doc = db.audit.find_one({"object_id" : obj_id})
		self.log.info(f"Max index is {doc}")
		if doc:
			return True
		else:
			return False



