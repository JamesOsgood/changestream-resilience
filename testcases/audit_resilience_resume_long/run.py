# Import base test
from CSEMBaseTest import CSEMBaseTest
class PySysTest(CSEMBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		CSEMBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.last_index = -1

	def execute(self):
		EVENTS_TO_INSERT = 10
		MAX_PERMS = 10

		db = self.get_db_connection()
		# Clear all data
		self.clear_data(db)
		change_stream = self.start_src_data_listen_thread(db)

		for index in range(EVENTS_TO_INSERT):
			self.log.info(index)
			doc = self.create_src_data(index, MAX_PERMS)
			self.insert_src_data(db, doc)
			self.last_index = index

			if index % 7 == 0:
				change_stream.close()
				self.done()

		self.wait(10.0)
		change_stream = self.start_src_data_listen_thread(db)

		done = False
		while not done:
			done = self.have_audit_index(db, self.last_index)
			if not done:
				self.wait(1.0)
		
		change_stream.close()
		self.done()

	def validate(self):
		db = self.get_db_connection()
		self.assertTrue(self.have_audit_index(db, self.last_index))


