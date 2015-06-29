import logging
import time

class dbLogger:
	def __init__(self):
		timestamp = time.strftime("%Y%m%d_%H%M")
		logfilename = "dblog_" + timestamp
		self.logger = logging.getLogger(logfilename)
		self.logger.setLevel(logging.DEBUG)

		filehandler = logging.FileHandler(logfilename)
		self.logger.addHandler(filehandler)

		stdoutHandler  = logging.StreamHandler() # actually going to stderr, maybe fix this
		self.logger.addHandler(stdoutHandler)

	def log(self, msg):
		self.logger.debug(msg)
