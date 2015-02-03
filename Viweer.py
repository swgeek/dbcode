# temporary file, here to exactly match the legacy code while porting
import DbHelper

class Viweer():
	def __init__(self, dbFileName):
		self.databaseHelper = DbHelper.DbHelper(dbFileName)
		self.databaseHelper.OpenConnection()

	def CreateNewDatabase(string newDbFilePath):
		
