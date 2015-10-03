#
# code to hash files, directories, and either insert them into depot or just check if they are in there already
#
import Sha1HashUtilities
import miscQueries
import os
import CopyFilesEtc
import ctypes


# TODO: subdirectories too!
def checkIfFilesInDirAreInDatabase(db, dirpath):
	for filename in os.listdir(dirpath):
		filepath = os.path.join(dirpath, filename)
		print checkIfFileInDatabase(db, filepath)

#
# will eventually split stuff into threads (e.g. check if in db in one thread, 
# move into db in second thread, update db in third thread). May need to switch
# to postgres first.
#
def getListOfFilesNotInDb(db, rootDirPath, logger):
	filesToAdd = []

	for dirpath, subDirList, fileList in os.walk(rootDirPath):
		logger.log("dir: %s" % dirpath)
		# check if dirName is in depot, add if not
		#print subDirList
		#logger.log(fileList)
		for filename in fileList:
			filepath = os.path.join(dirpath, filename)
			logger.log("\tfile: %s" % filepath)
			filehash = Sha1HashUtilities.HashFile(filepath)
			logger.log("\tfilehash: %s" % filehash)
			if miscQueries.checkIfFilehashInDatabase(db, filehash.upper()):
				logger.log("already in database")
			else:
				logger.log("not in database, need to add")
				filesToAdd.append((filename, dirpath, filehash))

	return filesToAdd
				


def getListOfFilesInDirAndSubdirs(rootDirPath, logger):
	filesToAdd = []

	for dirpath, subDirList, fileList in os.walk(rootDirPath):
		logger.log(dirpath)
		for filename in fileList:
			#filename = filename.encode('utf-8')
			logger.log(filename)
			#filename = filename.decode('utf-8').encode("latin-1")
			#dirpath = dirpath.decode('utf-8').encode("latin-1")
			filepath = os.path.join(dirpath, filename)
			try:
				filehash = Sha1HashUtilities.HashFile(filepath)
				filesToAdd.append((filename, dirpath, filehash))
			except Exception , e:
				logger.log("EXCEPTION: %s" % str(e))

	return filesToAdd