#
# code to hash files, directories, and either insert them into depot or just check if they are in there already
#
import Sha1HashUtilities
import miscQueries
import os

def checkIfFileInDatabase(db, filepath):
	# hash file
	filehash = Sha1HashUtilities.HashFile(filepath)

	print filepath
	print filehash
	# check if file in database
	return miscQueries.checkIfFilehashInDatabase(db, filehash.upper())


# TODO: subdirectories too!
def checkIfFilesInDirAreInDatabase(db, dirpath):
	for filename in os.listdir(dirpath):
		filepath = os.path.join(dirpath, filename)
		print checkIfFileInDatabase(db, filepath)


def checkIfAllFilesInDirAreInDatabase(db, dirpath):
	for dirName, subDirList, fileList in os.walk(dirpath):
		print dirName
		print subDirList
		print fileList
		print ""


def addFilesToDepot(db, rootDirPath):
	for dirpath, subDirList, fileList in os.walk(rootDirPath):
		print dirpath
		# check if dirName is in depot, add if not
		#print subDirList
		print fileList
		for filename in fileList:
			filepath = os.path.join(dirpath, filename)
			print "\t%s" % filepath
			if checkIfFileInDatabase(db, filepath):
				print "already in database"
			else:
				print "not in database, need to add"
				# add file to depot
				
			# check if filepath in database, add if not

		print ""
