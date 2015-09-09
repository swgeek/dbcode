import ntpath
import Sha1HashUtilities

import miscQueries
import CoreDb
import DbLogger
import DbSchema

# cache all directories, need both for cache and to iterate through dirs to do work
def getDictionaryOfAllDirs(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoriesTable
	allDirsFromDb = db.ExecuteSqlQueryReturningMultipleRows(command)
	dirpathDict = dict(allDirsFromDb)
	return dirpathDict


def getAncestorPaths(dirpath):
	ancestorPaths = []
	partialPath = "dummy"
	while dirpath and partialPath:
		ancestorPaths.append(dirpath)
		dirpath, partialPath = ntpath.split(dirpath)

	return ancestorPaths


# should only have to do this occasionally, or even just a one time thing 
# if I update insert dir to automatically insert ancestors as well
def insertAncestorDirsIntoDb(db, dirpathDict, dirpath, logger):
	ancestorDirs = getAncestorPaths(dirpath)

	for dirpathToAdd in ancestorDirs:
		dirhash = Sha1HashUtilities.HashString(dirpathToAdd)
		if dirpathDict.get(dirhash):
			# already in db, no need to add
			continue

		# not in db, add
		dirpathDict[dirhash] = dirpathToAdd
		miscQueries.insertDirHash(db, dirhash, dirpathToAdd)
			

#dbpath = "C:\\depotListing\\listingDb.sqlite"
dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

dirpathDict = getDictionaryOfAllDirs(db)
logger.log("initially have %d dirs" % len(dirpathDict))

dirpathList = dirpathDict.values()
for i, dirpath in enumerate(dirpathList):
	logger.log(i)
	insertAncestorDirsIntoDb(db, dirpathDict, dirpath, logger)	

logger.log("Now have %d dirs" % len(dirpathDict))



