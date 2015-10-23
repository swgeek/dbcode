# compare all files in two directories, delete any in second dir that already exist in first, even if different name

import os.path

import FileUtils
import DbLogger
import Sha1HashUtilities


def getFilehashListFromDirPath(rootDirPath, logger, excludeList = []):
	filehashList = []
	fileListing = FileUtils.getListOfAllFilesInDir(rootDirPath)
	for dirpath, filename in fileListing:
		if filename in excludeList:
			logger.log("skipping %s" % filename)
			continue
		filepath = os.path.join(dirpath, filename)
		logger.log("%s" % filepath)
		filehash = Sha1HashUtilities.HashFile(filepath)
		filehash = filehash.upper()
		filehashList.append((filehash, filepath))

	return filehashList


logger = DbLogger.dbLogger()

mainPath  = "E:\\20050205all"
pathToCheck = "F:\\extract\\I_\\p1backupa\\archived\\4\\2005a_1\\200502\\20050205_sean_james_etc"

if mainPath == pathToCheck:
	exit(1)

filehashAndPathList = getFilehashListFromDirPath(mainPath, logger)
# only need hash values
mainDirFileHashSet = set([x[0] for x in filehashAndPathList])

filehashAndPathList = getFilehashListFromDirPath(pathToCheck, logger)

for filehash, filepath in filehashAndPathList:
	if filehash in mainDirFileHashSet:
		logger.log("duplicate, deleting %s" % filepath)
		if not os.path.isfile(filepath):
			logger.log("ERROR: no such file")
			exit(1)
		os.remove(filepath)

	else:
		logger.log("new, keeping %s" % filepath)