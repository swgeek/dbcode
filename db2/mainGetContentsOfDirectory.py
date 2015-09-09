import HashFilesEtc
import DbLogger

logger = DbLogger.dbLogger()

dirpath = "H:\\xvidsNotArchiving"
fileList = HashFilesEtc.getListOfFilesInDirAndSubdirs(dirpath, logger)
logger.log("contents of %s: \n" % dirpath)

for entry in fileList:
	logger.log( entry )