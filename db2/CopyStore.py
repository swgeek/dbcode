# nothing to do with database, simply copies depot from one drive to another.
# this will only work for depots, as make assumption that only directories at top level
# and only files at bottom level. May generalize, may not.
import os
import shutil

sourceDepot = "I:\\objectstore1p5"
destinationDepot = "E:\\objectstore1p5"

if not os.path.exists(sourceDepot):
	raise Exception("%s does not exist" % sourceDepot)

if not os.path.exists(destinationDepot):
	raise Exception("%s does not exist" % destinationDepot)






for dirName in os.listdir(sourceDepot):
	print "copying %s" % dirName
	sourceDirPath = os.path.join(sourceDepot, dirName)
	destDirPath = os.path.join(destinationDepot, dirName)
#	if not os.path.exists(destDirPath):
#		os.mkdir(destDirPath)
	shutil.copytree(sourceDirPath, destDirPath)
