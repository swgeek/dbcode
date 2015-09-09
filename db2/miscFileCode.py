import os

def getPathOfFileInDepot(depotRootPath, filehash):
		subdir = filehash[0:2]
		filepath = os.path.join(depotRootPath, subdir, filehash)

		if os.path.isfile(filepath):
			return filepath
		else:
			return None



depotRoot = "I:\\objectstore1"
depotRoot2 = "H:\\objectstore2"
filehash = "1A00A35086006EADC79F9206F94122B4B9E1B419"

filepath = getPathOfFileInDepot(depotRoot, filehash)
if filepath:
	print "found in %s" % depotRoot

filepath = getPathOfFileInDepot(depotRoot2, filehash)
if filepath:
	print "found in %s" % depotRoot2
