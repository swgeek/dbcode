
#
# queries and extraction code to find specific files from the past
# probably temporary code, quickie stuff for when I need to find something,
# e.g. a particular photo
#
import CoreDb
import miscQueries
import CopyFilesEtc
import HashFilesEtc

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")


dirpath = "H:\\alex"


HashFilesEtc.addFilesToDepot(db, dirpath)
