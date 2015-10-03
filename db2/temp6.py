
# delete dir and file entries for files with status "deleted"
import CoreDb
import DbLogger
import miscQueries
import DbSchema


logger = DbLogger.dbLogger()

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

list = miscQueries.getFilesWithStatus(db, "deleted")
logger.log("got %d entries " % len(list))

if list is None:
    logger.log("list is None")
    exit(1)

if not list:
    logger.log("list is empty")
    exit(1)

for row in list:
    logger.log(row)

    filehash, dummy, dummy2, dummy3 = row
    logger.log(filehash)

    # get list of entries in originalDirectoriesForFile
    dirlist = miscQueries.getOriginalDirectoriesForFile(db, filehash)
    logger.log("dirs:")
    for entry in dirlist:
        logger.log(entry)

    logger.log("deleting dirs")
    # delete from originalDirectoriesForFile
    command = "delete from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
    db.ExecuteNonQuerySql(command)

    dirlist = miscQueries.getOriginalDirectoriesForFile(db, filehash)
    logger.log("dirs after delete:")
    for entry in dirlist:
        logger.log(entry)

    # delete completely from newFilesTable
    command = "delete from %s where filehash = '%s';" % (DbSchema.newFilesTable, filehash)
    db.ExecuteNonQuerySql(command)
