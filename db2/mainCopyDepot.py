import FileUtils
import DbLogger


logger = DbLogger.dbLogger()

sourceDepotRoot = "I:\\objectstoreTodo"
destinationDepotRoot = "F:\\objectstore1backup"
FileUtils.copyDepot(sourceDepotRoot, destinationDepotRoot, logger)
