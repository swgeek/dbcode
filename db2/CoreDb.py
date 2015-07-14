import os.path
import sqlite3
import sys

# core database connection and commands
# really want to switch to postgres, so temporary
#
class CoreDb:
    def __init__(self, dbFilePath):

        if not os.path.isfile(dbFilePath):
            print "file %s does not exist! Exiting" % dbFilePath
            sys.exit(1)

        self.dbFilePath = dbFilePath

        try:
            #TODO: should I keep this open all the time? Or open as close as needed? For now do not keep it open
            # note that connect will create empty database file if does not exist already
            dbConnection = sqlite3.connect(dbFilePath)
            #dbConnection.text_factory = lambda x: str(x, 'utf_8')
            #dbConnection.text_factory = bytes
            dbConnection.text_factory = str
            #dbConnection.text_factory = lambda x: unicode(x, "utf-8", "ignore")
            #dbConnection.text_factory = lambda x: unicode(x, "utf-16", "ignore")
        except sqlite3.Error, e:
            print "Error %s" % e.args[0]
            sys.exit(1)

        self.connection = None


    def ExecuteSqlQueryReturningMultipleRows(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        #dbConnection.text_factory = lambda x: str(x, 'utf_8')
        #dbConnection.text_factory = bytes
        connection.text_factory = str
        #dbConnection.text_factory = lambda x: unicode(x, "utf-8", "ignore")
        #dbConnection.text_factory = lambda x: unicode(x, "utf-16", "ignore")
        try:
            with connection: # if do not use with, then have to do "commit" at end
                cursor = connection.cursor()
                cursor.execute(sqlStatement)
                rows = cursor.fetchall()
        except sqlite3.Error, e:
            print "Error %s" % e.args[0]
            sys.exit(1)
        return rows


    def ExecuteSqlQueryReturningSingleRow(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)
            row = cursor.fetchone()
        return row


    def ExecuteNonQuerySql(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)

    def ExecuteManyNonQuery(self, sqlStatement, entries):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.executemany(sqlStatement, entries)

    def ExecuteSqlQueryReturningSingleInt(self, sqlStatement):
        row = self.ExecuteSqlQueryReturningSingleRow(sqlStatement)
        if row is None:
            return None
        value = int(row[0])
        return value


    def ExecuteSqlQueryForSingleString(self, sqlStatement):
        row = self.ExecuteSqlQueryReturningSingleRow(sqlStatement)
        if row is None:
            return None
        value = str(row[0])
        return value

'''

    @staticmethod
    def CreateEmptyDbFile(dbFilePath):

        if os.path.isfile(dbFilePath):
            print "Cannot create database file %s. File already exists!" % dbFilePath
            sys.exit(1)

        # connect automatically creates database file
        # TODO: add exception handling code
        dbConnection = sqlite3.connect(dbFilePath)
        dbConnection.close()


    def ExecuteNonQuerySql(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)


    def ExecuteMultipleSqlStatementsWithRollback(self, sqlStatementList):
        try:
            connection = sqlite3.connect(self.dbFilePath)
            cursor = connection.cursor()
            for statement in sqlStatementList:
                cursor.execute(statement)
            connection.commit()
        except sqlite3.Error, e:
            if connection:
                connection.rollback()
            print "Error %s" % e.args[0]
            sys.exit(1)
        finally:
            if connection:
                connection.close()





    def ExecuteSqlQueryReturningMultipleRows(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)
            rows = cursor.fetchall()
        return rows


    # bad name, I know. Maybe use return dataset or iter... Just get it working for now
    def ExecuteSqlQueryReturningReader(self, sqlStatement, maxReturn=100):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)
            while True:
                results = cursor.fetchmany(maxReturn)
                if not results:
                    break
                for row in results:
                    yield row
        print "DONE"
        yield None

 


    def ExecuteSqlQueryForSingleString(self, sqlStatement):
        row = self.ExecuteSqlQueryReturningSingleRow(sqlStatement)
        if row is None:
            return None
        value = str(row[0])
        return value


    def ExecuteSqlQueryForStrings(self, sqlStatement):
        rows = self.ExecuteSqlQueryReturningMultipleRows(sqlStatement)
        values = [ x[0] for x in rows ]
        return values


    #temporary, here to help with legacy code
    def GetDatasetForSqlQuery(self, sqlStatement):
        rows = self.ExecuteSqlQueryReturningMultipleRows(sqlStatement)
        return rows


    #not the same, but try this
    def GetDataReaderForSqlQuery(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)
            row = "dummy"
            while row is not None:
                row = cursor.fetchone()
                yield row


    # transaction stuff. Maybe return connection instead of using instance variable so can have multiple connections
    
    def startTransaction(self):
        self.connection = sqlite3.connect(self.dbFilePath)
        self.connection.isolation_level = None
        cursor = self.connection.cursor()
        cursor.execute('begin')


    def ExecuteNonQuerySqlWithinTransaction(self, sqlStatement):
        cursor = self.connection.cursor()
        cursor.execute(sqlStatement)

    def EndTransaction(self):
        self.connection.commit()
        self.connection = None
'''
