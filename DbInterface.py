# translation of legacy code from c# to python. Will rearchitect at some point
import os.path
import sqlite3
import sys

class DbInterface:
    def __init__(self, dbFilePath):

        self.dbFilePath = dbFilePath

        try:
            #TODO: should I keep this open all the time? Or open as close as needed? For now do not keep it open
            # note that connect will create empty database file if does not exist already
            dbConnection = sqlite3.connect(dbFilePath)
        except sqlite.Error, e:
            print "Error %s" % e.args[0]
            sys.exit(1)

        self.connection = None



    @staticmethod
    def CreateEmptyDbFile(dbFilePath):

        if os.path.isfile(dbFilePath):
            print "Cannot create database file %s. File already exists!" % dbFilePath
            sys.exit(1)

        # connect automatically creates database file
        # TODO: add exception handling code
        dbConnection = sqlite3.connect(dbFilePath)
        dbConnection.close()


    # creating empty code to facilitate port, remove
    def OpenConnection(self):
        pass

    def CloseConnection(self):
        pass


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



    def ExecuteSqlQueryReturningSingleRow(self, sqlStatement):
        connection = sqlite3.connect(self.dbFilePath)
        with connection: # if do not use with, then have to do "commit" at end
            cursor = connection.cursor()
            cursor.execute(sqlStatement)
            row = cursor.fetchone()
        return row


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

