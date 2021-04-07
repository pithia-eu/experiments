import re
import mysql.connector as mysql

from app import ACELogger


class RAWDB(object):
    @property
    def asdict(self):
        return self._dict_cursor

    @asdict.setter
    def asdict(self, value):
        self._dict_cursor = value

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if self._conn:
            self._conn.autocommit = value
            self._autocommit = value

    @property
    def isvalid(self, ):
        return self._valid

    @property
    def isconnected(self, ):
        ret = False
        if self._conn:
            cur = None
            try:
                cur = self.cursor()
                resultset = cur.execute("SELECT 1;")
                ret = True
            except mysql.OperationalError as e:
                ACELogger.logger.error('MySQL Operational Error: {}'.format(str(e)))
                self._valid = False
            finally:
                if cur is not None and self._curglob is False:
                    cur.close()
                return ret
        else:
            self._valid = False
            return ret

    @property
    def conn(self):
        if not self._conn:
            try:
                self._conn = mysql.connect(**self._uridict)
                assert self._conn is not None
                self._valid = True
                self.autocommit = self._autocommit
            except Exception as e:
                ACELogger.logger.error('Unable to connect to database')
                exit(0)

        return self._conn

    def conniso(self,database=True):
        if self._conn:
            self.disconnect()

        isouri = re.sub(r'dbname=\w+\s*', '', self._uri)
        cargs = {v[0].strip(): v[1].strip() for v in [kv.split('=') for kv in isouri.split()]}
        if not database:
            del cargs['database']
        try:
            self._conn = mysql.connect(**cargs)
            assert self._conn is not None
            self._valid = True
        except Exception as e:
            ACELogger.logger.error('Unable to connect to database')
            exit(0)

    def cursor(self, globally=None):
        cur = None
        if self._curglob is True and globally==False and self._cur:
            self._cur.close()
            self._cur = None

        if self._cur is None or self._cur.closed():
            try:
                if self._dict_cursor:
                    cur = self.conn.cursor(dictionary=True)
                else:
                    cur = self.conn.cursor()
            except mysql.OperationalError as e:
                self._valid = False
                ACELogger.logger.error('MySQL Operational Error: {}'.format(str(e)))
                exit(0)
        else:
            cur = self._cur

        if globally==True:
            self._cur = cur

        if globally is not None:
            self._curglob = globally

        return cur

    def commit(self):
        try:
            if self._conn:
                self.conn.commit()
                #self.conn.close()
        except mysql.Error as e:
            ACELogger.logger.error('MySQL Operational Error on COMMIT: {}'.format(str(e)))
            self._conn.rollback()
            self._valid = False
        finally:
            pass

    def fetch(self, query, args = None, one = False):
        cur = self.cursor()
        res = None
        try:
            if args:
                cur.execute(str(query), args)
            else:
                cur.execute(str(query))
            res = cur.fetchall()
            if self._autocommit:
                self.commit()
        except mysql.Error as e:
            self._conn.rollback()
            self._valid = False
            ACELogger.logger.error('MySQL Operational Error on FETCH: {}'.format(str(e)))
        finally:
            if cur is not None and self._curglob is False:
                cur.close()
            if one and res and len(res)==1:
                return res[0]
            return res

    def execute(self, stmt, args = None):
        query = [str(s) for s in stmt] if type(stmt) in (list,tuple) else [str(stmt),]
        cur = self.cursor()
        try:
            for sql in query:
                try:
                    if args:
                        cur.execute(sql,args)
                    else:
                        cur.execute(sql)
                    if self._autocommit:
                        self.commit()
                except mysql.Error as e:
                    self._conn.rollback()
                    ACELogger.logger.error('MySQL Operational Error on INSERT: {}'.format(str(e)))
                    if not re.match(r".*already exists", str(e.args)):
                        ACELogger.logger.warning('Duplicate record: {}'.format(str(e)))

            self._conn.commit()
        except:
            self._conn.rollback()
            ACELogger.logger.error('MySQL Operational Error on INSERT: {}'.format(str(e)))
            self._valid = False
        finally:
            if cur is not None and self._curglob is False:
                cur.close()

    def createDB(self, dbname = None):
        args = {'database': dbname}
        self.conniso(database=False)
        cur = self.cursor()
        finddbq = "select schema_name from information_schema.schemata where schema_name = lower(%(database)s);"
        createdbq = "CREATE DATABASE {};".format(dbname)
        try:
            cur.execute(finddbq, args)
            fdatabases = cur.fetchall()
            fdatabases = [item for sub in fdatabases for item in sub]
            if dbname.lower() in fdatabases:
                ACELogger.logger.critical('MySQL Database already exists: {}'.format(''))
                return

            cur.execute(createdbq)
            ACELogger.logger.info('MySQL Created DB: {}'.format(dbname))
        except mysql.Error as e:
            ACELogger.logger.error('MySQL Exception creating database: {}'.format(str(e)))
        finally:
            self.disconnect()

    def dropDB(self, dbname = None):
        args = {'database': dbname}
        self.conniso()
        cur = self.cursor()
        finddbq = "select schema_name from information_schema.schemata where schema_name = lower(%(database)s);"
        createdbq = "DROP DATABASE {};".format(dbname)
        try:
            cur.execute(finddbq, args)
            fdatabases = cur.fetchall()
            fdatabases = [item for sub in fdatabases for item in sub]
            if dbname.lower() not in fdatabases:
                ACELogger.logger.critical('MySQL Database does not exist: {}'.format(dbname))
                return

            cur.execute(createdbq)
            ACELogger.logger.info('MySQL Dropped DB: {}'.format(dbname))
        except mysql.Error as e:
            ACELogger.logger.error('MySQL Exception dropping database: {}'.format(str(e)))
        finally:
            self.disconnect()

    def __init__(self, uri = None, global_cursor = False, autocommit = False, asdict = False):
        self._uri = uri
        self._uridict = {v[0].strip(): v[1].strip() for v in [kv.split('=') for kv in self._uri.split()]}
        self._valid = True
        self._conn = None
        self._cur = None
        self._autocommit = autocommit
        self._curglob = global_cursor
        self._dict_cursor = asdict

    def disconnect(self):
        try:
            for d in (self._cur,self._conn):
                if d is not None:
                    d.close()
            if self._conn and not self._conn.is_connected():
                self.commit()
                self._conn.disconnect()
                self.conn.close()
        except Exception as e:
            ACELogger.logger.critical('MySQL Exception while disconnecting')
            pass
        finally:
            self._cur = None
            self._conn = None

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()