import os
from contextlib import contextmanager

from .raw import RAWDB
from .sqla import DB
from ._helpers import Session, DBBase

from app import CONF
cfg = CONF['MYSQLDB']

from app import ACELogger

class DBUtils:

    @classmethod
    @contextmanager
    def session_scope(cls, uri):
        """
        Provide a transactional scope
        around a series of operations.
        """
        uri = uri or cfg['SQLALCHEMY_DATABASE_URI']
        session = Session(uri)()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def prepareDB(db = None):
        sqlschemasarr = list()
        sqlextarr = list()

        if sqlschemasarr:
            db.execute(sqlschemasarr)
        if sqlextarr:
            db.execute(sqlextarr)

    @staticmethod
    def create():
        schemas = DB.schemas()
        rawDB = RAWDB(uri=cfg['SQLALCHEMY_DATABASE_MYSQLURI'])
        rawDB.createDB(dbname = cfg['DB'])
        DBUtils.prepareDB(db=rawDB)

    @staticmethod
    def init():
        db = DB(uri=cfg['SQLALCHEMY_DATABASE_URI'])
        db.init()
        db.sqlfileexec(cfg['FUNC_PATH'])
        ACELogger.logger.info('MySQL Initialized DB')

    @staticmethod
    def reset():
        db = DB(uri=cfg['SQLALCHEMY_DATABASE_URI'])
        db.reset()
        ACELogger.logger.info('MySQL Reset DB')

    @staticmethod
    def drop():
        rawDB = RAWDB(uri=cfg['SQLALCHEMY_DATABASE_MYSQLURI'])
        rawDB.dropDB(dbname=cfg['DB'])

    @staticmethod
    def deleteSA(id, model = None):
        try:
            db = DB(uri= cfg['SQLALCHEMY_DATABASE_URI'])
            obj = db.session.query(model).filter_by(id=id).first()
            db.delete(obj)
        except Exception as e:
            ACELogger.logger.error(e)
        return True

    @staticmethod
    def updateSA(data, index = None, update = None, onlynotnull = False):
        try:
            db = DB(uri= cfg['SQLALCHEMY_DATABASE_URI'])
            db.bulk_update(data, index = index, update=update, onlynotnull = onlynotnull)
        except Exception as e:
            ACELogger.logger.error(e)
        return True

    @staticmethod
    def storeSA(data):
        try:
            db = DB(uri= cfg['SQLALCHEMY_DATABASE_URI'])
            db.bulk_store(data)
        except Exception as e:
            ACELogger.logger.error(e)
        return True

    @staticmethod
    def storeSAThreaded(q):
        db = DB(uri= cfg['SQLALCHEMY_DATABASE_URI'])
        while not q.empty():
            pm = q.get()
            db.bulk_store(pm)
            q.task_done()

        return True

    @property
    def ormdb(self):
        if not self._db:
            self._db = DB(uri= cfg['SQLALCHEMY_DATABASE_URI'])
        return self._db

    @property
    def rawdb(self):
        if not self._rawdb:
            self._rawdb = RAWDB(uri= cfg['SQLALCHEMY_DATABASE_MYSQLURI'],asdict=self._asdict)
        return self._rawdb

    def truncate(self,clTable):
        sql = 'truncate table {}'.format(clTable.__table__.fullname)
        self.rawdb.execute(sql)

    def __init__(self, rawAsDict = False):
        self._asdict = rawAsDict
        self._rawdb = None
        self._db = None

    def disconnect(self):
        if self._rawdb:
            self._rawdb.disconnect()
            del self._rawdb
            self._rawdb = None

        if self._db:
            self._db.disconnect()
            del self._db
            self._db = None

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()
