from sqlalchemy.dialects import mysql
from sqlalchemy.schema import DDL
from sqlalchemy import func, case, text
from ._helpers import Session

from app import ACELogger


class DB(object):
    @classmethod
    def schemas(self):
        from app import Base
        schemas = [t.schema for k,t in Base.metadata.tables.items() if t.schema!='public' and t.schema]
        return sorted(list(set(schemas)))

    def commit(self):
        self.session.commit()

    def append(self, data):
        if isinstance(data,(list,tuple)):
            self.session.add_all(data)
        else:
            self.session.add(data)

    def store(self, data):
        self.append(data)
        self.commit()

    def bulk_append(self, data, update_changed_only=False):
        self.session.bulk_save_objects(data, update_changed_only=update_changed_only)

    def bulk_store(self, data, update_changed_only=False):
        self.bulk_append(data, update_changed_only=update_changed_only)
        self.commit()

    def bulk_update(self, data, index=None, update=None, onlynotnull = False, cond_flg_knd = None):
        if not data:
            return
        data = [data,] if type(data) not in (list,tuple) else data
        _data = data[0]
        update = update or _data.update_fld_names
        _tbl = _data.__table__
        _model = _data.__class__
        _pkey = _data.pkey

        pri_key = _pkey if isinstance(_pkey, list) else [_pkey,]
        index_elements = index if index else pri_key
        if not index_elements:
            raise ValueError('Table indexing elements cannot be empty')

        stmt = mysql.insert(_tbl)
        if onlynotnull:
            _upd = {c: func.coalesce(getattr(_model,c),getattr(stmt.inserted, c)) for c in update}
        elif cond_flg_knd is not None:
            _upd = {}
            for c in update:
                _upd[c] = case([[_model.flg_knd<=text(f'{cond_flg_knd}'), getattr(stmt.inserted, c)]],
                     else_=getattr(_model, c))
        else:
            _upd = {c: getattr(stmt.inserted, c) for c in update}

        if not _upd:
            raise ValueError('Table fields to update cannot be empty')

        stmt = stmt.on_duplicate_key_update(**_upd)

        self.session.execute(stmt, [d.dict for d in data])
        self.session.commit()

    def append_update(self, data, index = None, update=None):
        _cls = data.__class__
        index = index if index else _cls.INDEX
        index = index if type(index) in (list, tuple) else [index, ]
        upd = ({field.name: getattr(data, field.name) for field in update} if update else data.UPDATE) or {}
        upd = {} if update==False else upd
        q = _cls.query.filter_by(**{c.key:getattr(data,c.key) for c in index})
        exists = q.first()
        if exists:
            if upd:
                q.update(upd, synchronize_session='fetch')
            data = exists
        else:
            self.append(data)

        return data, bool(exists)

    def append_store(self, data, index = None, update=None):
        data,_ = self.append_update(data, index = index, update=update)
        self.commit()
        return data,_

    def todel(self, data):
        data = [data, ] if type(data) not in (list, tuple) else data
        for r in data:
            self.session.delete(r)

    def delete(self, data):
        self.todel(data)
        self.commit()

    def reset(self,):
        from app import Base
        Base.metadata.drop_all(self.session.get_bind())
        Base.metadata.create_all(self.session.get_bind())

    def sqlfileexec(self, path):
        try:
            with open(path) as file:
                stmt = file.read()

            ddl_stmt = DDL(stmt)
            self.session.execute(ddl_stmt)
        except RuntimeError:
            raise
        except FileNotFoundError:
            print(f'SQL file: {path} does not exist.')
            raise

    def init(self):
        from app import Base
        Base.metadata.create_all(self.session.get_bind())

    def __init__(self, uri = None):
        self._uri = uri
        self.session = Session(self._uri)()

    def disconnect(self):
        if self.session:
            connection = None
            try:
                engine = self.session.get_bind()
                connection = engine.raw_connection()
                self.session.commit()
            except:
                self.session.rollback()
            finally:
                self.session.close()
                if connection:
                    connection.close()

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()