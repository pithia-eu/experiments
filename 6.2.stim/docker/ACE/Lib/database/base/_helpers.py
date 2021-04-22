from sqlalchemy import create_engine, Column, UniqueConstraint
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import sessionmaker

Session = lambda uri: sessionmaker(
        bind=create_engine(uri,
                           pool_size=5,  # 5 = default in SQLAlchemy
                           max_overflow=15,  # 15 = default in SQLAlchemy
                           pool_timeout=1
                           ),
        autocommit=False, autoflush=True
    )

from app import ACELogger


class DBBase(object):
    _PKEY = False
    _INDEX_COLUMNS = False
    _UNQ_CONSTRAINTS = False
    _UPDATE_FLD_NAMES = False
    _UPDATE_COLUMNS = False

    @property
    def pkey(self):
        cls = self.__class__
        if cls._PKEY==False:
            col = self.__table__.c
            pr_key = [column for name, column in col.items() if isinstance(column, Column) and any([column.primary_key, ])]
            cls._PKEY = pr_key[0] if pr_key and len(pr_key) == 1 else pr_key

        return cls._PKEY

    @property
    def index_columns(self):
        cls = self.__class__
        if cls._INDEX_COLUMNS==False:
            col = self.__table__.c
            cls._INDEX_COLUMNS = [column for name, column in col.items() if isinstance(column, Column) and any([column.unique, ])]

        return cls._INDEX_COLUMNS

    @property
    def unq_constraints(self):
        cls = self.__class__
        if cls._UNQ_CONSTRAINTS == False:
            ac = list(self.__table__.constraints) if self.__table__.constraints else []
            cls._UNQ_CONSTRAINTS = [c for c in ac if type(c)==UniqueConstraint]

        return cls._UNQ_CONSTRAINTS

    @property
    def update_fld_names(self):
        cls = self.__class__
        if cls._UPDATE_FLD_NAMES == False:
            col = self.__table__.c
            un = [name for name,column in col.items() if isinstance(column,Column) and not any([column.primary_key, column.unique])]
            uc = self.unq_constraints
            if uc:
                ucd = [k[0] for k in list(set([v for s in [c.columns for c in uc] for v in s.items()]))]
                un = [u for u in un if u not in ucd]
            cls._UPDATE_FLD_NAMES = un

        return cls._UPDATE_FLD_NAMES

    @property
    def update_columns(self):
        cls = self.__class__
        if cls._UPDATE_COLUMNS == False:
            col = self.__table__.c
            _upd = {field: getattr(col,field) for field in self.update_fld_names}
            cls._UPDATE_COLUMNS =  _upd

        return cls._UPDATE_COLUMNS

    @property
    def dict(self):
        return {c.name: getattr(self,c.name) for c  in self.__table__.columns}

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
