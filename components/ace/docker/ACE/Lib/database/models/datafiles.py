import portion as P
from datetime import datetime
from sqlalchemy.dialects import mysql
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, VARCHAR, Boolean,SMALLINT, String, Text, DateTime, Float, text, func, event, Interval
from sqlalchemy import func
from sqlalchemy.orm import relationship
from typing import Optional, Literal, List, Any
from pydantic import BaseModel, validator, Extra, Field, ValidationError
from .data import MagDataSerial, SwepamDataSerial

from app import Base


class DataFiles_Mixin(object):

    id = Column(mysql.INTEGER, primary_key = True)
    fname = Column(mysql.VARCHAR(50), nullable=False, unique=True)
    flg_knd = Column(mysql.TINYINT,index=True, nullable=False)
    ser_id = Column(mysql.INTEGER, index=True, nullable=False)
    datemin = Column(DateTime(), index=True, nullable=True)
    datemax = Column(DateTime(), index=True, nullable=True)
    parsed = Column(mysql.TINYINT(1), index=True, nullable=False,default=0)

    def __repr__(self):
        pass


class DataFilesSerial(BaseModel):
    id: Optional[int] = None
    fname: str
    flg_knd: int
    ser_id: int
    datemin: Optional[datetime] = None
    datemax: Optional[datetime] = None
    parsed: int
    nowcast: Optional[bool] = False
    data: List[Any] = []

    def range(self):
        return P.closed(self.datemin,self.datemax)

    def setData(self,data):
        self.data = data

    def todict(self):
        return self.dict()

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class MagDataFiles(Base,DataFiles_Mixin):
    __tablename__ = 'magdatafiles'
    __table_args__ = (UniqueConstraint('flg_knd', 'ser_id', name='ser_flg_idx'),)
    __mapper_args__ = {'polymorphic_identity': 'magdatafiles', 'concrete': True}

    def __init__(self, **mdfile):
        super(MagDataFiles, self).__init__(**mdfile)


class SwepamDataFiles(Base, DataFiles_Mixin):
    __tablename__ = 'swepamdatafiles'
    __table_args__ = (UniqueConstraint('flg_knd', 'ser_id', name='ser_flg_idx'),)
    __mapper_args__ = {'polymorphic_identity': 'swepamdatafiles', 'concrete': True}

    def __init__(self, **sdfile):
        super(SwepamDataFiles, self).__init__(**sdfile)


class MagDataFilesSerial(DataFilesSerial):
    data: List[MagDataSerial] = []

    def setData(self,data):
        super(MagDataFilesSerial, self).setData(data)
        self.datemin = self.data[0].ACE_date if self.data else None
        self.datemax = self.data[-1].ACE_date if self.data else None

    def toDB(self):
        return MagDataFiles(fname=self.fname,flg_knd=self.flg_knd,ser_id=self.ser_id,
                            datemin=self.datemin,datemax=self.datemax,parsed=self.parsed)

    def datatoDB(self):
        return [d.toDB() for d in self.data]


class SwepamDataFilesSerial(DataFilesSerial):
    data: List[SwepamDataSerial] = []

    def setData(self,data):
        super(SwepamDataFilesSerial, self).setData(data)
        self.datemin = self.data[0].ACE_date if self.data else None
        self.datemax = self.data[-1].ACE_date if self.data else None

    def toDB(self):
        return SwepamDataFiles(fname=self.fname,flg_knd=self.flg_knd,ser_id=self.ser_id,
                            datemin=self.datemin,datemax=self.datemax,parsed=self.parsed)

    def datatoDB(self):
        return [d.toDB() for d in self.data]