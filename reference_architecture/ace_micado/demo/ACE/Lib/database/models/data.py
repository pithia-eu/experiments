from datetime import datetime
from sqlalchemy.dialects import mysql
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, VARCHAR, Boolean,SMALLINT, String, Text, DateTime, Float, text, func, event, Interval
from sqlalchemy import func
from sqlalchemy.orm import relationship
from typing import Optional, Literal
from pydantic import BaseModel, validator, Extra, Field, ValidationError, root_validator

from app import Base


class MagDataMinute(Base):
    __tablename__ = 'min_magdata'

    ACE_date = Column(DateTime, primary_key = True)
    flg_knd = Column(mysql.TINYINT, index=True, primary_key = True)
    Bmag = Column(mysql.FLOAT,index=True, nullable=True)
    Bx = Column(mysql.FLOAT, index=True, nullable=True)
    By = Column(mysql.FLOAT, index=True, nullable=True)
    Bz = Column(mysql.FLOAT, index=True, nullable=True)

    def __init__(self, **mdata):
        super(MagDataMinute, self).__init__(**mdata)

class MagData(Base):
    __tablename__ = 'magdata'
    __table_args__ = (UniqueConstraint('flg_knd', 'ACE_date', name='date_flg_idx'),)

    id = Column(mysql.INTEGER, primary_key = True)
    ACE_date = Column(DateTime, nullable=False, unique=True)
    Bmag = Column(mysql.FLOAT,index=True, nullable=True)
    Bx = Column(mysql.FLOAT, index=True, nullable=True)
    By = Column(mysql.FLOAT, index=True, nullable=True)
    Bz = Column(mysql.FLOAT, index=True, nullable=True)
    flg_knd = Column(mysql.TINYINT, index=True, nullable=False)
    flg_scstrm = Column(mysql.TINYINT, index=True, nullable=False, default=0)

    def __init__(self, **mdata):
        super(MagData, self).__init__(**mdata)


class SwepamDataMinute(Base):
    __tablename__ = 'min_swepamdata'

    ACE_date = Column(DateTime, primary_key = True)
    flg_knd = Column(mysql.TINYINT, index=True, primary_key=True)
    proton_density = Column(mysql.FLOAT,index=True, nullable=True)
    proton_temp = Column(mysql.DOUBLE, index=True, nullable=True)
    proton_speed = Column(mysql.FLOAT, index=True, nullable=True)

    def __init__(self, **sdata):
        super(SwepamDataMinute, self).__init__(**sdata)

class SwepamData(Base):
    __tablename__ = 'swepamdata'
    __table_args__ = (UniqueConstraint('flg_knd', 'ACE_date', name='date_flg_idx'),)

    id = Column(mysql.INTEGER, primary_key = True)
    ACE_date = Column(DateTime, nullable=False, unique=True)
    proton_density = Column(mysql.FLOAT,index=True, nullable=True)
    proton_temp = Column(mysql.DOUBLE, index=True, nullable=True)
    proton_speed = Column(mysql.FLOAT, index=True, nullable=True)
    flg_knd = Column(mysql.TINYINT, index=True, nullable=False)

    def __init__(self, **sdata):
        super(SwepamData, self).__init__(**sdata)


class MagDataSerial(BaseModel):
    id: Optional[int] = None
    ACE_date: datetime
    Bmag: Optional[float] = None
    Bx: Optional[float] = None
    By: Optional[float] = None
    Bz: Optional[float] = None
    flg_knd: int
    flg_scstrm: int

    @validator('ACE_date',pre=True, always=True)
    def dateVLD(cls, v):
        v = datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')
        return v

    @validator('Bmag','Bx','By','Bz',pre=True, always=True)
    def dataVLD(cls, v):
        v = float(v)
        return None if v==-999.9 else v

    def toDB(self):
        if self.flg_knd%10==0:
            return MagDataMinute(ACE_date=self.ACE_date,Bmag=self.Bmag,
                           Bx=self.Bx,By=self.By,Bz=self.Bz,
                           flg_knd=self.flg_knd)
        else:
            return MagData(ACE_date=self.ACE_date,Bmag=self.Bmag,
                           Bx=self.Bx,By=self.By,Bz=self.Bz,
                           flg_knd=self.flg_knd,flg_scstrm=self.flg_scstrm)

    def todict(self):
        return self.dict()

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class SwepamDataSerial(BaseModel):
    id: Optional[int] = None
    ACE_date: datetime
    proton_density: Optional[float] = None
    proton_temp: Optional[float] = None
    proton_speed: Optional[float] = None
    flg_knd: int

    @validator('ACE_date',pre=True, always=True)
    def dateVLD(cls, v):
        v = datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')
        return v

    @validator('proton_density','proton_speed',pre=True, always=True)
    def protAVLD(cls, v):
        v=float(v)
        return None if v==-9999.9 else v

    @validator('proton_temp',pre=True, always=True)
    def protBVLD(cls, v):
        v = float(v)
        return None if v==-1.00e+05 else v

    def toDB(self):
        if self.flg_knd%10==0:
            return SwepamDataMinute(ACE_date=self.ACE_date,proton_density=self.proton_density,
                           proton_temp=self.proton_temp,proton_speed=self.proton_speed,
                           flg_knd=self.flg_knd)
        else:
            return SwepamData(ACE_date=self.ACE_date,proton_density=self.proton_density,
                           proton_temp=self.proton_temp,proton_speed=self.proton_speed,
                           flg_knd=self.flg_knd)

    def todict(self):
        return self.dict()

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore