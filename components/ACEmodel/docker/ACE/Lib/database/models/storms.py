from sqlalchemy.dialects import mysql
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, VARCHAR, Boolean,SMALLINT, String, Text, DateTime, Float, text, func, event, Interval

from app import Base


class StormsAce(Base):
    __tablename__ = 'stormsace'
    __table_args__ = (UniqueConstraint('Storm_Onset', 'Bz_minTime', name='StormDef'),)

    id = Column(mysql.INTEGER, primary_key = True)
    Storm_Onset = Column(DateTime(), index=True, unique=True, nullable=False)
    Bz_minTime = Column(DateTime(), index=True, nullable=False)
    Storm_Offset = Column(DateTime(), index=True, nullable=True)
    isclosed = Column(mysql.TINYINT(1), index=True, nullable=False,default=0)

    def __init__(self, **sdata):
        super(StormsAce, self).__init__(**sdata)