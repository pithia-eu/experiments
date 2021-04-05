import os,sys
sys.path.insert(0, os.path.abspath('../'))
import typer
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import ciso8601
import numpy as np
from app import CONF as cfg
from Lib.database.base import DBUtils

from app import ACELogger

app = typer.Typer()

_QStorms = """
select * from stormsace as a
order by a.Storm_Onset desc LIMIT 10;
"""

_QMinBmag = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bmag is not Null)
order by a.Bmag asc LIMIT 10;
"""

_QMaxBmag = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bmag is not Null)
order by a.Bmag desc LIMIT 10;
"""

_QMinBx = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bx is not Null)
order by a.Bx asc LIMIT 10;
"""

_QMaxBx = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bx is not Null)
order by a.Bx desc LIMIT 10;
"""

_QMinBy = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.By is not Null)
order by a.By asc LIMIT 10;
"""

_QMaxBy = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.By is not Null)
order by a.By desc LIMIT 10;
"""

_QMinBz = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bz is not Null)
order by a.Bz asc LIMIT 10;
"""

_QMaxBz = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
having (a.Bz is not Null)
order by a.Bz desc LIMIT 10;
"""

@app.command()
def recentStorms():
    """
    Find Recent Storms
    """

    ACELogger.logger.info(f"Querying 10 Most recent Ionosperic storms")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QStorms)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)

@app.command()
def minBx():
    """
    Find Historical Minima for parameter Bx
    """

    ACELogger.logger.info(f"Querying 10 Minimum Bx values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMinBx)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)


@app.command()
def maxBx():
    """
    Find Historical Maxima for parameter Bx
    """

    ACELogger.logger.info(f"Querying 10 Maximum Bx values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMaxBx)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)


@app.command()
def minBy():
    """
    Find Historical Minima for parameter By
    """

    ACELogger.logger.info(f"Querying 10 Minimum By values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMinBy)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)


@app.command()
def maxBy():
    """
    Find Historical Maxima for parameters By
    """

    ACELogger.logger.info(f"Querying 10 Maximum By values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMaxBy)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)

@app.command()
def minBz():
    """
    Find Historical Minima for parameter Bz
    """

    ACELogger.logger.info(f"Querying 10 Minimum Bz values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMinBz)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)

@app.command()
def maxBz():
    """
    Find Historical Maxima for parameter Bz
    """

    ACELogger.logger.info(f"Querying 10 Maximum Bz values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMaxBz)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)


@app.command()
def minBmag():
    """
    Find Historical Minima for parameter Bmag
    """

    ACELogger.logger.info(f"Querying 10 Minimum Bmag values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMinBmag)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)


@app.command()
def maxBmag():
    """
    Find Historical Maxima for parameter Bmag
    """

    ACELogger.logger.info(f"Querying 10 Maximum Bmag values")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_QMaxBmag)

    if not data:
        ACELogger.logger.info(f"No data found ")

    print(data)

if __name__ == '__main__':
    app()