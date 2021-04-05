from sqlalchemy.sql import func
from  Lib.ace import Source
from . import models


def getArchiveStatus(db = None, product = None):
    mapper = {Source.Products.MAG: models.MagDataFiles,
              Source.Products.SWEPAM: models.SwepamDataFiles,
              Source.Products.PLASMA: models.SwepamDataFiles}
    model = mapper[product]
    qcvalues = db.session.query(model.flg_knd,
                        func.min(model.datemin).label('minValid'), func.max(model.datemax).label('maxValid')
                ).group_by(model.flg_knd).order_by(model.flg_knd).all()

    return qcvalues


_magavg = """
INSERT INTO magdata (`ACE_date`,`Bmag`,`Bx`,`By`,`Bz`,`flg_scstrm`,flg_knd)
select TIMESTAMP(MAKEDATE(yr,doy),MAKETIME(hr,0,0))as ACE_date,Bmag,Bx,`By`,Bz,0,%(flg_knd)s from
(
    select avg(Bmag) as Bmag,avg(Bx) as Bx,avg(`By`) as `By`,avg(Bz) as Bz,year(ACE_date) as yr,DAYOFYEAR(ACE_date) as doy,hour(ACE_date) as hr
    from min_magdata where flg_knd=%(flg_knd)s
    group by yr,doy,hr
) as tbl
ON DUPLICATE KEY UPDATE
`Bmag` = CASE WHEN (magdata.flg_knd <=%(flg_knd)s) THEN VALUES(`Bmag`) ELSE magdata.`Bmag` END, 
`Bx` = CASE WHEN (magdata.flg_knd <=%(flg_knd)s) THEN VALUES(`Bx`) ELSE magdata.`Bx` END, 
`By` = CASE WHEN (magdata.flg_knd <=%(flg_knd)s) THEN VALUES(`By`) ELSE magdata.`By` END, 
`Bz` = CASE WHEN (magdata.flg_knd <=%(flg_knd)s) THEN VALUES(`Bz`) ELSE magdata.`Bz` END,
flg_knd = CASE WHEN (magdata.flg_knd <=%(flg_knd)s) THEN VALUES(flg_knd) ELSE magdata.flg_knd END,
flg_scstrm = CASE WHEN (magdata.flg_knd <=%(flg_knd)s) THEN VALUES(flg_scstrm) ELSE magdata.flg_scstrm END;
"""

_swepamavg = """
INSERT INTO swepamdata (`ACE_date`,`proton_density`,`proton_temp`,`proton_speed`,flg_knd)
select TIMESTAMP(MAKEDATE(yr,doy),MAKETIME(hr,0,0))as ACE_date,`proton_density`,`proton_temp`,`proton_speed`,%(flg_knd)s from
(
    select avg(proton_density) as proton_density,avg(proton_temp) as proton_temp,avg(`proton_speed`) as `proton_speed`,year(ACE_date) as yr,DAYOFYEAR(ACE_date) as doy,hour(ACE_date) as hr
    from min_swepamdata where flg_knd=%(flg_knd)s
    group by yr,doy,hr
) as tbl
ON DUPLICATE KEY UPDATE 
`proton_density` = CASE WHEN (swepamdata.flg_knd <=%(flg_knd)s) THEN VALUES(`proton_density`) ELSE swepamdata.`proton_density` END, 
`proton_temp` = CASE WHEN (swepamdata.flg_knd <=%(flg_knd)s) THEN VALUES(`proton_temp`) ELSE swepamdata.`proton_temp` END, 
`proton_speed` = CASE WHEN (swepamdata.flg_knd <=%(flg_knd)s) THEN VALUES(`proton_speed`) ELSE swepamdata.`proton_speed` END,
flg_knd = CASE WHEN (swepamdata.flg_knd <=%(flg_knd)s) THEN VALUES(flg_knd) ELSE swepamdata.flg_knd END;
"""


def setAvgData(db = None, product = None, satellite = None):
    mapperQry = {Source.Products.MAG: _magavg,
                 Source.Products.SWEPAM: _swepamavg,
                 Source.Products.PLASMA: _swepamavg}
    mapperFKind = {Source.Satellites.DSCOVR: 10,
                   Source.Satellites.ACE: 0}

    query = mapperQry[product]
    flg_knd = mapperFKind[satellite]
    db.execute(query,{'flg_knd':flg_knd})


_groupHRData_old = """
with mindata as (
	select a.*,date_format( ACE_date, '%Y%m%d%H' ) as dfhour from min_::product::data a 
	where a.flg_knd=%(flg_knd)s
	order by ACE_date asc
) select dfhour,min(ACE_date) as datemin,max(ACE_date) as datemax,
CASE WHEN dfhour=(select max(dfhour) from mindata) THEN true ELSE false END as islast
from mindata
group by dfhour;
"""

_groupHRData = """
select dfhour,min(ACE_date) as datemin,max(ACE_date) as datemax,
CASE WHEN dfhour=maxdfhour THEN true ELSE false END as islast
from (
    select it.*,max(it.dfhour) over() as maxdfhour from (
        select a.*,date_format( ACE_date, '%Y%m%d%H' ) as dfhour 
        from min_::product::data a 
        where a.flg_knd=%(flg_knd)s
    ) it
    order by ACE_date asc
) as mindata
group by dfhour;
"""


def getGroupHRData(db = None, product = None, satellite = None):
    mapperFKind = {Source.Satellites.DSCOVR: 10,
                   Source.Satellites.ACE: 0}
    mapperProd = {Source.Products.MAG: 'mag',
                   Source.Products.SWEPAM: 'swepam',
                   Source.Products.PLASMA: 'swepam'}
    query = _groupHRData.replace('::product::', mapperProd[product])
    flg_knd = mapperFKind[satellite]
    pdct = db.asdict
    db.asdict = True
    results = db.fetch(query,{'flg_knd':flg_knd})
    db.asdict = pdct
    return results

_clearHRData_old = """
with arcvstats as (
    select max(datemax) as maxvalid 
    from ::product::datafiles where flg_knd=%(flg_knd)s and parsed=2
) delete from min_::product::data a where flg_knd<=%(flg_knd)s
and ACE_date<=(select maxvalid from arcvstats);
"""

_clearHRData = """
delete a from min_::product::data as a
inner join 
(
    select max(datemax) as maxvalid
    from ::product::datafiles where flg_knd=%(flg_knd)s and parsed=2
) b
ON a.ACE_date<=b.maxvalid
where a.flg_knd<=%(flg_knd)s;
"""

def clearHRData(db = None, product = None, satellite = None):
    mapperFKind = {Source.Satellites.DSCOVR: 10,
                   Source.Satellites.ACE: 0}
    mapperProd = {Source.Products.MAG: 'mag',
                   Source.Products.SWEPAM: 'swepam',
                   Source.Products.PLASMA: 'swepam'}
    query = _clearHRData.replace('::product::', mapperProd[product])
    flg_knd = mapperFKind[satellite]
    db.execute(query,{'flg_knd':flg_knd})
    return True


def setStorms(db = None):
    storm_proc =  "CALL storm_proc();"
    swif_proc =  "CALL SWIF_proc();"

    db.execute(storm_proc)
    #db.execute(swif_proc)
    return True