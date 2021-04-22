import portion as P
from datetime import datetime, timedelta
from app import CONF as cfg
from Lib.database.base import DBUtils
from Lib.database import operations as dbo
from Lib.ace import Sources, Source
from Lib.parsers import AceCollection
from Lib.parsers import DscovrCollection

from app import ACELogger

class Manager(object):

    @staticmethod
    def rangeduration(rng):
        if rng.empty:
            return timedelta()
        return sum([at.upper-at.lower for at in list(rng)],timedelta())

    def setClock(self):
        self.clock = datetime.utcnow()

    def getArchiveStatus(self,product= None, base=-1):
        dbstatus = dbo.getArchiveStatus(db = self.db.ormdb, product=product)
        mntotal = [rec.minValid for rec in dbstatus]
        mxtotal = [rec.maxValid for rec in dbstatus]
        mnvalid = [rec.minValid for rec in dbstatus if rec.flg_knd>=base]
        mxvalid = [rec.maxValid for rec in dbstatus if rec.flg_knd>=base]
        return min(mntotal) if mntotal else None,max(mxtotal) if mxtotal else None,\
               min(mnvalid) if mnvalid else None,max(mxvalid) if mxvalid else None

    def _storeDSCOVR(self,cl,product,satellite):
        dtDB = cl.dataset.datatoDB()
        self.db.ormdb.bulk_update(dtDB)

        datasets = cl.source.createHourlyDatasets(db=self.db)
        dfsDB = []
        for df in datasets:
            if df.ser_id == 99999999:
                cl.dataset.fname = df.fname
                cl.dataset.datemin = df.datemin
                cl.dataset.datemax = df.datemax
                cl.dataset.parsed = 1
                dfsDB.append(cl.dataset.toDB())
            else:
                dfsDB.append(df.toDB())

        self.db.ormdb.bulk_update(dfsDB)
        dbo.setAvgData(db=self.db.rawdb, product=product, satellite=satellite)
        dbo.clearHRData(db=self.db.rawdb, product=product, satellite=satellite)

    def handleDSCOVR(self,gk,group):
        product, satellite = gk

        ACELogger.logger.info(f'Handling {product}@{satellite.name}:{1}m')

        mntotal,mxtotal,mnvalid,mxvalid = self.getArchiveStatus(product=product,base=10)
        arcvrange = P.closed(mnvalid, mxvalid) if mnvalid and mxvalid else P.open(datetime.min, datetime.min)

        ranges = [mnvalid,mxvalid] if mnvalid and mxvalid else []
        cls = []
        for source in group:
            cl = DscovrCollection(source)
            cl.scan(nowcast=self.clock)
            cls.append(cl)
            ranges += [cl.dataset.datemin,cl.dataset.datemax]

        maxrange = P.closed(min(ranges), max(ranges))
        maxcover = maxrange - arcvrange

        ranges = []
        for cl in cls:
            clrange = cl.dataset.range()
            rng = { 'data': cl,
                    'clrange': {'range': clrange, 'duration': None},
                    'covisect': {'range': clrange & maxcover, 'duration': None},
                    'arcvisect': {'range': clrange & arcvrange, 'duration': None}
                }
            for k in ('clrange','covisect','arcvisect'):
                rng[k]['duration'] = Manager.rangeduration(rng[k]['range'])
            ranges.append(rng)

        ranges = sorted(ranges, key = lambda x: (-x['covisect']['duration'], x['clrange']['duration']))
        i = 0
        cl = None
        while i<=len(ranges)-1:
            rng = ranges[i]
            cl = rng['data']
            cl.parse()
            newclrange = P.closed(cl.dataset.datemin,self.clock)
            newcovisect = newclrange & maxcover
            if rng['covisect']['range'].contains(newcovisect):
                break
            i+=1

        if not cl:
            return

        try:
            ACELogger.logger.info(f'Storing {product}@{satellite.name}:{1}m')
            self._storeDSCOVR(cl,product,satellite)
        except Exception as e:
            ACELogger.logger.error(f"Error {e} while storing: {product}@{satellite.name}:{1}m")


    def _storeACE(self,cls,product,satellite,resolution):
        mapperRes = {timedelta(minutes=1): 0,
                    timedelta(hours=1): 1}

        def iterator():
            dfsDB = list()
            dtDB = list()
            tot = 0
            cl = None
            icl = idf = 0
            for icl, cl in enumerate(cls):
                for idf,df in enumerate(cl.datasets):
                    if df.nowcast:
                        df.parsed = 1
                    else:
                        df.parsed = 2
                    dfsDB.append(df.toDB())
                    dtDB += df.datatoDB()
                    if len(dtDB)>=1000:
                        tot+=len(dtDB)
                        yield (icl+1,len(cls)),(idf+1,(len(cl.datasets))),tot,dtDB, dfsDB
                        dfsDB = list()
                        dtDB = list()
            if cls and cl:
                tot+=len(dtDB)
                yield (icl+1,len(cls)),(idf+1,(len(cl.datasets))),tot,dtDB, dfsDB

        for cli, dfi, tot, dtDB,dfsDB in iterator():
            if not dtDB:
                continue
            ACELogger.logger.info(f"Storing collection: {cli[0]}/{cli[1]}, dataset: {dfi[0]}/{dfi[1]}, total records: {tot}")
            update = dtDB[0].update_fld_names + ['flg_knd', ]
            self.db.ormdb.bulk_update(dtDB, update=update, cond_flg_knd=mapperRes[resolution])
            self.db.ormdb.bulk_update(dfsDB)

        if mapperRes[resolution] == 0:
            dbo.setAvgData(db=self.db.rawdb, product=product, satellite=satellite)
            dbo.clearHRData(db=self.db.rawdb, product=product, satellite=satellite)

    def handleACE(self,gk,group):
        mapperRes = {timedelta(minutes=1): 0,
                    timedelta(hours=1): 1}
        product, satellite, resolution = gk

        ACELogger.logger.info(f'Handling {product}@{satellite.name}:{resolution.total_seconds()//60}m')

        mntotal,mxtotal, mnvalid,mxvalid = self.getArchiveStatus(product=product,base=mapperRes[resolution])
        arcvrange = P.closed(mnvalid, mxvalid) if mnvalid and mxvalid else P.open(datetime.min, datetime.min)
        allowparse = P.closed(cfg['BASE_DATE'],datetime.max) if cfg['BASE_DATE'] else  P.open(datetime.min, datetime.max)

        cls = []
        for source in group:
            if cfg['NODATA_ACE_THRESH'] and mntotal and mxvalid and \
                    (cfg['BASE_DATE'] and cfg['BASE_DATE']>=mntotal) and \
                    (mxvalid and (self.clock-mxvalid)<cfg['NODATA_ACE_THRESH']):
                ACELogger.logger.warning(f"Skipping Source {source.baseurl.url}")
                ACELogger.logger.warning(
                    f"Higher priority stream gap from nowcasting less than threshold: {cfg['NODATA_ACE_THRESH'].total_seconds() // 60}m")
                continue
            cl = AceCollection(source)
            cl.scan(nowcast=self.clock)
            cl.filter(arcvrange = arcvrange, allowparse = allowparse)
            cl.parse()
            cl.filter(arcvrange = arcvrange, allowparse = allowparse)
            cls.append(cl)

        try:
            ACELogger.logger.info(f'Storing {product}@{satellite.name}:{resolution.total_seconds() // 60}m')
            self._storeACE(cls,product,satellite,resolution)
        except Exception as e:
            ACELogger.logger.error(f"Error {e} while storing: {product}@{satellite.name}:{resolution.total_seconds()//60}m")

    def manage(self):
        self.setClock()

        iterator_DSCOVR = self.sources.groupby(
            group=(Sources.Groups.PRODUCT, Sources.Groups.SATELLITE),
            select=Source.Satellites.DSCOVR
        )
        for gk, group in iterator_DSCOVR:
            try:
                self.handleDSCOVR(gk, group)
            except Exception as e:
                ACELogger.logger.error(f'Error while handling DSCOVR sources: {e}')

        iterator_ACE = self.sources.groupby(
            group=(Sources.Groups.PRODUCT, Sources.Groups.SATELLITE, Sources.Groups.RESOLUTION),
            select=Source.Satellites.ACE
        )
        for gk, group in iterator_ACE:
            try:
                self.handleACE(gk, group)
            except Exception as e:
                ACELogger.logger.error(f'Error while handling ACE sources: {e}')

        try:
            ACELogger.logger.info(f'Calculating Storms')
            dbo.setStorms(db= self.db.rawdb)
        except Exception as e:
            ACELogger.logger.error(f'Error while setting ACE Storms: {e}')

    def __init__(self):
        self.clock = None
        self._dbstatus = None
        try:
            self.sources = Sources()
        except Exception as e:
            ACELogger.logger.error(f'Error while parsing sources: {e}')
            exit(0)

        try:
            self.db = DBUtils()
        except Exception as e:
            ACELogger.logger.error(f'Unable to initialize database: {e}')
            exit(0)

    def disconnect(self):
        try:
            self.db.disconnect()
        except Exception as e:
            pass

    def __del__(self):
        self.disconnect()