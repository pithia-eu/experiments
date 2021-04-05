import re
import itertools
from enum import Enum, IntFlag
from furl import furl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pytimeparse.timeparse import timeparse
from app import CONF as cfg
from Lib.database import models


class Provider(object):
    Providers = Enum('Name', 'SWPC_RTSW SWPC_ACE')
    Protocol = Enum('Type', 'HTTP HTTPS FTP SFTP FTPS')

    def __init__(self,data):
        self._name = data['name']
        self.active = data['active']
        self.products = [kv[0] for kv in [list(d.items())[0] for d in data['products']] if kv[1]]
        _map = {'swpcRTSW': Provider.Providers.SWPC_RTSW,'swpcACE': Provider.Providers.SWPC_ACE}
        if not self._name in _map:
            raise(f'Unknown Provider: {data["provider"]}.Valid values <{", ".join(list(_map.keys()))}>')
        else:
            self.name = _map[self._name]

        prot_ = [i for i in Provider.Protocol if i.name == data['protocol'].upper()]
        if not prot_:
            raise (
                f'Communication protocol: {data["protocol"]} not supported <{", ".join(i.name for i in Provider.Protocol)}>')
        self.cprotocol = prot_[0]
        self._host = data['host']
        self.host = furl(scheme=self.cprotocol.name.lower(), host=self._host)

        if 'user' in data:
            self.user = data['user']
        if 'pass' in data:
            self.password = data['pass']


class Source(object):
    Products = Enum('Name', 'MAG SWEPAM PLASMA')
    Satellites = Enum('Name', 'ACE DSCOVR')
    DurationBase = Enum('Type', 'NOW START')

    @property
    def providername(self):
        return self.provider.name

    @property
    def cprotocol(self):
        return self.provider.cprotocol

    @property
    def baseurl(self):
        if self._baseurl:
            return self._baseurl
        baseurl = furl(scheme=self.cprotocol.name.lower(),host=self._host,path=self._path)
        if self.search is False:
            file = baseurl.path.segments[-1]
            self.file = file
            baseurl.path.segments = baseurl.path.segments[:-1]

        self._baseurl = baseurl
        return self._baseurl

    def __init__(self,src,provider):
        self.provider = provider
        self.product = src['product']
        self.resolution = timedelta(seconds=timeparse(src['resolution']))
        self._duration = self._durationBase = None
        _range = re.findall(r'(\w+)\.?(\w)?', src['range']) if 'range' in src else None
        if _range:
            _duration, _begin = _range[0]
            if _duration[-1:] in ('M'):
                self._duration = relativedelta(months=+int(_duration[:-1]))
            else:
                self._duration = timedelta(seconds=timeparse(_duration))
            if _begin:
                _map = {'N':Source.DurationBase.NOW, 'S':Source.DurationBase.START}
                if _begin in _map:
                    self._durationBase = _map[_begin]
                else:
                    raise (f'Unknown Duration Base: {_begin}.Valid values <{", ".join(list(_map.keys()))}>')

        sat_ = [i for i in Source.Satellites if i.name == src['satellite'].upper()]
        if not sat_:
            raise (f'Requested satellite: {src["satellite"]} not supported <{", ".join(i.name for i in Source.Satellites)}>')
        self.satellite = sat_[0]

        self.file = None
        self._baseurl = None
        self._host = self.provider.host.host
        self._path = src['path']
        self.search = False
        if self._path.endswith('*'):
            self._path = self._path[:-1]
            self.search = True
            self._pattern = re.compile(src['pattern']) if 'pattern' in src else None

        _baseurl = self.baseurl


class SourceACE(Source):
    _PATTERNARCV = re.compile('(\d{4})(\d{2})(\d{2})?_ace_(\w+)_(\dm|\dh).txt')
    _PATTERNNCST = re.compile('ace_(\w+)_(\dm|\dh).txt')
    _FLG_KND = {timedelta(minutes=1):0,timedelta(hours=1):1}

    @property
    def listftppattern(self):
        if self.search is False:
            return None
        satellite = self.satellite.name.lower()
        product = self.product.name.lower()
        resolution = f'{self.resolution.seconds // 3600}h' if self.resolution.seconds%3600==0 else f'{self.resolution.seconds // 60}m'
        pattern = f'*_{satellite}_{product}_{resolution}.txt'
        return pattern

    @property
    def nowcast(self):
        if self._durationBase == Source.DurationBase.NOW:
            return True
        return False

    def _extractmetaArchive(self, fname):
        felem = re.findall(SourceACE._PATTERNARCV, fname)
        felem = felem[0] if felem and len(felem[0])==5 else None
        if not felem:
            return False
        ser_id = ''.join(felem[0:3])

        if felem[4]=='1m':
            datemin = datetime.strptime(f'{felem[0]}{felem[1]}{felem[2]}T00:00:00', '%Y%m%dT%H:%M:%S')
            datemax = datemin+ timedelta(days=1)-self.resolution
        elif felem[4]=='1h':
            datemin = datetime.strptime(f'{felem[0]}{felem[1]}01T00:00:00', '%Y%m%dT%H:%M:%S')
            datemax = datemin+ relativedelta(months=+1)-self.resolution
        else:
            return False

        return (ser_id, datemin, datemax)

    def _extractmetaNowcast(self, fname, nowcast = None):
        felem = re.findall(SourceACE._PATTERNNCST, fname)
        felem = felem[0] if felem and len(felem[0]) == 2 else None
        if not felem:
            return False
        ser_id = 99999999

        nowcast = nowcast or datetime.utcnow()
        datemax = nowcast
        datemin = datemax-self._duration-self.resolution

        return (ser_id, datemin, datemax)

    def serialize(self,fname, nowcast = None):
        meta = None
        if self.nowcast:
            meta = self._extractmetaNowcast(fname,nowcast=nowcast)
        else:
            meta = self._extractmetaArchive(fname)

        if not meta:
            return False
        ser_id, datemin, datemax = meta
        isnowcast = self.nowcast
        if nowcast:
            if datemax>=nowcast:
                isnowcast = True
            datemax = min(nowcast, datemax)

        if self.resolution in SourceACE._FLG_KND:
            flg_knd = SourceACE._FLG_KND[self.resolution]
        else:
            return False

        mapper = {Source.Products.MAG: models.MagDataFilesSerial,
                  Source.Products.SWEPAM: models.SwepamDataFilesSerial}
        serializer = mapper[self.product]
        sdata = serializer(fname=fname,ser_id=ser_id,flg_knd=flg_knd,datemin=datemin, datemax=datemax, parsed=0, nowcast=isnowcast)
        return sdata

    def __init__(self, *args, **kwargs):
        super(SourceACE, self).__init__(*args,**kwargs)


class SourceDSCVR(Source):
    _PATTERNNCST = re.compile('(\w+)-(\d)-(\w+).json')
    _FLG_KND = {timedelta(minutes=1):10,}

    @property
    def listftppattern(self):
        if self.search is False:
            return None

        product = self.product.name.lower()
        pattern = f'{product}-*-*.json'
        return pattern

    @property
    def nowcast(self):
        if self._durationBase == Source.DurationBase.NOW:
            return True
        return False

    def _extractmetaNowcast(self, fname, nowcast = None):
        durationmap = {'minute':timedelta(minutes=1), 'hour': timedelta(hours=1), 'day':timedelta(days=1)}
        felem = re.findall(SourceDSCVR._PATTERNNCST, fname)
        felem = felem[0] if felem and len(felem[0]) == 3 else None
        if not felem:
            return False
        ser_id = 99999999

        product = felem[0]
        mul = int(felem[1])
        durbase = durationmap[felem[2].lower()]
        duration = mul*durbase
        if self._duration!=duration:
            raise ValueError(f'Error parsing DSCOVR source filename extracted duration: {duration} does not match nominal duration: {self._duration}')
        nowcast = nowcast or datetime.utcnow()
        datemax = nowcast
        datemin = datemax-duration-self.resolution

        return (ser_id, datemin, datemax)

    def createHourlyDatasets(self,db=None):
        from Lib.database import operations as dbo
        mapperSerial = {Source.Products.MAG: models.MagDataFilesSerial,
                  Source.Products.SWEPAM: models.SwepamDataFilesSerial,
                  Source.Products.PLASMA: models.SwepamDataFilesSerial}
        mapperProd = {Source.Products.MAG: 'mag',
                      Source.Products.SWEPAM: 'swepam',
                      Source.Products.PLASMA: 'swepam'}

        serializer = mapperSerial[self.product]
        flg_knd = SourceDSCVR._FLG_KND[self.resolution]
        result = dbo.getGroupHRData(db=db.rawdb, product=self.product, satellite=self.satellite)
        datasets = []
        for rec in result:
            if rec['islast']:
                fname = f"{self.satellite.name.lower()}_{mapperProd[self.product]}_1m.json"
                sdata = serializer(fname=fname, ser_id=99999999, flg_knd=flg_knd,
                                   datemin=rec['datemin'], datemax=rec['datemax'], parsed=1)
            else:
                fname = f"{rec['dfhour']}_{self.satellite.name.lower()}_{mapperProd[self.product]}_1m.json"
                sdata = serializer(fname=fname,ser_id=int(rec['dfhour']),flg_knd=flg_knd,
                               datemin=rec['datemin'], datemax=rec['datemax'], parsed=2)
            datasets.append(sdata)
        return datasets

    def serialize(self,fname, nowcast = None):
        meta = None
        if self.nowcast:
            meta = self._extractmetaNowcast(fname, nowcast = nowcast)
        else:
            raise NotImplementedError('DSCOVR datasets format should be strictly nowcasting')

        if not meta:
            return False
        ser_id, datemin, datemax = meta
        isnowcast = self.nowcast

        if self.resolution in SourceDSCVR._FLG_KND:
            flg_knd = SourceDSCVR._FLG_KND[self.resolution]
        else:
            return False

        mapper = {Source.Products.MAG: models.MagDataFilesSerial,
                  Source.Products.SWEPAM: models.SwepamDataFilesSerial,
                  Source.Products.PLASMA: models.SwepamDataFilesSerial}
        serializer = mapper[self.product]
        sdata = serializer(fname=fname,ser_id=ser_id,flg_knd=flg_knd,datemin=datemin, datemax=datemax, parsed=0, nowcast=isnowcast)
        return sdata

    def __init__(self, *args, **kwargs):
        super(SourceDSCVR, self).__init__(*args,**kwargs)


class Sources(object):
    Groups = Enum('Name','PRODUCT PROTOCOL PROVIDER RESOLUTION SATELLITE')

    @property
    def products(self):
        products = set([s.product for s in self._sources])
        return sorted(list(products))

    def groupby(self, group, select = None):
        mapper = {  Sources.Groups.PRODUCT:'product',
                    Sources.Groups.PROTOCOL:'cprotocol',
                    Sources.Groups.PROVIDER:'providername',
                    Sources.Groups.RESOLUTION:'resolution',
                    Sources.Groups.SATELLITE: 'satellite',
        }
        select = [select,] if select and not isinstance(select,(list,tuple)) else None

        selector = {}
        if select:
            for s in select:
                if not s.__class__ in selector:
                    selector[s.__class__] = []
                selector[s.__class__].append(s)

        group_ = [mapper[g] for g in group]
        sources_ = sorted(self._sources,key = lambda x: (x.product.name, x.satellite.name,-x.resolution))
        self._iterator = itertools.groupby(sources_, lambda x: [getattr(x,g) for g in group_])

        for k,g in self._iterator:
            filterflag = False
            for k_ in k:
                if k_.__class__ in selector and k_ not in selector[k_.__class__]:
                    filterflag=True
            if filterflag:
                continue

            yield k,g

    def __init__(self):
        self._cfgprov = {obj['name']:obj for obj in cfg['Providers']}
        self._cfgprod = cfg['Products']
        self._providers = {}
        self._sources = []
        for prov, provs in self._cfgprov.items():
            self._providers[prov] = Provider(provs)

        for prod,srcs in self._cfgprod.items():
            prod_ = [i for i in Source.Products if i.name==prod.upper()]
            if not prod_:
                raise(f'Requested product: {prod} not supported <{", ".join(i.name for i in Source.Products)}>')

            prod_ = prod_[0]

            for src in srcs:
                if not src['active']:
                    continue

                if not src['provider'] in self._cfgprov:
                    raise (f'Requested provider: {src["provider"]} not supported <{", ".join(list(self._cfgprov.keys()))}>')

                if not self._providers[src['provider']].active:
                    continue
                if not prod in self._providers[src['provider']].products:
                    continue

                src['product'] = prod_
                _srccl = SourceACE if src['satellite'] == 'ACE' else SourceDSCVR
                src_ = _srccl(src,self._providers[src['provider']])
                self._sources.append(src_)

