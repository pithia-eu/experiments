import re
from Lib.utils.ftp import FTPMod
from Lib.database import models

from app import ACELogger

class Parser(object):
    serializer = None
    recparser = None

    def _parse(self,flg_knd=0):
        match = re.findall(r':(.*-{2,}\n)(.*)', self._content, re.DOTALL)
        header = data = None
        if match and len(match[0])==2:
            header, data = match[0]
        else:
            return

        records = []
        for dl in data.splitlines():
            try:
                rowarg = self.__class__.recparser(dl.split())
                sdata = self.__class__.serializer(**rowarg, flg_knd=flg_knd, flg_scstrm=0)
                records.append(sdata)
            except Exception as e:
                pass
        return records

    def parse(self, dataset):
        records = self._parse(flg_knd=dataset.flg_knd)
        dataset.setData(records)

    def __init__(self,content):
        self._content = content


class SWEPAM(Parser):
    serializer = models.SwepamDataSerial
    recparser = lambda rec: {'ACE_date': f'{rec[0]}-{rec[1]}-{rec[2]} {rec[3][0:2]}:{rec[3][2:]}:00.000',
                             'proton_density': rec[7],'proton_speed': rec[8], 'proton_temp': rec[9]
                             }

    def __init__(self,content):
        super(SWEPAM, self).__init__(content)


class MAG(Parser):
    serializer = models.MagDataSerial
    recparser = lambda rec: {'ACE_date': f'{rec[0]}-{rec[1]}-{rec[2]} {rec[3][0:2]}:{rec[3][2:]}:00.000',
                             'Bx': rec[7],'By': rec[8], 'Bz': rec[9],
                             'Bmag': rec[10]
                             }

    def __init__(self,content):
        super(MAG, self).__init__(content)


class Collection(object):

    @property
    def datasets(self):
        return self._datasets if self._fdatasets is None else self._fdatasets

    def filter(self,arcvrange = None, allowparse = None):
        datasets = [d for d in self.datasets]
        self._fdatasets = []
        for dataset in datasets:
            drange = dataset.range()
            if arcvrange.contains(drange):
                continue
            if allowparse and not allowparse.contains(drange):
                continue
            self._fdatasets.append(dataset)

    def listProducts(self, dir=None):
        response = self.ftp.list(dir,pattern=self.source.listftppattern)
        return response

    def parse(self):
        _parsermap = {self.source.Products.MAG: MAG, self.source.Products.SWEPAM: SWEPAM}
        for i,ds in enumerate(self.datasets):
            ACELogger.logger.info(f'Parsing {i+1}/{len(self.datasets)} dataset: {ds.fname}')
            try:
                content = self.ftp.read(ds.fname, basepath = self.source.baseurl.pathstr)
            except Exception as e:
                ACELogger.logger.error(f'Error {e} while fetching dataset: {ds.fname}')
                continue

            try:
                _parser = _parsermap[self.source.product]
                parser = _parser(content)
                parser.parse(ds)
            except Exception as e:
                ACELogger.logger.error(f'Error {e} while parsing dataset: {ds.fname}')
                continue

    def scan(self, nowcast = None, base=None):
        response = list()
        if self.source.search:
            try:
                response = self.listProducts(dir=self.source.baseurl.pathstr)
            except Exception as e:
                ACELogger.logger.error(f'Error while listing products for source: {e}')
                return
            if not response:
                ACELogger.logger.warning(f'No products where listed for source')
                return
        else:
            response = [self.source.file,]
        for fname in response:
            try:
                serial = self.source.serialize(fname, nowcast=nowcast)
            except Exception as e:
                ACELogger.logger.error(f'Error while serializing source: {e}')
                continue
            if not serial:
                ACELogger.logger.error('Empty source')
                continue
            self._datasets.append(serial)

        self._datasets = sorted(self._datasets, key=lambda x:x.datemin)

    def __init__(self, source, ftp=None):
        self.source = source
        self._datasets = []
        self._fdatasets = None
        self._conown = False
        if not ftp:
            self._conown = True
            self.ftp = FTPMod(addr=self.source.provider.host.host,username=self.source.provider.user,password=self.source.provider.password)
        else:
            self.ftp = ftp

    def __del__(self):
        if self._conown:
            try:
                self.ftp.disconnect()
            except Exception as e:
                pass


