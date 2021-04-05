import re
import requests
from Lib.utils.http import HTTPMod
from Lib.database import models

from app import ACELogger

def getLinks(soup,baseurl,pattern):
    s2aroot = soup.find(tag_='body')
    links = s2aroot.find_all('a')
    res = []
    for linkobj in links:
        link = linkobj['href']
        text = linkobj.text
        curl = baseurl.copy().set(path=link)
        fname = curl.path.segments[-1]
        felem = re.findall(pattern, fname)
        if felem:
            res.append(curl)
        else:
            continue

    return res

class Parser(object):
    serializer = None
    recparser = None

    def _parse(self,flg_knd=0):
        header = self._content[0]
        records = []
        for dl in self._content[1:]:
            try:
                rowarg = self.__class__.recparser({key:dl[i] for i,key in enumerate(header)})
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


class PLASMA(Parser):
    serializer = models.SwepamDataSerial
    recparser = lambda rec: {'ACE_date': rec['time_tag'],
                             'proton_density': rec['density'],'proton_speed': rec['speed'], 'proton_temp': rec['temperature']
                             }

    def __init__(self,content):
        super(PLASMA, self).__init__(content)


class MAG(Parser):
    serializer = models.MagDataSerial
    recparser = lambda rec: {'ACE_date': rec['time_tag'],
                             'Bx': rec['bx_gsm'],'By': rec['by_gsm'], 'Bz': rec['bz_gsm'],
                             'Bmag': rec['bt']
                             }

    def __init__(self,content):
        super(MAG, self).__init__(content)


class Collection(object):
    @property
    def dataset(self):
        if self._datasets:
            if len(self._datasets)==1:
                return self._datasets[0]
            else:
                raise AssertionError('More than one datasets in DSCOVR Collection')
        else:
            return None

    def listProducts(self, dir=None):
        response = self.http.list(dir,pattern=self.source.listftppattern,callback=getLinks)
        return response

    def parse(self):
        _parsermap = {self.source.Products.MAG: MAG, self.source.Products.PLASMA: PLASMA}
        for ds in self._datasets:
            ACELogger.logger.info(f'Parsing dataset: {ds.fname}')
            try:
                content = self.http.read(ds.fname, path = self.source.baseurl.pathstr, json=True)
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

    def scan(self, base=None, nowcast = None):
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
                serial = self.source.serialize(fname, nowcast = nowcast)
            except Exception as e:
                ACELogger.logger.error(f'Error while serializing source: {e}')
                continue
            if not serial:
                ACELogger.logger.error('Empty source')
                continue
            self._datasets.append(serial)

        self._datasets = sorted(self._datasets, key=lambda x:x.datemin)

    def __init__(self, source, http=None):
        self.source = source
        self._datasets = []
        self._conown = False
        if not http:
            self._conown = True
            self.http = HTTPMod(scheme=self.source.cprotocol.name.lower(), host=self.source.provider.host.host,username=None,password=None)
        else:
            self.http = http

    def __del__(self):
        if self._conown:
            try:
                self.http.disconnect()
            except Exception as e:
                pass

