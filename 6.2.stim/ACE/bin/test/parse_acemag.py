import os, sys
sys.path.insert(0, os.path.abspath('../../'))
from datetime import datetime
from app import CONF
from Lib.database import  models
from Lib.parsers.ace import MAG

def getHour():
    fname = '202101_ace_mag_1h.txt'
    path = os.path.join(CONF['DATA_PATH'],'samples',fname)
    source = models.MagDataFilesSerial(
        fname=fname, ser_id=202101, flg_knd=1,
        datemin=datetime(2021,1,1,0,0,0),
        datemax=datetime(2021,1,31,23,0,0),
        parsed=0
    )
    content = None
    with open(path,'r') as f:
        content = f.read()
    return source, content

def getMinute():
    fname = '20150730_ace_mag_1m.txt'
    path = os.path.join(CONF['DATA_PATH'],'samples',fname)
    source = models.MagDataFilesSerial(
        fname=fname, ser_id=20150730, flg_knd=0,
        datemin=datetime(2015,7,30,0,0,0),
        datemax=datetime(2015,7,30,23,59,0),
        parsed=0
    )
    content = None
    with open(path,'r') as f:
        content = f.read()
    return source, content

def main(argv):
    #source, content = getMinute()
    source, content = getHour()
    parser = MAG(content)
    parser.parse(source)
    print('coco')


if __name__ == '__main__':
    sys.exit(main(sys.argv))