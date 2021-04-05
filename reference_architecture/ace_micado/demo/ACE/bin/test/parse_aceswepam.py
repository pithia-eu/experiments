import os, sys
sys.path.insert(0, os.path.abspath('../../'))
from datetime import datetime
from app import CONF
from Lib.database import  models
from Lib.parsers.ace import SWEPAM

def getHour():
    fname = '202012_ace_swepam_1h.txt'
    path = os.path.join(CONF['DATA_PATH'],'samples',fname)
    source = models.SwepamDataFilesSerial(
        fname=fname, ser_id=202012, flg_knd=1,
        datemin=datetime(2020,12,1,0,0,0),
        datemax=datetime(2020,12,31,23,0,0),
        parsed=0
    )
    content = None
    with open(path,'r') as f:
        content = f.read()
    return source, content

def getMinute():
    fname = '20150729_ace_swepam_1m.txt'
    path = os.path.join(CONF['DATA_PATH'],'samples',fname)
    source = models.SwepamDataFilesSerial(
        fname=fname, ser_id=20150729, flg_knd=0,
        datemin=datetime(2015,7,29,0,0,0),
        datemax=datetime(2015,7,29,23,59,0),
        parsed=0
    )
    content = None
    with open(path,'r') as f:
        content = f.read()
    return source, content

def main(argv):
    source, content = getMinute()
    #source, content = getHour()
    parser = SWEPAM(content)
    parser.parse(source)
    print('coco')


if __name__ == '__main__':
    sys.exit(main(sys.argv))