import os
import ciso8601
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
import uuid
import yaml
import psutil
from sqlalchemy.ext.declarative import declarative_base

from Lib.logger import ACELogger as _ACELogger


def join(loader, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


yaml.add_constructor('!join', join)

_F = os.path.abspath(__file__)

def normpath(extpath, basepath = _F):
    if basepath is None:
        return None
    elif os.path.isabs(extpath):
        return extpath
    elif os.path.isfile(basepath):
        return str(os.path.normpath(os.path.join(os.path.dirname(basepath),extpath)))
    else:
        return str(os.path.normpath(os.path.join(basepath,extpath)))


def _fixConf():
    CONF['DATA_PATH'] = normpath(CONF['DATA_PATH'], _F)
    CONF['PLOT_PATH'] = normpath(CONF['PLOT_PATH'], _F)
    CONF['RUN_PATH'] = normpath(CONF['RUN_PATH'], _F)
    CONF['MYSQLDB']['FUNC_PATH'] = normpath(CONF['MYSQLDB']['FUNC_PATH'], _F)
    if CONF['BASE_DATE']:
        if not isinstance(CONF['BASE_DATE'], datetime):
            CONF['BASE_DATE'] = ciso8601.parse_datetime(CONF['BASE_DATE'])
    if CONF['NODATA_ACE_THRESH']:
        CONF['NODATA_ACE_THRESH'] = timedelta(seconds=timeparse(CONF['NODATA_ACE_THRESH']))


_CONF = normpath('./var/aceConf.yaml')
CONF = dict()
with open(_CONF) as f:
    CONF = yaml.load(f, Loader=yaml.Loader)

_fixConf()

os.environ['SQLALCHEMY_DATABASE_URI'] = CONF['MYSQLDB']['SQLALCHEMY_DATABASE_MYSQLURI']

NCPU = psutil.cpu_count()
UUID = uuid.UUID(CONF['UUID'])

from pydantic import Extra


class PydanticConfig:
    arbitrary_types_allowed = True
    extra = Extra.ignore


ACELogger = _ACELogger()

from Lib.database.base import DBBase
Base = declarative_base(cls = DBBase)
from Lib.database.models import *


