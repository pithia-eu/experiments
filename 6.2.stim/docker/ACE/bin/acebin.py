import os,sys
sys.path.insert(0, os.path.abspath('../'))
from Lib.ace import Manager
from app import CONF as cfg

from app import ACELogger

def main(argv):
    ACELogger.logger.info('Initializing ACE Application')
    manager = Manager()
    manager.manage()


if __name__ == '__main__':
    sys.exit(main(sys.argv))