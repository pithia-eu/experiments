import os,sys
sys.path.insert(0, os.path.abspath('../'))
from datetime import datetime
import signal
from Lib.ace import Manager
from app import CONF as cfg

from app import ACELogger

class DaemonKiller(object):
    CLOSESIGON = False

    def onExit(self,signum, frame):
        ACELogger.logger.warning('EXITING')
        self.CLOSESIGON = True
        self._exit()

    def __init__(self, exit_func = None):
        self._exit = exit_func
        signal.signal(signal.SIGINT, self.onExit)
        signal.signal(signal.SIGTERM, self.onExit)


def setRun():
    with open(cfg['RUN_PATH'],'w') as f:
        f.write(datetime.utcnow().isoformat() + '\n')

def unsetRun():
    if os.path.exists(cfg['RUN_PATH']):
        try:
            os.unlink(cfg['RUN_PATH'])
        except:
            pass

def stop_all():
    unsetRun()
    exit(0)

def main(argv):
    ACELogger.logger.info('Initializing ACE Application')
    exitWatcher = DaemonKiller(exit_func=stop_all)
    setRun()
    manager = Manager()
    manager.manage()
    unsetRun()


if __name__ == '__main__':
    sys.exit(main(sys.argv))