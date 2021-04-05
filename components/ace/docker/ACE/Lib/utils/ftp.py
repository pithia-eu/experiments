import os
import time
from io import StringIO
from socket import error as SocketError
import errno
import ftplib

class FTPMod(object):

    @property
    def conn(self):
        retries = 3
        while retries>0:
            try:
                if self._conn and self._conn.voidcmd('NOOP'):
                    return self._conn
            except Exception:
                pass
            try:
                ftpSession = ftplib.FTP()
                #ftpSession.debugging = 2
                ftpSession.encoding = 'UTF-8'
                ftpSession.connect(self.ftpaddr, int(self.port), timeout=100)
                ftpSession.login(self.username, self.password)
                ftpSession.set_pasv(True)
                self._conn = ftpSession
                return self._conn
            except Exception as e:
                print(f'Cannot connect to FTP: {e}')
                if retries>1:
                    print(f'Retrying')
                time.sleep(5)
                retries-=1

        return None

    def dir_exists(self, dir):
        resp = ''
        try:
            resp = self.conn.sendcmd(f'MLST {dir}')
        except Exception as e:
            pass
        finally:
            if 'type=dir;' in resp:
                return True

        return False

    def make_dirs(self, dir):
        dirlist = dir.split('/')
        checkpaths = ['/'.join(dirlist[0:i]) for i in range(2, len(dirlist) + 1)]
        for path in checkpaths:
            resp = ''
            try:
                resp = self.conn.sendcmd(f'MLST {path}')
            except Exception as e:
                pass
            finally:
                if 'type=dir;' not in resp:
                    self.conn.mkd(path)

    def upload(self, src = None, path = None, fname = None, usetemp = False):
        try:
            tmpfname = None
            if usetemp:
                farr = os.path.splitext(fname)
                ext = farr[1] if len(farr) == 2 else None
                tmpfname = fname.replace(ext, '.TMP') if ext else fname + '.TMP'

            status = None
            if path:
                if self.dir_exists(path) is False:
                    self.make_dirs(path)
                self.conn.cwd(path)
            with open(src, 'rb') as binaryFile:
                status = self.conn.storbinary('STOR {0}'.format(tmpfname if tmpfname else fname), binaryFile)

            if usetemp:
                status = self.conn.rename(tmpfname, fname)
            if status and status[0:3] in ('250','226'):
                return True
            else:
                print("An error occurred on FTP server after uploading file {0}".format(path))
                return False
        except Exception as e:
            print("An error occurred {}. File {} could not be uploaded to FTP successfully".format(e, path))
            return False

    def read(self,fname, basepath= '.'):
        f = None
        try:
            self.conn.cwd(basepath)
            f = StringIO()
            def writer(line):
                if not line.endswith('\n'):
                    line = line + '\n'
                f.write(line)
            self.conn.retrlines(f'RETR {fname}', writer)
            f.seek(0)
            content = f.getvalue()
            return content
        except Exception as e:
            print(f"An error occurred {e}. Could not read FTP file: {fname}")
            return False
        finally:
            try:
                if f:
                    f.close()
            except:
                pass

    def list(self, path, pattern=None):
        try:
            self.conn.cwd(path)
            results = self.conn.nlst(pattern)
            return results
        except Exception as e:
            print(f"An error occurred {e}. Could not list FTP directory: {path}")
            return False

    def __init__(self, addr = None, username = 'anonymous', password= 'anonymous', port = 21):
        self.ftpaddr = addr
        self.username = username
        self.password = password
        self.port = port
        self._conn = None

    def disconnect(self):
        try:
            if self._conn:
                self._conn.quit()
        except Exception as e:
            pass

    def __del__(self):
        self.disconnect()
