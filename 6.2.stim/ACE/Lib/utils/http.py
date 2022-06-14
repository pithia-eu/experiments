import os
import time
import requests
from furl import furl
from bs4 import BeautifulSoup
from io import StringIO
from socket import error as SocketError


class HTTPMod(object):

    @property
    def session(self):
        retries = 3
        while retries>0:
            try:
                if self._session:
                    return self._session
            except Exception:
                pass
            try:
                session = requests.Session()
                if self.username and self.password:
                    session.auth = (self.username, self.password)
                self._session = session
                return self._session
            except Exception as e:
                print(f'Cannot connect to HTTP: {e}')
                if retries>1:
                    print(f'Retrying')
                time.sleep(5)
                retries-=1

        return None

    def read(self,fname, path= None,json = False):
        curl = self.baseurl.copy().set(path=[*path.split('/'),fname])
        resp = None
        try:
            if json:
                resp = self.session.get(curl.url).json()
            else:
                resp = self.session.get(curl.url).content

            return resp
        except Exception as e:
            print(f"An error occurred {e}. Could not read HTTP file: {curl.url}")
            return False

    def list(self, path, pattern=None, callback=None):
        curl = self.baseurl.copy().set(path=path)
        try:
            content = self.session.get(curl.url).text
            soup = BeautifulSoup(content, "lxml")
            return callback(soup,self.baseurl.url,pattern)
        except Exception as e:
            print(f"An error occurred {e}. Could not list HTTP path: {curl.url}")
            return False

    def __init__(self, scheme='http', host = None, username = None, password= None):
        self.baseurl = furl(scheme=scheme,host=host)
        self.username = username
        self.password = password
        self._session = None

    def disconnect(self):
        try:
            if self._session:
                self._session.close()
        except Exception as e:
            pass

    def __del__(self):
        self.disconnect()
