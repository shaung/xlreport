# coding: utf-8

import os
import zipfile

original_zipinfo = zipfile.ZipInfo
original_sep = os.sep

class MyZipInfo(original_zipinfo):
    def __init__(self, *args, **kws):
        os.sep = '/'
        original_zipinfo.__init__(self, *args, **kws)
        os.sep = original_sep

zipfile.ZipInfo = MyZipInfo


def ensure_unicode(s):
    if isinstance(s, unicode):
        return s
    else:
        return s.decode('utf8')


def index2int(index):
    """Translates the "AZ" format column numbers. 

    NOTE: Excel's rowno / colno starts from 1.
    """

    s = 0
    pow = 1
    for char in index[::-1]:
        d = int(char, 36) - 9
        s += pow * d
        pow *= 26

    # excel starts column numeration from 1
    return s
