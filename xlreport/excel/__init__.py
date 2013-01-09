# coding: utf-8

"""
    xlreport.excel
    ~~~~~~~~~~~~~~


"""

import math
import time
import os
import tempfile
import shutil
from lxml import etree
import xlpy

from xlreport.engine import XmlEngine, JsonEngine
from xlreport.util import zipfile
from xlreport.util import ensure_unicode as uni

from template import Template
from image import Image
from filter import create_filter

import logging, traceback
logger = logging.getLogger(__file__)


class BookInfo(object):
    """Manages the names of worksheets in a workbook.
    """

    def __init__(self):
        self.names = {}
        self.cntr = {}

    def register_name(self, idx, sheet_name):
        """Register the *idx* th sheet's name.

        Excel sheet names are limited to 31 chars (ascii).
        So if it's too long we need to cut it,
        and to prevent duplication a unique number needs to be assigned to each sheet.

        Returns the normalized name based on the original *sheet_name*.
        """
        name = sheet_name[:31]
        self.cntr[name] = self.cntr.get(name, 0) + 1
        dup_idx = self.cntr[name]
        if dup_idx > 1:
            # This name has been used
            len_idx = len(str(dup_idx)) + 1
            name = '%s%s' % (name[:31-len_idx], dup_idx)
        self.names[idx] = name
        return name

    def get_sheet_name(self, idx, default='unknown'):
        """Returns the *idx* th sheet's normalized name.
        
        When not registered, returns *default*.
        """
        return self.names.get(idx) or default


def generate_report(src_doc, template_path, dest_path, split=False, engine=XmlEngine):
    """Generate excel file.

    * *src_doc*:        Data source path
    * *template_path*:  Template file path
    * *dest_path*:      Destination file path
    * *split*:          When set to True, sheets will be splitted into seperate workbooks and compressed into a single zip file.
    * *engine*          Data source engine. See `xlreport.engine`

    Yields the worksheet's name each time a new worksheet generated.
    """

    w = xlpy.create_copy(template_path)
    info = BookInfo()
    template = Template.parse(template_path)

    if split:
        tmpdir = tempfile.mkdtemp()
        # TODO: total count is available later
        #total_cnt = w.get_sheet_count()
        #digits = int(math.log10(total_cnt)) + 1
        digits = 4
        fmt = "%%0%sd" % digits

    try:
        for idx, node_sheet in enumerate(template.apply(src_doc, engine=engine)):
            nodesheet = node_sheet['Sheet']
            sheet_name = engine.get_child(nodesheet, 'name')
            sheet_name = info.register_name(idx, sheet_name)
            sheet = generate_sheet(w, nodesheet, sheet_name, engine=engine)
            del nodesheet
            page_setup_default(idx, sheet)

            if split and sheet.is_visible():
                wb = xlpy.Workbook()
                wb.copy_sheet_from_book(w, sheet.index, sheet.name)
                path = os.path.join(tmpdir, u'%s_%s.xls' % (fmt % idx, uni(sheet.name)))
                wb.save(path.encode('utf8'))
                wb = None
 
            sheet.flush_row_data()
            sheet = None

            yield sheet_name
    except:
        logger.error('error occured during excel generation')
        raise
    finally:
        del info
        del template

    for idx, sheet in enumerate(w.get_original_sheets()):
        page_setup_default(idx, sheet)

    w.save(dest_path)
    w = None

    if split:
        # split the sheets and zip all
        fd, tmppath = tempfile.mkstemp(suffix='.zip')
        rslt = zipfile.ZipFile(os.fdopen(fd, 'wb'), 'w', zipfile.ZIP_DEFLATED)
        for r, dirs, files in os.walk(tmpdir):
            for fpath in files:
                if isinstance(fpath, str):
                    fpath = unicode(fpath, 'utf8')
                rslt.write(os.path.join(r, fpath.encode('utf8')), arcname=fpath.encode('cp932'))
        rslt.close()
        shutil.copy2(tmppath, dest_path)


def generate_sheet(workbook, nodesheet, sheet_name, engine=XmlEngine):
    """Make excel worksheet.

    * *workbook*        Workbook to append sheet to
    * *nodesheet*       Data
    * *sheet_name*      Sheet's name
    * *engine*          Data engine to use. See `xlreport.engine`

    """

    w = workbook

    start = time.time()

    is_multiple = engine.get_child(nodesheet, 'multiple') == 'True'
    ref_sheet_id = int(engine.get_child(nodesheet, 'copy_from'))

    if is_multiple:
        sheet = w.copy_sheet(ref_sheet_id, sheet_name)
    else:
        sheet = w.get_combined_sheet(ref_sheet_id)
        sheet.name = sheet_name

    nodeprior = engine.find(nodesheet, 'Priors')
    for node in engine.findall(nodeprior, 'clear_cell'):
        row = int(engine.get_child(node, 'row'))
        col = int(engine.get_child(node, 'col'))
        sheet.set_value(row, col, '')

    insert_rows = []
    for node in engine.findall(nodeprior, 'insert_rows'):
        before = int(engine.get_child(node, 'before'))
        count = int(engine.get_child(node, 'count'))
        ref_row = int(engine.get_child(node, 'copy_from'))
        insert_rows.append((before, count, ref_row))

    # Insert rows from bottom to top, so we won't mess up the sheet
    insert_rows.sort(key=(lambda (b,c,r): b*-1))

    for before, count, ref_row in insert_rows:
        print before, count, ref_row
        if count > 0:
            sheet.insert_row_before(before, count)
        for i in xrange(count):
            #sheet.insert_row_before(before)
            cols = [(0, '')]
            sheet.write_row(before + i, ref_row, *cols)

    nodecells = engine.find(nodesheet, 'Cells')
    if nodecells is not None:
        data = {}
        for nodecell in engine.findall(nodecells, 'Cell'):
            row = int(engine.get_child(nodecell, 'row'))
            col = int(engine.get_child(nodecell, 'col'))
            ref_row, value = write_cell(sheet, nodecell, engine)
            if ref_row == -1:
                continue
            if row not in data:
                data[row] = [ref_row, []]
            data[row][1].append([col, value])

        for row, (ref_row, cols) in data.items():
            sheet.write_row(row, ref_row, *cols)
            #sheet.flush_row_data()

    end = time.time()

    return sheet


def write_cell(sheet, nodecell, engine):
    """Write data to a cell.

    * *sheet*       Worksheet object. See `xlpy.xlwt.worksheet.Worksheet`
    * *nodecell*    Cell data.
    * *engine*      Data source engine. See `xlreport.engine`

    Returns a tuple of (ref_rowno, value).
    """

    row     = int(engine.get_child(nodecell, 'row'))
    col     = int(engine.get_child(nodecell, 'col'))
    ori_row = int(engine.get_child(nodecell, 'ori_row'))
    ori_col = int(engine.get_child(nodecell, 'ori_col'))
    value   = uni(engine.get_child(nodecell, 'value')) or ''
    ref_row = int(engine.get_child(nodecell, 'ref_row') or -1)

    filters = []
    extras = engine.find(nodecell, 'Extras')
    if extras is not None:
        for extra in engine.findall(extras, 'Extra'):
            func = engine.get_child(extra, 'func')
            argn = engine.get_child(extra, 'argn') or 0
            args = [engine.get_child(extra, 'arg%s' % i) or '' for i in range(int(argn))] or []
            args = [x.strip() for x in args]
            filters.append(create_filter(func, args))

    for filter in filters:
        value = filter.apply(sheet, row, col, value, ref_row, ori_row, ori_col)

    if ref_row == -1:
        sheet.set_value(row, col, value)
        value = ''

    return ref_row, value


# FIXME: just an adhoc solution for page setup.
def page_setup_default(idx, sheet):
    sheet.print_scaling = 100
    sheet.vert_page_breaks = []

    if '$' in sheet.name:
        sheet.set_very_hidden()

