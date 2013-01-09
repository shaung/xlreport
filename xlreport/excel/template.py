# coding: utf-8

"""
    xlreport.excel.template
    ~~~~~~~~~~~~~~~~~~~~~~~

    :$foo$:           query expression.
    :$foo | bar$:     if foo is Empty then use bar instead.
    :#foo#:           start row-based loop.
    :#end#:           end of loop with auto-increasing length.
    :#no#:            auto No. of loop.

    :~FUNC(PARA):     See `filter`
"""


import os
import re
from xlpy import xlrd

from xlreport.engine import *
from xlreport import context

import logging
logger = logging.getLogger(__file__)


class TemplateError(Exception):
    pass


class Path(object):
    """Single query path."""

    MODE_PLAIN, MODE_GROUP = range(2)

    def __init__(self, mode, pathstr):
        self.mode = mode
        self.pathstr = pathstr
        self.fallback_chain = [self.translate_path(x.strip()) for x in pathstr.split('|')]

    def get_value(self, ctx, search=True):
        """データを取得する。
        *search* : キャッシュ上にない場合、新たに取得するかを指定する。
        """

        for chain in self.fallback_chain:
            if len(chain) == 2 and chain[-1] == '.':
                return chain[0]
            path = '/'.join(chain[:-1])
            prop = chain[-1]
            value = ctx.get(path, prop, search)
            if value:
                return value
        return ''

    def is_no_col(self):
        """番号列であるかを取得する。"""
        return self.mode == Path.MODE_GROUP and self.pathstr.lower() == 'no'

    def validate(self):
        """
        """
        # TODO
        return True

    @staticmethod
    def translate_path(pathstr):
        """Normalize *pathstr* .
        Returns tuple(path, last_attribute).
        """

        path_tuple = re.split(r'(?<!@)\.', pathstr)
        if len(path_tuple) == 1:
            return pathstr, '.'
        else:
            path, attr = '/'.join(path_tuple[:-1]), path_tuple[-1]
            return path, attr


class Macro(object):
    """Represents a cell.
    """

    REGEX_VALUE = '(?P<plain>\$.*?\$)|(?P<group>#.*?#)'
    REGEX_EXTRA = '(\w+)\(([^)]*)\)'

    def __init__(self, macrostr):
        # all pathes in the cell, may have different prefix,
        # but should have at most one path which might have multiple values.
        self.path_group = []
        self.value_template = ''
        self.extras = []
        self.match_macro(macrostr)
        self._is_group = len([p for p in self.path_group if p.mode == Path.MODE_GROUP]) > 0
        self._is_group_start = self.is_group_start()
        self._is_const = len(self.path_group) == 0

    def get_max_prefix(self):
        if len(self.path_group) > 0:
            pathes = [chain[0] for chain in self.path_group[0].fallback_chain]
            max_depth = max([p.count('/') for p in pathes])
            return [p for p in pathes if p.count('/') == max_depth][0]
        return ''

    def get_pathes(self):
        """Returns all the pathes in this cell."""

        combine = lambda x, y: x + y
        chains = [p.fallback_chain for p in self.path_group]
        return chains and reduce(combine, chains) or []

    def evaluate(self, values):
        """*values* リストで指定した値で、パスを置換してセルに設定する値を取得する。"""

        if len(values) == 0:
            return self.value_template
        elif len(values) == 1:
            return self.value_template % values[0]
        return self.value_template % tuple(values)

    def get_value(self, ctx, search=True):
        search = search and not self._is_group
        return self.evaluate([path.get_value(ctx, search) for path in self.path_group])

    def __str__(self):
        return self.value_template

    def __unicode__(self):
        return self.value_template

    @classmethod
    def parse(cls, macrostr):
        self = cls(macrostr)
        #if self.is_const():
        #    return None
        return self

    def match_macro(self, value):
        self.value_template = re.sub(self.REGEX_VALUE, self._sub_cb, value)

    def _sub_cb(self, m):
        mode = Path.MODE_GROUP if m.group('plain') is None else Path.MODE_PLAIN
        path = Path(mode, m.group(0)[1:-1])
        self.path_group.append(path)
        return '%s'

    def is_no_col(self):
        if len(self.path_group) == 1:
            return self.path_group[0].is_no_col()
        return False

    def is_const(self):
        return len(self.path_group) == 0

    def is_group(self):
        return any(True for p in self.path_group if p.mode == Path.MODE_GROUP)

    def is_group_start(self):
        groups = [p for p in self.path_group if p.mode == Path.MODE_GROUP]
        if len(groups) == 0:
            return False

        return groups[0].fallback_chain[0][0] not in ('end', 'grow')

    def get_end_type(self):
        if not self._is_group or self._is_group_start:
            return None

        return self.path_group[0].fallback_chain[0]

    @classmethod
    def test(cls, macro_str):
        return re.match(cls.REGEX_VALUE, macro_str) is not None

    def validate(self):
        """All the macros in the same group must be at the same level.
        """
        # TODO
        return True


class Extra(object):
    """Filters in a single cell."""

    def __init__(self, extrastr):
        self.extrastr = extrastr
        self.funcname = ''
        self.macros = []
        self.parse_macro()

    def parse_macro(self):
        regex = r'^(\w+)\((.*)\)|$'
        m = re.match(regex, self.extrastr)
        func, arg_str = m.group(1), m.group(2)
        arg_str = arg_str.strip()
        if len(arg_str) == 0:
            args = []
        else:
            args = re.split(r',', arg_str)
        self.funcname = func
        for arg in args:
            macro = Macro.parse(arg) or arg
            self.macros.append(macro)

    def get_value(self, ctx):
        return [self.funcname] + self.get_args_value(ctx)

    def get_args_value(self, ctx):
        return [macro.get_value(ctx) for macro in self.macros]

    @classmethod
    def parse(cls, extrastr):
        return cls(extrastr)


class SheetMetaInfo(object):
    """シートの情報。"""

    def __init__(self):
        self.sheet_macro = None
        self.macros = []
        self.group_macros = []

    def is_static(self):
        return len(self.group_macros) == 0


class MacroDef(object):
    """ Represents a Cell.
    """

    def __init__(self, r, c, macro, *extras):
        self.row = r
        self.col = c
        self.macro = macro
        self.extras = extras

    def get_value(self, ctx):
        return self.macro.get_value(ctx), [extra.get_value(ctx) for extra in self.extras]

    def get_max_prefix(self):
        return self.macro.get_max_prefix()

    def __repr__(self):
        return '<MacroDef> %s, %s, %s' % (self.row, self.col, '; '.join(['.'.join(x) for x in self.macro.get_pathes()]))


class Level(object):
    """ Each cell has a max depth which we call *level*.
    """
    __map = {}

    def __init__(self):
        self.__map = {}

    def append(self, *cells):
        if len(cells) == 0:
            return
        prefix = cells[0].get_max_prefix()
        depth = prefix.count('/')
        if prefix not in self.__map:
            self.__map[prefix] = [depth, list(cells)]
        else:
            self.__map[prefix][-1] += cells

    def __iter__(self):
        """セルを一つずつyieldする。
        """

        # カラムの順番で優先順位を決める
        keys = sorted(self.__map, key=(lambda x: self.__map.get(x)[1][0].col))
        for prefix in keys:
            depth, cells = self.__map[prefix]
            yield prefix, cells
        return


class GroupMacroDef(object):
    """ Represents a row or a col block.
    """

    def __init__(self, rstart, rend, mode, macros):
        self.rstart = rstart
        self.rend = rend
        self.rowno_col = None
        self.mode = mode
        self.cells = {}
        self.level = Level()
        self.calc_level(macros)

    @classmethod
    def parse(cls, groups):
        # TODO: we really should allow multi groups start from the same row. 
        #       however it's difficult to decide to which group the columns belong,
        #       due to the lackness of information.
        #       currently we just assume there is only one group per row.
        # TODO: check if all the cols belongs to the same path space.
        return [groups]

    def get_end_col(self):
        if self.rowno_col is None:
            return min(self.cells)
        else:
            return min(self.rowno_col, min(self.cells))

    def calc_level(self, macros):
        self.cells = {}

        get_chain = lambda m: m.path_group[0].fallback_chain[0][0].split('/')
        for col, macro, extras in macros:
            if macro.is_no_col():
                self.rowno_col = col
                continue
            self.cells[col] = cell = MacroDef(0, col, macro, *extras)
            self.level.append(cell)

    def iter(self, levels, ctx, engine=XmlEngine):
        """ Generator to yield all the levels one row at a time, and set the xml context.
        Also it yields information of the previous level.
        """
        
        if len(levels) == 0:
            yield []
            return

        prefix, cells = levels[0]
        nodes = list(ctx.query(prefix))
        if not isinstance(nodes, (list, tuple, set)):
            nodes = [nodes]
        if len(nodes) == 0:
            ctx.clear_children(prefix)
            yield [(c.col, c.get_value(ctx)) for c in cells]
            return
        for node in nodes:
            ctx.cache(prefix, node)
            ctx.clear_children(prefix)
            data = [(c.col, c.get_value(ctx)) for c in cells]
            dummy = [(col, ('', [])) for col, value in data]
            for i, tmp in enumerate(self.iter(levels[1:], ctx, engine)):
                if i == 0:
                    yield data + tmp
                else:
                    yield dummy + tmp
        return

    def iter_data(self, ctx, engine=XmlEngine):
        levels = [x for x in self.level]
        for row in self.iter(levels, ctx, engine):
            yield row

    def get_row_data(self, ctx):
        return [(col, cell.macro.get_value(ctx)) for col, cell in self.cells.iteritems()]


class Template(object):
    """The template."""

    def __init__(self):
        self.meta = {}
        self.tmp_groups = {}
        self.ctx = context.create()

    @classmethod
    def parse(cls, template_path):
        """Read and parse the template file specified by *template_path*."""

        self = cls()
        w = xlrd.open_workbook(template_path, formatting_info=True)
        for idx, sht in enumerate(w.sheets()):
            self.tmp_groups = {}
            sheet_macro = Macro(sht.name)
            self.meta[idx] = meta = SheetMetaInfo()
            meta.sheet_macro = sheet_macro

            for rx in xrange(sht.nrows):
                cols = ((cx, sht.cell_value(rx, cx)) for cx in xrange(sht.ncols))
                cols = [col for col in cols if len(unicode(col[-1])) > 0]
                if len(cols) == 0:
                    continue 

                macros, groups = self.parse_column(cols)
                meta.macros += [MacroDef(rx, cx, m, *es) for cx, m, es in macros]

                group_starts = [(i, m, es) for (i, m, es) in groups if m._is_group_start]
                if len(group_starts) > 0:
                    group_macros = GroupMacroDef.parse(group_starts)
                    head = group_starts[0][0]
                    for g in group_macros:
                        self.register_group(rx, head, g)

                group_ends = ((i, m) for (i, m, es) in groups if not m._is_group_start)
                for g in group_ends:
                    self.process_group(idx, rx, *g)

        w = None
        return self

    def parse_column(self, cols):
        raws = ((i, unicode(col).split('~')) for i, col in cols)
        ms_pre = ((i, Macro.parse(raw[0]), [Extra.parse(extrastr) for extrastr in raw[1:]]) for i, raw in raws)
        ms = [(i, m, es) for i, m, es in ms_pre if not m._is_const or len(es) > 0]
        if len(ms) == 0:
            return [], []
        macros = [(i, m, es) for i, m, es in ms if not m._is_group]
        groups = [(i, m, es) for i, m, es in ms if m._is_group]
        return macros, groups

    def register_group(self, row, col, group):
        if not self.tmp_groups.get(col):
            self.tmp_groups[col] = []
        self.tmp_groups[col].append((row, group))

    def process_group(self, idx, row, col, group_end):
        rstart, group = self.tmp_groups[col].pop()
        self.meta[idx].group_macros += [
            GroupMacroDef(rstart, row, group_end.get_end_type(), group)]

    def get_direct_parameters(self):
        rslt = set([])
        for idx, meta in self.meta.iteritems():
            for macrodef in meta.macros:
                for chain in macrodef.macro.get_pathes():
                    if len(chain) > 1 and chain[0] == 'g':
                        rslt.add(chain[1])
        return rslt

    def _make_sheet(self, idx, meta, is_multiple=False, engine=XmlEngine):
        attrib = {
            'name'      : meta.sheet_macro.get_value(self.ctx, True),
            'copy_from' : str(idx),
            'multiple'  : str(is_multiple)
        }
        nodesheet = engine.make_element('Sheet', **attrib)

        priors = engine.make_element("Priors")
        cells = engine.make_element("Cells")

        offset = 0
        offset_table = {}
        def get_offset(row):
            for k in sorted(offset_table.keys(), reverse=True):
                if row >= k:
                    return offset_table[k]
            return 0

        pending_cells = []

        for macrodef in meta.macros:
            # TODO: offset
            attrib = {
                'row'       : str(macrodef.row), #str(macrodef.row + get_offset(macrodef.row)),
                'col'       : str(macrodef.col),
                'ori_row'   : str(macrodef.row),
                'ori_col'   : str(macrodef.col),
                'value'     : macrodef.macro.get_value(self.ctx)
            }
            nodecell = engine.make_element('Cell', **attrib)
            if len(macrodef.extras) > 0:
                node_extras = engine.make_element('Extras')
                for extra in macrodef.extras:
                    attrib = {'func': extra.funcname, 'argn': str(len(extra.macros))}
                    for i, arg in enumerate(extra.get_args_value(self.ctx)):
                        attrib['arg%s' % i] = arg
                    node_extra = engine.make_element('Extra', **attrib)
                    engine.append_as_list(node_extras, node_extra)
                engine.append(nodecell, node_extras)
            pending_cells.append(nodecell)
            attrib = {
                'row'       : str(macrodef.row),
                'col'       : str(macrodef.col),
            }
            node_clearcell = engine.make_element('clear_cell', **attrib)
            engine.append_as_list(priors, node_clearcell)

        for gm in meta.group_macros:
            available_lines = gm.rend - gm.rstart
            for col in gm.cells:
                attrib = {
                    'row': str(gm.rstart),
                    'col': str(col),
                }
                node_clearcell = engine.make_element('clear_cell', **attrib)
                engine.append_as_list(priors, node_clearcell)
            if gm.rowno_col is not None:
                attrib = {
                    'row': str(gm.rstart),
                    'col': str(gm.rowno_col),
                }
                node_clearcell = engine.make_element('clear_cell', **attrib)
                engine.append_as_list(priors, node_clearcell)
            attrib = {
                'row': str(gm.rend),
                'col': str(gm.get_end_col()),
            }
            node_clearcell = engine.make_element('clear_cell', **attrib)
            engine.append_as_list(priors, node_clearcell)

            for i, data in enumerate(gm.iter_data(self.ctx, engine)):
                if not any(((col, (value, extras))
                            for (col, (value, extras)) in data
                            if len(value) > 0 or len(extras) > 0)):
                    continue

                available_lines -= 1
                if len(data) > 0 and gm.rowno_col is not None:
                    attrib = {
                        'row'       : str(gm.rstart + offset + i),
                        'ref_row'   : str(gm.rstart + min(i, 1)),
                        'col'       : str(gm.rowno_col),
                        'ori_row'   : str(gm.rstart + i),
                        'ori_col'   : str(gm.rowno_col),
                        'value'     : str(i + 1)
                    }
                    nodecell = engine.make_element('Cell', **attrib)
                    engine.append_as_list(cells, nodecell)

                for col, (value, extras) in data:
                    attrib = {
                        'row'       : str(gm.rstart + offset + i),
                        'ref_row'   : str(gm.rstart + min(i, 1)),
                        'col'       : str(col),
                        'ori_row'   : str(gm.rstart + i),
                        'ori_col'   : str(col),
                        'value'     : value
                    }
                    nodecell = engine.make_element('Cell', **attrib)
                    node_extras = engine.make_element('Extras')
                    for extra in extras:
                        funcname = extra[0]
                        args = extra[1:]
                        attrib = {'func': funcname, 'argn': str(len(args))}
                        for idx, arg in enumerate(args):
                            attrib['arg%s' % idx] = arg
                        node_extra = engine.make_element('Extra', **attrib)
                        engine.append_as_list(node_extras, node_extra)
                    engine.append(nodecell, node_extras)
                    engine.append_as_list(cells, nodecell)

                #self.ctx.clear_cache()

            if available_lines < 0:
                extra_line_needed = -1 - available_lines
                attrib = {
                    'before': str(gm.rend),
                    'count': str(extra_line_needed),
                    'copy_from': str(gm.rstart + 2)
                }
                nodemeta = engine.make_element('insert_rows', **attrib)
                engine.append_as_list(priors, nodemeta)
                offset -= 1 + available_lines
                offset_table[gm.rend] = offset

        for cell in pending_cells:
            node_ori_row = engine.find(cell, 'ori_row')
            ori_row = int(engine.text(node_ori_row)) if node_ori_row is not None else 0
            engine.set(cell, 'row', str(ori_row + get_offset(ori_row)))
            engine.append_as_list(cells, cell)
        pending_cells = []

        engine.append(nodesheet, priors)
        engine.append(nodesheet, cells)
        return nodesheet

    def apply(self, doc, engine=XmlEngine):
        """Generate information with data source specified by *doc*.

        * *doc* :       Data source path.
        * *engine* :    Data source engine.
        """

        self.ctx.engine = JsonDBEngine.load(doc)
        self.ctx.root = -1
        for idx, meta in self.meta.iteritems():
            print 'generating data for sheet %s' % idx
            if len(meta.sheet_macro.path_group) == 0:
                nodesheet = self._make_sheet(idx, meta, engine=JsonEngine)
                yield nodesheet
            else:
                # Oh. sheet name contains xpath
                path = meta.sheet_macro.path_group[0].fallback_chain[0]
                pathstr = '/'.join(path[:-1])
                prop = path[-1]
                print 'pathstr=', pathstr
                for i, node in enumerate(self.ctx.engine.xpath(-1, pathstr)):
                    print 'sheet obj %s' % i
                    # The trick: cache the current path,
                    #            so no need to modify the xpath prefix
                    self.ctx.cache(pathstr, node)
                    nodesheet = self._make_sheet(idx, meta, is_multiple=True, engine=JsonEngine)
                    self.ctx.clear_children(pathstr)
                    del node
                    yield nodesheet

        self.ctx.clear_cache()
        self.ctx.root = None
        self.ctx.engine.close()
        del self.ctx.engine
        return
