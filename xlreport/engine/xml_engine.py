# coding: utf-8

"""
    xlreport.engine.xml_engine
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    Data source engine for XML.
"""

import lxml
from lxml import etree

from xlreport.engine.base import BaseEngine


class XmlEngine(BaseEngine):
    """Xmlエンジン"""

    @staticmethod
    def load(s):
        return etree.fromstring(s)

    @staticmethod
    def dump(node, **kws):
        return etree.tostring(node, **kws)

    @staticmethod
    def normalize_path(path):
        return path

    @staticmethod
    def text(node):
        return node.text

    @staticmethod
    def xpath(node, path):
        return node.xpath(path)

    @staticmethod
    def findall(node, tag):
        return node.findall(tag)

    @staticmethod
    def find(node, tag):
        return node.find(tag)

    @staticmethod
    def set(node, name, value):
        node.set(name, value)

    @staticmethod
    def append(node, subnode):
        node.append(subnode)

    @staticmethod
    def remove(node, subnode):
        node.remove(subnode)

    @staticmethod
    def make_element(tag, parent=None, **kws):
        node = etree.SubElement(parent, tag) if parent is not None else etree.Element(tag)
        for k, v in kws.iteritems():
            child = etree.SubElement(node, k)
            child.text = v
        return node

    @staticmethod
    def clear(node):
        del node

    @staticmethod
    def get_child(node, tag):
        child = node.find(tag)
        return child.text if child is not None else ''
