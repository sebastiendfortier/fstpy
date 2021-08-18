# -*- coding: utf-8 -*-

def pytest_itemcollected(item):
    node = item.obj
    doc = node.__doc__.strip() if node.__doc__ else ''
    item._nodeid = ' '.join([item._nodeid,doc])
