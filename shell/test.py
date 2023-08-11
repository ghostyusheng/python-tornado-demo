#!/usr/bin/env python3
import asyncio
from importlib import import_module
from sys import argv
from os.path import dirname, join, abspath
from os import listdir


DIR = abspath(join(dirname(__file__), 'test'))
files = [i[0:-3] for i in listdir(DIR) if i.endswith('py') and i not in [
    'base.py', '__init__.py', 'mock_context_var.py']]
for _file in files:
    import_module('test.' + _file)
    CLASS = ''.join([i.capitalize() for i in _file.split('_')])


def command_desc():
    NAMESPACE = __import__('test')
    DIR = abspath(join(dirname(__file__), 'test'))
    files = [i[0:-3] for i in listdir(DIR) if i.endswith('py') and i not in [
        'base.py', '__init__.py', 'mock_context_var.py']]
    desc = """
usage: ./test.py <class> [param1] [param2]>
example: python3 test.py index_test
    or python3 test.py index_test 42

options:
    """
    print(desc)
    for _file in files:
        import_module('test.' + _file)
        CLASS = ''.join([i.capitalize() for i in _file.split('_')])
        anno = getattr(getattr(NAMESPACE, _file), CLASS).__doc__
        print('     {}    {}'.format(_file.ljust(20), anno))


def main():
    if len(argv) == 1:
        command_desc()
        return
    _filename = argv[1]
    params = argv[2:]
    NAMESPACE = __import__('test')
    try:
        CLASS = ''.join([i.capitalize() for i in _filename.split('_')])
        asyncio.get_event_loop().run_until_complete(
            getattr(getattr(NAMESPACE, _filename), CLASS)()._run(params))
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
