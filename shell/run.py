#!/usr/bin/env python3
from importlib import import_module
from os import listdir
from os.path import dirname, join, abspath
from sys import argv

DIR = abspath(join(dirname(__file__), 'cron'))
files = [i[0:-3] for i in listdir(DIR) if i.endswith('py') and i != 'base.py']
for _file in files:
    import_module('cron.' + _file)
    CLASS = ''.join([i.capitalize() for i in _file.split('_')])


def command_desc():
    NAMESPACE = __import__('cron')
    DIR = abspath(join(dirname(__file__), 'cron'))
    files = [i[0:-3]
             for i in listdir(DIR) if i.endswith('py') and i != 'base.py']
    desc = """
usage: ./run.py <class> [param1] [param2]>
example: python3 run.py calc_user_register_rate
    or python3 run.py calc_user_register_rate 2020-01-01

options:
    """
    print(desc)
    for _file in files:
        import_module('cron.' + _file)
        CLASS = ''.join([i.capitalize() for i in _file.split('_')])
        anno = getattr(getattr(NAMESPACE, _file), CLASS).__doc__
        print('     {}    {}'.format(_file.ljust(20), anno))


def main():
    if len(argv) == 1:
        command_desc()
        return
    _filename = argv[1]
    params = argv[2:]
    NAMESPACE = __import__('cron')
    CLASS = ''.join([i.capitalize() for i in _filename.split('_')])
    getattr(getattr(NAMESPACE, _filename), CLASS)()._run(params)


if __name__ == '__main__':
    main()
