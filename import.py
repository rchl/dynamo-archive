#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
import datetime


BACKUP_PATH = 'dbBackups'
TABLES = [
    'configuration',
    'controller',
    'file',
    'module',
    'page',
    'redirect',
    'section',
    'spec',
    'template',
    'user',
]
TABLE_PREFIX = 'cmsapi_'
AWS_ENV = {
    'AWS_ACCESS_KEY_ID': 'dbkey',
    'AWS_SECRET_ACCESS_KEY': 'dbsecret',
}


def restore_handler(args):
    backup_path = os.path.join(BACKUP_PATH, args.backup_name)
    if not os.path.isdir(backup_path):
        print 'Specified backup path \'%s\' doesn\'t exist' % backup_path
        return -1

    AWS_ENV['AWS_DYNAMODB_ENDPOINT'] = args.endpoint or 'http://localhost:8000'
    for key, value in AWS_ENV.iteritems():
        os.environ[key] = value

    for table in TABLES:
        filepath = os.path.join(backup_path, '%s.json' % table)
        try:
            data = open(filepath, 'r').read()
        except IOError as ex:
            print 'Cannot open backup file \'%s\'' % filepath
            return

        table_name = '%s%s' % (args.table_prefix, table)
        print 'Restoring backup for table \'%s\'...' % table_name
        process = subprocess.Popen([
            'bin/dynamo-restore.js',
            '--table', table_name,
            '--rate', '1000',
        ], stdin=subprocess.PIPE)
        process.communicate(data)


def backup_handler(args):
    now = datetime.datetime.now().isoformat()
    backup_path = os.path.join(BACKUP_PATH, now)
    os.makedirs(backup_path)
    print 'Created backup directory \'%s\'' % backup_path
    for table in TABLES:
        table_name = '%s%s' % (args.table_prefix, table)
        print 'Backing up table \'%s\'...' % table_name
        output = subprocess.check_output([
            'bin/dynamo-archive.js',
            '--table', table_name
        ])
        filepath = os.path.join(backup_path, '%s.json' % table)
        print 'Saving backup to file \'%s\'' % filepath
        with open(filepath, 'w') as f:
            f.write(output)


def list_handler(args):
    print 'Existing backups:'
    for name in sorted(os.listdir(BACKUP_PATH)):
        path = os.path.join(BACKUP_PATH, name)
        if os.path.isdir(path):
            print '  %s' % name


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    subp = subparsers.add_parser('restore')
    subp.add_argument(
        '--table-prefix', default=TABLE_PREFIX,
        help='The prefix to use for created table names.')
    subp.add_argument('backup_name')
    subp.add_argument('--endpoint')
    subp.set_defaults(func=restore_handler)
    subp = subparsers.add_parser('backup')
    subp.add_argument(
        '--table-prefix', default=TABLE_PREFIX,
        help='The prefix to use for created table names.')
    subp.set_defaults(func=backup_handler)
    subp = subparsers.add_parser('list')
    subp.set_defaults(func=list_handler)
    args = parser.parse_args()
    sys.exit(args.func(args))
