import os, sys
sys.path.insert(0, os.path.abspath('../'))
import argparse
from app import CONF as cfg
from Lib.database.base import DBUtils

from app import ACELogger


def create_parser():
    def dboper(args):
        dbcfg = cfg['MYSQLDB']
        if dbcfg['DB'] == 'server_dias':
            ACELogger.logger.error('Operations on <server_dias> DB are not allowed')
            return False

        if args.create:
            ACELogger.logger.info('Creating DB')
            try:
                DBUtils.create()
            except Exception as e:
                ACELogger.logger.error('Error while creating DB: {}'.format(e))
        elif args.initialize:
            ACELogger.logger.info('Initializing DB')
            try:
                DBUtils.init()
            except Exception as e:
                ACELogger.logger.error('Error while initializing DB: {}'.format(e))
        elif args.reset:
            ACELogger.logger.info('Resetting DB')
            try:
                DBUtils.reset()
            except Exception as e:
                ACELogger.logger.error('Error while resetting DB: {}'.format(e))
        elif args.delete:
            ACELogger.logger.info('Dropping DB')
            try:
                DBUtils.drop()
            except Exception as e:
                ACELogger.logger.error('Error while dropping DB: {}'.format(e))

    _parser = argparse.ArgumentParser(prog='ACEDIAS_oper', description='ACE Model Operations')
    _parser.add_argument('--version', action='version', version='1.0.0')
    subparsers = _parser.add_subparsers(help='sub-command help')

    # ------- DB parser -------
    db_parser = subparsers.add_parser('db', help='DB operations')
    db_parser.set_defaults(func=dboper)
    db_group = db_parser.add_mutually_exclusive_group(required=True)

    db_group.add_argument('-c', '--create', action='store_true', help='Create DB if not exists')
    db_group.add_argument('-i', '--initialize', action='store_true', help='Initialize DB Schema')
    db_group.add_argument('-r', '--reset', action='store_true',help='Reset DB response if DB not empty')
    db_group.add_argument('-d', '--delete', action='store_true', help='Drop DB if exists')

    return _parser


parser = create_parser()


def main(argv):
    ACELogger.logger.info('ACE DIAS OPERATIONS')

    args = parser.parse_args()
    if not bool(args.__dict__):
        raise KeyError('No arguments supplied')
    try:
        args.func(args)
    except Exception as e:
        raise RuntimeError(e)


if __name__ == '__main__':
    sys.exit(main(sys.argv))