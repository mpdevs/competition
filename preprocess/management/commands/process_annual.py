# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.process import process_annual


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('industry_db_name')
        parser.add_argument('table_from')
        parser.add_argument('table_to')
        parser.add_argument('one_shop',nargs='?',default='')

    def handle(self, *args, **options):
        industry_db_name = options['industry_db_name']
        table_from = options['table_from']
        table_to = options['table_to']
        print "行业:",industry_db_name
        print "数据从{}读入并写入{}".format(table_from, table_to)
        one_shop = options['one_shop']
        process_annual(industry_db_name, table_from, table_to, one_shop)
