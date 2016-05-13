# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.nonstd_process import process_annual


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('industry')
        parser.add_argument('to_which_db')
        parser.add_argument('table_from')
        parser.add_argument('table_to')
        
        parser.add_argument('one_shop')
        #parser.add_argument('one_shop',nargs='?',default='')

    def handle(self, *args, **options):
        industry = options['industry']
        db_name = options['to_which_db']
        table_from = options['table_from']
        table_to = options['table_to']
        print "行业:",industry
        print "数据从{}.{}读入并写入{}.{}".format(industry, table_from, db_name, table_to)
        one_shop = options['one_shop']
        process_annual(industry, db_name, table_from, table_to, one_shop)