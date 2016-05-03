# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.process import process_annual


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('industry_db_name', 'table_from', 'table_to')

    def handle(self, *args, **options):
        industry_db_name = options['industry_db_name']
        table_from = options['table_from']
        table_to = options['table_to']
        process_annual(industry_db_name, table_from, table_to)
