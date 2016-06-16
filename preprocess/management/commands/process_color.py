# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.color import process_color


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('industry_db_name')
        parser.add_argument('table_name')

    def handle(self, *args, **options):
        industry_db_name = options['industry_db_name']
        table_name = options['table_name']
        process_color(industry_db_name, table_name)


