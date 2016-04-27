# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.process import process_annual


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('industry_db_name')

    def handle(self, *args, **options):
        industry_db_name = options['industry_db_name']
        process_annual(industry_db_name)
