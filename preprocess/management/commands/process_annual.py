# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.process import process_annual


class Command(BaseCommand):
    def handle(self, *args, **options):
        args1 = 'mp_women_clothing' if len(args) == 0 else args[0]
        process_annual(args1)
