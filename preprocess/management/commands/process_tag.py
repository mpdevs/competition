# -*- coding: utf-8 -*-
__author__ = 'Dragon'


from django.core.management import BaseCommand
from preprocess.process import process_tag


class Command(BaseCommand):
    def handle(self, *args, **options):
        args1 = 'mp_women_clothing' if len(args) == 0 else args[0]
        args2 = 'item' if len(args) == 0 else args[1]
        process_tag(args1, args2)
