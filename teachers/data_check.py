# coding: utf-8
from __future__ import unicode_literals
from teacher_john import TeachTaggingClass


ttc = TeachTaggingClass(industry="mp_women_clothing")

df = ttc.data
sample = df[(df.source_item == '16767763233') & (df.target_item == '40481167478')]
print sample
