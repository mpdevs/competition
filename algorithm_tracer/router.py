# coding: utf-8
# __author__: "John"
import os
from form_handler import *
from chart_handler import *


handler = [
    (ur"/", IndexHandler),
    (ur"/reverse/(\w+)", ReverseHandler),
    (ur"/wrap", WrapHandler),
    (ur"/poem", PoemPageHandler),
    (ur"/demo/result", AlgorithmDemoHandler),
    (ur"/demo/form", AlgorithmDemoFormHandler),
    (ur"/chart", ChartIndexHandler),
    (ur"/chart/fvd", FeatureVectorDistributionHandler),
]

template_path = os.path.join(os.path.dirname(__file__), u"template")
static_path = path.join(path.dirname(__file__), u"static")
