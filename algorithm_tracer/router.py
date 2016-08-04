# coding: utf-8
# __author__: "John"
from form_handler import *
from chart_handler import *
from tag_handler import *


handler = [
    (ur"/", IndexHandler),
    (ur"/reverse/(\w+)", ReverseHandler),
    (ur"/wrap", WrapHandler),
    (ur"/demo/result", AlgorithmDemoHandler),
    (ur"/demo/form", AlgorithmDemoFormHandler),
    (ur"/chart", ChartIndexHandler),
    (ur"/chart/fvd", FeatureVectorDistributionHandler),
    (ur"/tag/form", TagCheckFormHandler),
    (ur"/tag/result", TagCheckHandler),
    (ur"/retag/form", RetagFormHandler),
]

template_path = path.join(path.dirname(__file__), u"template")
static_path = path.join(path.dirname(__file__), u"static")
