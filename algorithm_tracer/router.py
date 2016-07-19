# coding: utf-8
import os
from handler import *


handler = [
    (r"/", IndexHandler),
    (r"/reverse/(\w+)", ReverseHandler),
    (r"/wrap", WrapHandler),
    (r"/poem", PoemPageHandler),
    (r"/demo/result", AlgorithmDemoHandler),
    (r"/demo/form", AlgorithmDemoFormHandler),
]

template_path = os.path.join(os.path.dirname(__file__), "template")
