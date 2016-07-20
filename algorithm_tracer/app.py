# coding: utf-8
# __author__: "John"
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define, options
from router import handler, template_path
define(u"port", default=8280, help=u"run on given port", type=int)
define(u"address", default=u"0.0.0.0", help=u"access from remote", type=str)

if __name__ == u"__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=handler, template_path=template_path, debug=True)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    print (u"web server start")
    tornado.ioloop.IOLoop.instance().start()
