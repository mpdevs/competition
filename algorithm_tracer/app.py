# coding: utf-8
# __author__: u"John"
from router import *
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define, options
define(u"port", default=8280, help=u"run on given port", type=int)
define(u"address", default=u"0.0.0.0", help=u"access from remote", type=str)


run = False


def port_adapter(http_server):
    global run
    try:
        http_server.listen(options.port)
        run = True
    except Exception:
        options.port += 1
    return


def main():
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=handler, template_path=template_path, static_path=static_path, debug=True,
                                  autoreload=True)
    http_server = tornado.httpserver.HTTPServer(app)
    while not run:
        port_adapter(http_server)
    print (u"app run on {0}:{1}".format(options.address, options.port))
    tornado.ioloop.IOLoop.instance().start()


if __name__ == u"__main__":
    main()
