" Charon: Web application root. "

import logging

import tornado
import tornado.web
import couchdb

from charon import settings
from charon import constants
from charon import utils
from charon import uimodules
from charon.requesthandler import RequestHandler

from charon.login import *
from charon.version import *
from charon.project import *
from charon.sample import *
from charon.libprep import *
from charon.seqrun import *
from charon.api import *


if settings.get('LOGGING_DEBUG'):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

settings['DB_SERVER_VERSION'] = couchdb.Server(settings['DB_SERVER']).version()


class Home(RequestHandler):
    "Home page: Form to login or link to create new account. Links to pages."

    def get(self):
        try:
            utils.check_settings()
        except (KeyError, ValueError), msg:
            self.render('settings_error.html', message=str(msg))
        else:
            self.render('home.html', next=self.get_argument('next', ''))


class ApiHome(ApiRequestHandler):
    "API root: Links to API entry points."

    def get(self):
        """Return links to API entry points.
        Success: HTTP 200."""
        data = dict(links=dict(
                self=dict(href=self.reverse_url('api_home'),
                          title='API root'),
                version=dict(href=self.reverse_url('api_version'),
                             title='software versions'),
                projects=dict(href=self.reverse_url('api_projects'),
                              title='all projects'),
                ))
        self.write(data)


class ApiDoc(RequestHandler):
    "API documentation generated by introspection."

    @tornado.web.authenticated
    def get(self):
        "Display only URLs beginning with '/api/'."
        hosts = []
        for handler in self.application.handlers:
            urlspecs = []
            hosts.append((handler[0].pattern.rstrip('$'), urlspecs))
            for urlspec in handler[1]:
                if not urlspec.regex.pattern.startswith('/api/'): continue
                methods = []
                for name in ('get', 'post', 'put', 'delete'):
                    method = getattr(urlspec.handler_class, name)
                    if method.__doc__ is None: continue
                    methods.append((name, self.process_text(method.__doc__)))
                urlspecs.append(dict(pattern=urlspec.regex.pattern.rstrip('$'),
                                     text=self.process_text(urlspec.handler_class.__doc__),
                                     methods=methods))
        self.render('apidoc.html', hosts=hosts)

    def process_text(self, text):
        if not text: return '-'
        lines = text.split('\n')
        if len(lines) >= 2:
            prefix = len(lines[1]) - len(lines[1].strip())
            for pos, line in enumerate(lines[1:]):
                try:
                    lines[pos+1] = line[prefix:]
                except IndexError:
                    pass
        return '\n'.join(lines)


class Dummy(RequestHandler):

    @tornado.web.authenticated
    def get(self, *args, **kwargs):
        pass


URL = tornado.web.url

handlers = \
    [URL(r'/', Home, name='home'),
     URL(r'/project', ProjectCreate, name='project_create'),
     URL(r'/project/([^/]+)', Project, name='project'),
     URL(r'/project/([^/]+)/edit', ProjectEdit, name='project_edit'),
     URL(r'/projects', Projects, name='projects'),
     URL(r'/sample/([^/]+)', SampleCreate, name='sample_create'),
     URL(r'/sample/([^/]+)/([^/]+)', Sample, name='sample'),
     URL(r'/sample/([^/]+)/([^/]+)/edit', SampleEdit, name='sample_edit'),
     URL(r'/libprep/([^/]+)/([^/]+)', LibprepCreate, name='libprep_create'),
     URL(r'/libprep/([^/]+)/([^/]+)/([^/]+)', Libprep, name='libprep'),
     URL(r'/libprep/([^/]+)/([^/]+)/([^/]+)/edit',
         LibprepEdit, name='libprep_edit'),
     URL(r'/seqrun/([^/]+)/([^/]+)/([^/]+)',
         SeqrunCreate, name='seqrun_create'),
     URL(constants.LOGIN_URL, Login, name='login'),
     URL(r'/logout', Logout, name='logout'),
     URL(r'/version', Version, name='version'),
     URL(r'/apidoc', ApiDoc, name='apidoc'),
     URL(r'/api/v1', ApiHome, name='api_home'),
     URL(r'/api/v1/project', ApiProjectCreate, name='api_project_create'),
     URL(r'/api/v1/project/(?P<projectid>[^/]+)',
         ApiProject, name='api_project'),
     URL(r'/api/v1/projects', ApiProjects, name='api_projects'),
     URL(r'/api/v1/sample/(?P<projectid>[^/]+)',
         ApiSampleCreate, name='api_sample_create'),
     URL(r'/api/v1/sample/(?P<projectid>[^/]+)/(?P<sampleid>[^/]+)',
         ApiSample, name='api_sample'),
     URL(r'/api/v1/samples/(?P<projectid>[^/]+)',
         ApiSamples, name='api_samples'),
     URL(r'/api/v1/libprep/(?P<projectid>[^/]+)/(?P<sampleid>[^/]+)',
         ApiLibprepCreate, name='api_libprep_create'),
     URL(r'/api/v1/libprep/(?P<projectid>[^/]+)/(?P<sampleid>[^/]+)/(?P<libprepid>[^/]+)',
         ApiLibprep, name='api_libprep'),
     URL(r'/api/v1/libpreps/(?P<projectid>[^/]+)/(?P<sampleid>[^/]+)',
         ApiLibpreps, name='api_libpreps'),
     URL(r'/api/v1/seqrun/(?P<projectid>[^/]+)/(?P<sampleid>[^/]+)/(?P<libprepid>[^/]+)',
         ApiSeqrunCreate, name='api_seqrun_create'),
     URL(r'/api/v1/seqrun/(?P<projectid>[^/]+)/(?P<sampleid>[^/]+)/(?P<libprepid>[^/]+)/(?P<seqrunid>[^/]+)',
         ApiSeqrun, name='api_seqrun'),
     URL(r'/api/v1/version', ApiVersion, name='api_version'),
     URL(r'/api/v1/doc/([a-f0-9]{32})', ApiDocument, name='api_doc'),
     URL(r'/api/v1/logs/([a-f0-9]{32})', ApiLogs, name='api_logs'),
     ]

application = tornado.web.Application(
    handlers=handlers,
    debug=settings.get('TORNADO_DEBUG', False),
    cookie_secret=settings['COOKIE_SECRET'],
    ui_modules=uimodules,
    template_path=constants.TEMPLATE_PATH,
    static_path=constants.STATIC_PATH,
    static_url_prefix=constants.STATIC_URL,
    login_url=constants.LOGIN_URL)


if __name__ == "__main__":
    import tornado.ioloop
    application.listen(settings['URL_PORT'])
    logging.info("Charon web server on port %s", settings['URL_PORT'])
    tornado.ioloop.IOLoop.instance().start()
