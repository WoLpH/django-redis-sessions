import math
import time
import datetime

from django.core.management.base import BaseCommand
from django.contrib.sessions.models import Session
from redis_sessions import utils
from redis_sessions.session import SessionStore

CHUNK_SIZE = 100000


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        server = SessionStore().server

        self.sessions = Session.objects.all()
        self.start_progressbar()

        sessions = utils.queryset_iterator(self.sessions, CHUNK_SIZE)
        now = datetime.datetime.now()
        pipe = None
        for i, session in enumerate(sessions):
            if i % CHUNK_SIZE == 0:
                if pipe:
                    pipe.execute()
                pipe = server.pipeline(session.session_key)

            # convert the expire date to a ttl in seconds
            delta = session.expire_date - now
            ttl = (delta.days * 24 * 60 * 60) + delta.seconds

            # set the key, value and ttl
            pipe.set(session.session_key, session.session_data)
            pipe.expire(session.session_key, ttl)
            self.update_progressbar(i)

        self.end_progressbar()

    def start_progressbar(self):
        self.total = self.sessions.count()
        self.log = int(math.ceil(math.log(self.total) / math.log(10)))
        self.progressbar = None
        try:
            import progressbar
            self.progressbar = progressbar.ProgressBar(
                widgets=[
                    progressbar.Percentage(),
                    ' :: ',
                    progressbar.Counter(), '/%d' % self.total,
                    ' :: ',
                    progressbar.ETA(),
                    ' :: ',
                    progressbar.Bar(),
                ],
                maxval=self.total,
                poll=0.1,
            )

        except ImportError:
            print 'Using the `progressbar` module is recommended to get a',
            print 'pretty live progressbar.'

        print
        print 'Going to process %d items' % self.total

        self.progressbar.start()
        self.start = time.time()

    def update_progressbar(self, value):
        if self.progressbar:
            self.progressbar.update(value)
        else:
            print '%0*d/%0*d' % (self.log, value, self.log, self.total)

    def end_progressbar(self):
        self.progressbar = None
        delta = time.time() - self.start
        print ('Processed %d sessions in %.3f seconds processing %.3f items '
         'per second on average') % (
            self.total,
            delta,
            self.total / delta,
        )

