import gc
import math
import time
import datetime

from django.core.management.base import BaseCommand
from django.contrib.sessions.models import Session
from redis_sessions.session import SessionStore

CHUNK_SIZE = 100000


class Command(BaseCommand):
    '''Convert your database sessions to redis sessions

    If this script is too slow, you can use this instead:

    Put this query in `sessions.sql`:

    COPY (
        SELECT
            'SETEX ' || session_key || ' '
            || DATE_PART('epoch', expire_date - NOW())::integer ||
            ' "' || session_data || '"'
        FROM django_session
        where expire_date > NOW()
    )
    TO STDOUT;

    And execute this command:
    # psql -o sessions.txt -f sessions.sql | redis-cli
    '''
    def handle(self, *args, **kwargs):
        server = SessionStore().server

        self.sessions = Session.objects.all().values_list(
            'session_key', 
            'session_data',
            'expire_date', 
        )                
        self.start_progressbar()

        now = datetime.datetime.now()
        pipe = server.pipeline(transaction=False)
        for i, session in enumerate(self.sessions.iterator()):
            session_key, session_data, expire_date = session
            if i % CHUNK_SIZE == 0:
                gc.collect()

            # convert the expire date to a ttl in seconds
            delta = expire_date - now
            ttl = (delta.days * 24 * 60 * 60) + delta.seconds

            # set the key, value and ttl
            pipe.set(session_key, session_data)
            pipe.expire(session_key, ttl)
            self.update_progressbar(i)

        pipe.execute()
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

