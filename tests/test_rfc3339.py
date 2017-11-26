from __future__ import absolute_import, division, print_function

import datetime
import time

from nose.tools import eq_, ok_

from sunspear.lib.rfc3339 import _timedelta_to_seconds, _timezone, _utc_offset, rfc3339


class TestRFC3339(object):
    '''
    Test the use of the timezone saved locally. Since it is hard to test using
    doctest.
    '''

    def setUp(self):
        local_utcoffset = _utc_offset(datetime.datetime.now(), True)
        self.local_utcoffset = datetime.timedelta(seconds=local_utcoffset)
        self.local_timezone = _timezone(local_utcoffset)

    def test_datetime(self):
        d = datetime.datetime.now()
        eq_(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + self.local_timezone)

    def test_datetime_timezone(self):

        class FixedNoDst(datetime.tzinfo):
            'A timezone info with fixed offset, not DST'

            def utcoffset(self, dt):
                return datetime.timedelta(hours=2, minutes=30)

            def dst(self, dt):
                return None

        fixed_no_dst = FixedNoDst()

        class Fixed(FixedNoDst):
            'A timezone info with DST'

            def dst(self, dt):
                return datetime.timedelta(hours=3, minutes=15)

        fixed = Fixed()

        d = datetime.datetime.now().replace(tzinfo=fixed_no_dst)
        timezone = _timezone(_timedelta_to_seconds(fixed_no_dst.\
                                                   utcoffset(None)))
        eq_(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + timezone)

        d = datetime.datetime.now().replace(tzinfo=fixed)
        timezone = _timezone(_timedelta_to_seconds(fixed.dst(None)))
        eq_(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + timezone)

    def test_datetime_utc(self):
        d = datetime.datetime.now()
        d_utc = d + self.local_utcoffset
        eq_(rfc3339(d, utc=True),
                         d_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))

    def test_date(self):
        d = datetime.date.today()
        eq_(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + self.local_timezone)

    def test_date_utc(self):
        d = datetime.date.today()
        # Convert `date` to `datetime`, since `date` ignores seconds and hours
        # in timedeltas:
        # >>> datetime.date(2008, 9, 7) + datetime.timedelta(hours=23)
        # datetime.date(2008, 9, 7)
        d_utc = datetime.datetime(*d.timetuple()[:3]) + self.local_utcoffset
        eq_(rfc3339(d, utc=True),
                         d_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))

    def test_timestamp(self):
        d = time.time()
        eq_(rfc3339(d),
                         datetime.datetime.fromtimestamp(d).\
                         strftime('%Y-%m-%dT%H:%M:%S') + self.local_timezone)

    def test_timestamp_utc(self):
        d = time.time()
        d_utc = datetime.datetime.utcfromtimestamp(d) + self.local_utcoffset
        eq_(rfc3339(d),
                         (d_utc.strftime('%Y-%m-%dT%H:%M:%S') +
                          self.local_timezone))

    def test_before_1970(self):
        d = datetime.date(1885, 01, 04)
        ok_(rfc3339(d).startswith('1885-01-04T00:00:00'))
        eq_(rfc3339(d, utc=True, use_system_timezone=False),
                         '1885-01-04T00:00:00Z')

    def test_1920(self):
        d = datetime.date(1920, 02, 29)
        x = rfc3339(d, utc=False, use_system_timezone=True)
        ok_(x.startswith('1920-02-29T00:00:00'))

    # If these tests start failing it probably means there was a policy change
    # for the Pacific time zone.
    # See http://en.wikipedia.org/wiki/Pacific_Time_Zone.
    if 'PST' in time.tzname:
        def test_PDTChange(self):
            '''Test Daylight saving change'''
            # PDT switch happens at 2AM on March 14, 2010

            # 1:59AM PST
            eq_(rfc3339(datetime.datetime(2010, 3, 14, 1, 59)),
                             '2010-03-14T01:59:00-08:00')
            # 3AM PDT
            eq_(rfc3339(datetime.datetime(2010, 3, 14, 3, 0)),
                             '2010-03-14T03:00:00-07:00')

        def test_PSTChange(self):
            '''Test Standard time change'''
            # PST switch happens at 2AM on November 6, 2010

            # 0:59AM PDT
            eq_(rfc3339(datetime.datetime(2010, 11, 7, 0, 59)),
                             '2010-11-07T00:59:00-07:00')

            # 1:00AM PST
            # There's no way to have 1:00AM PST without a proper tzinfo
            eq_(rfc3339(datetime.datetime(2010, 11, 7, 1, 0)),
                             '2010-11-07T01:00:00-07:00')

    def test_timedelta_to_seconds(self):
        eq_(10800, _timedelta_to_seconds(datetime.timedelta(hours=3)))
        eq_(11700, _timedelta_to_seconds(datetime.timedelta(hours=3, minutes=15)))

    def test_utc_offset(self):
        if time.localtime().tm_isdst:
            system_timezone = -time.altzone
        else:
            system_timezone = -time.timezone
        ok_(_utc_offset(datetime.datetime.now(), True) == system_timezone)
        ok_(not _utc_offset(datetime.datetime.now(), False))
