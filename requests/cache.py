# -*- coding: UTF-8 -*-
import inspect
import re
from collections import defaultdict
from cStringIO import StringIO
from datetime import datetime
from hashlib import md5

from .models import Response
from .structures import CaseInsensitiveDict
from requests import session

class Storage(object):
    """
    Storage mantains a collection of records, a record is a cached response
    associated with a url.

    Every url can have one or more records associated, different records must
    have different subtypes; a subtype can be None or a plain dictionary.

    As far as Storage is concerned a subtype is just an opaque object used to
    distinguish different versions of the cache; the subtypes are manipolated
    by the Handlers.

    To understand why this complexity is necessary read this:
    http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.6
    """
    def new_record(self, url, subtype, headers):
        """
        Prepares the storage to accept a new record; the returned object has
        two methods:

         - write(chunk)
         - close()

        Use the write method to append chunks of data to the new record, use
        the close method when you have finished.

        If you don't call the close method the record will not be saved in the
        storage.
        """
        pass

    def get_record(self, url, subtype):
        """
        Returns headers and content of a record:

            tuple(headers, content)
        """
        pass

    def get_record_headers(self, url, subtype):
        """
        Returns the headers of a record. This method exists for performance
        reasons; depending on the storage type headers and content could be
        stored in different manners.
        """
        pass

    def get_record_content(self, url, subtype):
        """
        Returns the content of a record. This method exists for performance
        reasons; depending on the storage type headers and content could be
        stored in different manners.
        """
        pass

    def get_record_subtypes(self, url):
        """
        Returns all known subtypes of a record.
        """
        pass

    def purge_record(self, url, subtype):
        """
        Remove an entry from the storage.
        """
        pass

class InMemory(object):
    def __init__(self, max_size=10*1024*1024):
        self.buffer = defaultdict(lambda: dict(timestamp=datetime.now, records=[]))

    def _key(self, url):
        return md5(url).digest()

    def _put(self, url, subtype, headers, content):
        k = self._key(url)
        self.buffer[k]['records'].append({
            'subtype': subtype,
            'headers': headers,
            'content': content,
        })

    def new_record(self, url, subtype, headers):
        headers = CaseInsensitiveDict(headers)
        class TSlot(list):
            def write(self, chunk):
                self.append(chunk)
            def close(_):
                self._put(url, subtype, headers, ''.join(_))
        return TSlot()

    def _get(self, url, subtype):
        k = self._key(url)
        if k in self.buffer:
            for record in self.buffer[k]['records']:
                if record['subtype'] == subtype:
                    return record
        return None

    def get_record_headers(self, url, subtype):
        record = self._get(url, subtype)
        if record:
            return record['headers']

    def get_record_content(self, url, subtype):
        record = self._get(url, subtype)
        if record:
            return record['content']

    def get_record(self, url, subtype):
        record = self._get(url, subtype)
        if record:
            return (record['headers'], record['content'])

    def get_record_subtypes(self, url):
        k = self._key(url)
        if k in self.buffer:
            return [ r['subtype'] for r in self.buffer[k]['records'] ]
        return None

    def purge_record(self, url, subtype):
        k = self._key(url)
        if k in self.buffer:
            for ix, record in enumerate(self.buffer[k]['records']):
                if record['subtype'] == subtype:
                    del self.buffer[k]['records'][ix]
                    return True
        return False

def time2httpfulldate(dt):
    """
    format a datetime (UTC) according to the RFC 1123

    http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1
    """
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return "%s, %02d %s %04d %02d:%02d:%02d GMT" % (
        days[dt.weekday()],
        dt.day,
        months[dt.month - 1],
        dt.year,
        dt.hour,
        dt.minute,
        dt.second,)


def httpfulldate2time(s):
    """
    Convert a string formatted as RFC 1123, RFC 850 or ANSI C's asctime() to a
    datetime.

    http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1
    """
    # Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
    # Sunday, 06-Nov-94 08:49:37 GMT ; RFC 850, obsoleted by RFC 1036
    # Sun Nov  6 08:49:37 1994       ; ANSI C's asctime() format
    fulldays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    # mode:
    #   0 -> rfc 1123
    #   1 -> rfc 1036
    #   2 -> asctime
    mode = 0
    s = s.strip().lower()
    for x in fulldays:
        if s.startswith(x):
            mode = 1
            break
    else:
        if ',' not in s:
            mode = 2

    if mode in (0, 1):
        text = s.split(',', 1)[1].strip()
        for ix, n in enumerate(months):
            if n in text:
                if mode == 0:
                    text = text.replace(n, '%02d' % (ix+1,))
                else:
                    text = text.replace('-%s-' % n, ' %02d ' % (ix+1,))
                break
        text = text.replace('gmt', '')
        if mode == 0:
            fmt = '%d %m %Y %H:%M:%S'
        else:
            fmt = '%d %m %y %H:%M:%S'
    else:
        text = s.split(' ', 1)[1].strip()
        for ix, n in enumerate(months):
            if text.startswith(n):
                text = text.replace(n, '%02d ' % (ix+1,))
                break
        fmt = '%m %d %H:%M:%S %Y'

    try:
        return datetime.strptime(text.strip(), fmt)
    except ValueError:
        return None

class Tee(object):
    def __init__(self, resp, tslot):
        self.raw = resp.raw
        self.tslot = tslot

    def __getattr__(self, name):
        return getattr(self.__dict__['raw'], name)

    def read(self, *args, **kwargs):
        chunk = self.raw.read(*args, **kwargs)
        if chunk:
            self.tslot.write(chunk)
        else:
            self.tslot.close()
        return chunk

class EtagValidator(object):
    def handle_request(self, req, subtypes):
        if not subtypes or None not in subtypes:
            return None
        if 'etag' in subtypes[None]:
            req.headers['If-None-Match'] = subtypes[None]['etag']
            return ('request', req)

    def handle_response(self, resp):
        if resp.status_code == 304:
            return ('fetch', (resp.url, None))
        elif resp.status_code < 300 and 'etag' in resp.headers:
            return ('store', (resp.url, None))

class CacheableRequest(object):
    def handle_request(self, req, subtypes):
        if not subtypes:
            return

        # Search a cached entry compatible with the current request; if a valid
        # entry is not found I exit in order to contact the remote server.
        fallback = subtypes.get(None)
        for subtype, cached_headers in subtypes.items():
            if subtype is None:
                continue
            for key, value in subtype.items():
                if req.headers[key] != value:
                    break
            else:
                break
        else:
            if fallback is not None:
                subtype = None
                cached_headers = fallback
            else:
                return

        # Cache-control max-age overrides the Expires header
        max_age = None
        expires = None
        ccontrol = cached_headers['cache-control']
        if ccontrol:
            if 'no-cache' in ccontrol:
                return
            match = re.search(r'max-age\s*=\s*(\d+)', ccontrol)
            if match:
                max_age = int(match.group(1))

        if max_age is None:
            if 'expires' in cached_headers:
                expires = httpfulldate2time(cached_headers['expires'])

        if max_age is None and expires is None:
            return

        def diff(a, b):
            t = a - b
            return t.days * 86400 + t.seconds

        # first of all we need to know how much old is a cache entry
        # see: http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.2.3
        date = httpfulldate2time(cached_headers['date'])

        apparent_age = max(
            0,
            diff(cached_headers['_response_time'], date))

        # the 'Age' header, if present, is added by an intermediate cache
        try:
            age_value = int(cached_headers['age'])
        except (TypeError, ValueError):
            age_value = 0

        corrected_received_age = max(apparent_age, age_value)

        # delay imposed by the neetwork latency
        response_delay = diff(cached_headers['_response_time'], cached_headers['_request_time'])

        corrected_initial_age = corrected_received_age + response_delay

        # with resident_time we take in account the time spent in the cache
        resident_time = diff(datetime.now(), cached_headers['_response_time'])

        current_age = resident_time + corrected_initial_age

        # now we cache determine if the entry is still valid
        # see: http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.2.4
        if max_age:
            freshness_lifetime = max_age
        else:
            freshness_lifetime = diff(expires, date)

        if freshness_lifetime > current_age:
            return ('fetch', (req.full_url, subtype))
        else:
            return ('purge', (req.full_url, subtype))

    def handle_response(self, resp):
        if resp.request.method not in ('GET', 'HEAD') or resp.status_code >= 300:
            return None

        if 'expires' not in resp.headers:
            return None

        # from: http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.44

        # The Vary field value indicates the set of request-header fields that
        # fully determines, while the response is fresh, whether a cache is
        # permitted to use the response to reply to a subsequent request
        # without revalidation.
        vary = resp.headers['vary']

        # A Vary field value of "*" implies that a cache cannot determine from
        # the request headers of a subsequent request whether this response is
        # the appropriate representation.
        if vary and vary.strip() == '*':
            return None

        if vary:
            subtype = {}
            for name in vary.split(','):
                name = name.lower().strip()
                value = resp.headers[name]
                if value is None:
                    return None
                subtype[name] = value
        else:
            subtype = None

        expires = httpfulldate2time(resp.headers['expires'])
        if expires > datetime.now():
            return ('store', (resp.url, subtype))

        return None

HANDLERS = (
    CacheableRequest(),
    EtagValidator(),
)

def _build_response_from_storage(storage, req, url, subtype):
    headers, content = storage.get_record(url, subtype)
    resp = Response()
    resp.from_cache = True
    resp.config = req.config
    resp.status_code = 200
    resp.headers = headers
    resp.raw = StringIO(content)
    req.response = resp
    req.sent = True
    return resp

def pre_send_hook(storage, req):
    cached = storage.get_record_subtypes(req.full_url)
    if cached:
        subtypes = {}
        for subtype in cached:
            subtypes[subtype] = storage.get_record_headers(req.full_url, subtype)
    else:
        subtypes = None

    for h in HANDLERS:
        res = h.handle_request(req, subtypes)
        if res is None:
            continue
        else:
            print h , 'can handle the request'
            if res[0] == 'request':
                print 'new request'
                req = res[1]
            elif res[0] == 'fetch':
                print 'fetch the response from the storage'
                _build_response_from_storage(storage, req, res[1][0], res[1][1])
            else:
                storage.purge(res[1][0], res[1][1])
            break
    req._request_time = datetime.now()
    return req

def response_hook(storage, resp):
    # HTTP/1.1 strongly recommends the use of the Date header
    date = resp.headers['date']
    if not date or not httpfulldate2time(date):
        return None

    for h in HANDLERS:
        res = h.handle_response(resp)
        if res is None:
            continue
        else:
            print h, 'can handle the response'
            cmd, key = res
            url, subtype = key
            if cmd == 'store':
                print 'store the response in the storage', subtype
                headers = CaseInsensitiveDict(resp.headers)
                headers['_request_time'] = resp.request._request_time
                headers['_response_time'] = datetime.now()
                record = storage.new_record(url, subtype, headers)
                resp.raw = Tee(resp, record)
            elif cmd == 'fetch':
                print 'fetch the response from the storage'
                headers, content = storage.get_record(url, subtype)
                resp.headers = headers
                resp.raw = StringIO(content)
            break

def SessionCache(storage=InMemory, *args, **kwargs):
    if inspect.isclass(storage):
        st = storage()
    else:
        st = storage
    user_hooks = kwargs.get('hooks', {})
    def chain_pre_request(req):
        f = user_hooks.get('pre_request')
        if f:
            req = f(req)
        return pre_send_hook(st, req)

    def chain_response(resp):
        f = user_hooks.get('response')
        if f:
            f(resp)
        return response_hook(st, resp)

    hooks = {
        'pre_request': chain_pre_request,
        'response': chain_response,
    }
    return session(hooks=hooks, *args, **kwargs)
