Requests: HTTP for Humans (now with caching support)
====================================================


This fork of `request` aim to add cache support to the original library.

::

    >>> from requests.cache import SessionCache, FSStorage
    >>> s = SessionCache(storage=FSStorage('./cache'))
    ...

Designed to support multiple backend the only actually implemented is a
filesystem based.

