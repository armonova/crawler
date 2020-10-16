import urllib.request
from collections import OrderedDict
from datetime import datetime


class Domain:
    def __init__(self, nam_domain, int_time_limit_between_requests):
        self.time_last_access = datetime(1970, 1, 1)
        self.nam_domain = nam_domain
        self.int_time_limit_seconds = int_time_limit_between_requests

    @property
    def time_since_last_access(self):
        now = datetime.today()
        diff = (now - self.time_last_access).total_seconds()
        return diff

    def accessed_now(self):
        self.time_last_access = datetime.today()
        pass

    def is_accessible(self):
        if self.time_since_last_access > self.int_time_limit_seconds:
            return True
        else:
            return False

    def __hash__(self):
        return hash(self.nam_domain)

    def __eq__(self, domain):
        return domain == self.nam_domain

    def __str__(self):
        return self.nam_domain

    def __repr__(self):
        return str(self)
