"""SFN activity task polling."""

import json
import errno
import threading
import logging as lg

import urllib3
from botocore import auth as botocore_auth
from botocore import awsrequest as botocore_awsrequest

from sfini import _util

_logger = lg.getLogger(__name__)


class _TrackingPool(botocore_awsrequest.AWSHTTPSConnectionPool):  # TODO: unit-test
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._released_conns = []

    def _get_conn(self, timeout=None):
        conn = super()._get_conn(timeout=timeout)
        self._released_conns.append(conn)
        return conn

    def _put_conn(self, conn):
        if conn in self._released_conns:
            self._released_conns.remove(conn)
        return super()._put_conn(conn)

    def close(self):
        super().close()
        [c.close() for c in self._released_conns.copy()]
        self._released_conns.clear()

    def request_from(self, request, timeout=None):
        """Perform a request.

        Args:
            request (botocore_awsrequest.AWSPreparedRequest): request data
            timeout (float or urllib3.Timeout): request timeout

        Returns:
            urllib3.HTTPResponse: response of request
        """

        return self.urlopen(
            request.method,
            request.url,
            headers=request.headers,
            body=request.body,
            timeout=timeout)


class _TaskPollBuilder:  # TODO: unit-test
    """Activity task poll request builder.

    Args:
        activity_arn (str): ARN of activity to poll for tasks
        worker_name (str): name of worker polling for tasks
        session (_util.AWSSession): session to get communication
            configuration from
    """

    scheme = "https"
    _method = "POST"
    _params = {"Action": "GetActivityTask", "Version": "2016-11-23"}
    _request_class = botocore_awsrequest.AWSRequest
    _auth_signer_class = botocore_auth.SigV4Auth

    def __init__(self, activity_arn, worker_name=None, *, session=None):
        self.activity_arn = activity_arn
        self.worker_name = worker_name
        self.session = session or _util.AWSSession()
        self._auth = self._auth_signer_class(
            self.session.credentials,
            "states",
            self.session.region)

    __repr__ = _util.easy_repr

    @_util.cached_property
    def host_name(self) -> str:
        """Step-function endpoint host-name."""
        return "states.%s.amazonaws.com" % self.session.region

    @_util.cached_property
    def _url(self) -> str:
        """Step-function endpoint URL."""
        return "%s://%s/" % (self.scheme, self.host_name)

    @_util.cached_property
    def _headers(self) -> dict:
        """Request headers."""
        return {"host": self.host_name, "Content-Type": "application/json"}

    @_util.cached_property
    def _data(self) -> dict:
        """Request headers."""
        data = {"activityArn": self.activity_arn}
        if self.worker_name is not None:
            data["workerName"] = self.worker_name
        return data

    def build_activity_poll_request(self):
        """Build the request for an activity task poll.

        Returns:
            botocore_awsrequest.AWSPreparedRequest: built request
        """

        req = self._request_class(
            method=self._method,
            url=self._url,
            headers=self._headers,
            data=self._data,
            params=self._params)
        self._auth.add_auth(req)
        return req.prepare()


class TaskPoll:  # TODO: unit-test
    """Polling for task.

    Args:
        activity_arn (str): ARN of activity to poll for tasks
        worker_name (str): name of worker polling for tasks
        session (_util.AWSSession): session to get communication
            configuration from
    """

    _pool_class = _TrackingPool
    _request_builder_class = _TaskPollBuilder
    _poll_timeout = 65.0

    def __init__(self, activity_arn, worker_name=None, *, session=None):
        self.activity_arn = activity_arn
        self.worker_name = worker_name
        self.session = session or _util.AWSSession()

        self._poller = threading.Thread(target=self._worker)
        self._exc = None
        self._stop = False
        self._resp = None
        self._got_response = threading.Event()
        self._pause_polling = threading.Event()
        self._pause_polling.set()
        self._request_builder = self._request_builder_class(
            activity_arn,
            worker_name=worker_name,
            session=self.session)
        self._pool = self._pool_class(
            self._request_builder.host_name,
            scheme=self._request_builder.scheme)

    __repr__ = _util.easy_repr

    @staticmethod
    def _handle_error_response(response):
        _s = "Received poll error response [%s]: %s"
        # _logger.error(_s % (response.status, response.reason))
        # assert response.status in (400, 403, 404, 500, 503)
        raise RuntimeError(_s % (response.status, response.reason))

    def _run_poll_uncaught(self):
        req = self._request_builder.build_activity_poll_request()
        resp = self._pool.request_from(req, timeout=self._poll_timeout)
        if resp.status != 200:
            self._handle_error_response(resp)
        return resp

    def _run_poll(self):
        try:
            return self._run_poll_uncaught()
        except urllib3.exceptions.ReadTimeoutError:
            _logger.debug("Poll timed out")
            return None
        except OSError as e:
            if e.errno != errno.EBADF:
                _logger.error("Socket error", exc_info=e)
                raise
            _logger.debug("Socket closed")
            return None
        except (Exception, KeyboardInterrupt) as e:
            _logger.error("Polling failed", exc_info=e)
            raise

    def _poll(self):
        while True:
            if self._stop:
                break
            self._pause_polling.wait()
            resp = self._run_poll()
            if resp is not None:
                self._resp = resp
                self._got_response.set()

    def _worker(self):
        try:
            self._poll()
        except (Exception, KeyboardInterrupt) as e:
            self._exc = e

    @staticmethod
    def _process_response(response):
        with response:
            data = response.read()
        return json.loads(data.decode("utf-8"))

    def start(self):
        """Start polling."""
        _util.assert_valid_name(self.worker_name)
        self._poller.start()

    def stop(self):
        """Stop polling."""
        self._stop = True
        self._pool.close()

    def get(self, timeout=None):
        if self._stop:
            raise RuntimeError("Polling stopped")
        if self._got_response.wait(timeout=timeout):
            self._got_response.clear()
            resp = self._resp
            self._resp = None
            return self._process_response(resp)
        return None

    def pause(self):
        self._pause_polling.clear()
        self._pool.close()

    def unpause(self):
        self._pause_polling.set()
