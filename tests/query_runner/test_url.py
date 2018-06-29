from redash.query_runner.url import _build_url
from tests import BaseTestCase


class TestBuildUrl(BaseTestCase):
    def test_build_url(self):
        self.assertEqual('http://example.com/foo/bar.json', _build_url('', 'http://example.com/foo/bar.json'))
        self.assertEqual('http://example.com/foo/bar.json', _build_url(None, 'http://example.com/foo/bar.json'))
        self.assertEqual('http://example.com/foo/bar.json', _build_url('http://example.com', '/foo/bar.json'))
        self.assertRaises(ValueError, _build_url, 'http://example.com', 'http://example.com/foo/bar.json')
