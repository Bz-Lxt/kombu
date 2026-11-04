from __future__ import annotations

import sys
from unittest.mock import Mock, patch

import pytest

from kombu.compression import compress
from kombu.exceptions import LimitExceeded
from kombu.message import Message


class test_Message:

    def test_repr(self):
        assert repr(Message('b', channel=Mock()))

    def test_decode(self):
        m = Message('body', channel=Mock())
        decode = m._decode = Mock()
        assert m._decoded_cache is None
        assert m.decode() is m._decode.return_value
        assert m._decoded_cache is m._decode.return_value
        m._decode.assert_called_with()
        m._decode = Mock()
        assert m.decode() is decode.return_value

    def test_reraise_error(self):
        m = Message('body', channel=Mock())
        callback = Mock(name='callback')
        try:
            raise KeyError('foo')
        except KeyError:
            m.errors.append(sys.exc_info())
        m._reraise_error(callback)
        callback.assert_called()

        with pytest.raises(KeyError):
            m._reraise_error(None)

    @patch('kombu.message.decompress')
    def test_decompression_stores_error(self, decompress):
        decompress.side_effect = RuntimeError()
        m = Message('body', channel=Mock(), headers={'compression': 'zlib'})
        with pytest.raises(RuntimeError):
            m._reraise_error(None)

    def test_decompression_without_limit_keeps_legacy_behavior(self):
        body, ctype = compress(b'data' * 100, 'zlib')
        m = Message(body, channel=Mock(),
                    headers={'compression': ctype})
        assert not m.errors
        assert m.body == b'data' * 100
        assert m._decompressed is True

    def test_decompression_with_limit_allows_message_within_limit(self):
        body, ctype = compress(b'data' * 100, 'zlib')
        m = Message(body, channel=Mock(),
                    headers={'compression': ctype},
                    max_decompression_size=400)
        assert not m.errors
        assert m.body == b'data' * 100
        assert m._decompressed is True

    def test_decompression_with_limit_rejects_oversized_body(self):
        body, ctype = compress(b'data' * 1000, 'zlib')
        m = Message(body, channel=Mock(),
                    headers={'compression': ctype},
                    max_decompression_size=64)
        # the failure is recorded for the decode-error path ...
        assert m.errors
        with pytest.raises(LimitExceeded):
            m._reraise_error(None)
        # ... and the original compressed body is preserved untouched.
        assert m.body == body
        assert m._decompressed is False

    def test_decompression_with_limit_rejects_corrupt_body(self):
        body, ctype = compress(b'data' * 1000, 'zlib')
        corrupt = body[:20] + b'\xff\xff\xff\xff' + body[24:]
        m = Message(corrupt, channel=Mock(),
                    headers={'compression': ctype},
                    max_decompression_size=10 ** 6)
        assert m.errors
        assert m.body == corrupt
        assert m._decompressed is False

    def test_uncompressed_body_ignores_limit(self):
        m = Message(b'plain body', channel=Mock(),
                    headers={}, max_decompression_size=1)
        assert not m.errors
        assert m.body == b'plain body'
        assert m._decompressed is False
