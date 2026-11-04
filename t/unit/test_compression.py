from __future__ import annotations

import importlib.util
import sys

import pytest

from kombu import compression
from kombu.exceptions import DecodeError, LimitExceeded


def _optional_codec(module, *aliases):
    names = aliases + (module,)
    if any(importlib.util.find_spec(name) for name in names):
        return module
    return pytest.param(module, marks=pytest.mark.skip(
        reason=f'{module} not installed'))


LIMITED_CODECS = [
    'zlib',
    _optional_codec('bzip2', 'bz2'),
    _optional_codec('lzma'),
    _optional_codec(
        'zstd',
        'compression.zstd' if sys.version_info >= (3, 14)
        else 'backports.zstd',
    ),
]


class test_compression:

    def test_encoders__gzip(self):
        assert 'application/x-gzip' in compression.encoders()

    def test_encoders__bz2(self):
        pytest.importorskip('bz2')
        assert 'application/x-bz2' in compression.encoders()

    def test_encoders__brotli(self):
        pytest.importorskip('brotli')

        assert 'application/x-brotli' in compression.encoders()

    def test_encoders__lzma(self):
        pytest.importorskip('lzma')

        assert 'application/x-lzma' in compression.encoders()

    def test_encoders__zstd(self):
        pytest.importorskip('compression.zstd' if sys.version_info >= (3, 14) else 'backports.zstd')

        assert 'application/zstd' in compression.encoders()

    def test_compress__decompress__zlib(self):
        text = b'The Quick Brown Fox Jumps Over The Lazy Dog'
        c, ctype = compression.compress(text, 'zlib')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__bzip2(self):
        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'bzip2')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__brotli(self):
        pytest.importorskip('brotli')

        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'brotli')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__lzma(self):
        pytest.importorskip('lzma')

        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'lzma')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    def test_compress__decompress__zstd(self):
        pytest.importorskip('compression.zstd' if sys.version_info >= (3, 14) else 'backports.zstd')

        text = b'The Brown Quick Fox Over The Lazy Dog Jumps'
        c, ctype = compression.compress(text, 'zstd')
        assert text != c
        d = compression.decompress(c, ctype)
        assert d == text

    @pytest.mark.parametrize('codec', LIMITED_CODECS)
    def test_decompress__with_limit__roundtrip(self, codec):
        text = b'The Quick Brown Fox Jumps Over The Lazy Dog ' * 100
        c, ctype = compression.compress(text, codec)
        assert compression.decompress(c, ctype, max_size=len(text)) == text
        # a message exactly at the limit is allowed
        assert compression.decompress(c, ctype, max_size=len(text)) == text

    @pytest.mark.parametrize('codec', LIMITED_CODECS)
    def test_decompress__with_limit__exceeded(self, codec):
        text = b'x' * 10000
        c, ctype = compression.compress(text, codec)
        with pytest.raises(LimitExceeded):
            compression.decompress(c, ctype, max_size=1024)

    @pytest.mark.parametrize('codec', LIMITED_CODECS)
    def test_decompress__with_limit__truncated(self, codec):
        text = b'y' * 10000
        c, ctype = compression.compress(text, codec)
        with pytest.raises(Exception):
            compression.decompress(c[:len(c) // 2], ctype, max_size=10 ** 9)

    @pytest.mark.parametrize('max_size', [0, -1, True, False, '100', 1.5])
    def test_decompress__with_limit__invalid(self, max_size):
        with pytest.raises(ValueError):
            compression.decompress(b'', 'zlib', max_size=max_size)

    def test_decompress__with_limit__unknown_content_type(self):
        with pytest.raises(DecodeError):
            compression.decompress(b'whatever', 'application/x-unknown',
                                   max_size=10)

    def test_decompress__bomb_is_bounded(self):
        """A small compressed payload expanding past the limit is stopped."""
        import zlib
        payload = b'\x00' * (10 * 1024 * 1024)
        compressor = zlib.compressobj(9)
        bomb = compressor.compress(payload) + compressor.flush()
        assert len(bomb) < 100 * 1024
        with pytest.raises(LimitExceeded):
            compression.decompress(bomb, 'zlib', max_size=1024 * 1024)

    @pytest.mark.masked_modules('bz2')
    def test_no_bz2(self, mask_modules):
        c = sys.modules.pop('kombu.compression')
        try:
            import kombu.compression
            assert not hasattr(kombu.compression, 'bz2')
        finally:
            if c is not None:
                sys.modules['kombu.compression'] = c

    @pytest.mark.masked_modules('lzma')
    def test_no_lzma(self, mask_modules):
        c = sys.modules.pop('kombu.compression')
        try:
            import kombu.compression
            assert not hasattr(kombu.compression, 'lzma')
        finally:
            if c is not None:
                sys.modules['kombu.compression'] = c
