"""Compression utilities."""

from __future__ import annotations

import sys
import zlib

from kombu.exceptions import DecodeError, LimitExceeded
from kombu.utils.encoding import ensure_bytes

_aliases = {}
_encoders = {}
_decoders = {}
_bounded_decoders = {}

__all__ = ('register', 'encoders', 'get_encoder',
           'get_decoder', 'compress', 'decompress')


def register(encoder, decoder, content_type, aliases=None,
             bounded_decoder=None):
    """Register new compression method.

    Arguments:
    ---------
        encoder (Callable): Function used to compress text.
        decoder (Callable): Function used to decompress previously
            compressed text.
        content_type (str): The mime type this compression method
            identifies as.
        aliases (Sequence[str]): A list of names to associate with
            this compression method.
        bounded_decoder (Callable): Optional function used to decompress
            previously compressed text while capping the decompressed
            size.  When provided it must accept ``(body, max_size)`` and
            stop decompressing as soon as more than ``max_size`` bytes
            would be produced, raising an exception instead.  Compression
            methods without a bounded decoder cannot enforce a
            decompression size limit.
    """
    _encoders[content_type] = encoder
    _decoders[content_type] = decoder
    if bounded_decoder is not None:
        _bounded_decoders[content_type] = bounded_decoder
    else:
        _bounded_decoders.pop(content_type, None)
    if aliases:
        _aliases.update((alias, content_type) for alias in aliases)


def encoders():
    """Return a list of available compression methods."""
    return list(_encoders)


def get_encoder(t):
    """Get encoder by alias name."""
    t = _aliases.get(t, t)
    return _encoders[t], t


def get_decoder(t):
    """Get decoder by alias name."""
    return _decoders[_aliases.get(t, t)]


def compress(body, content_type):
    """Compress text.

    Arguments:
    ---------
        body (AnyStr): The text to compress.
        content_type (str): mime-type of compression method to use.
    """
    encoder, content_type = get_encoder(content_type)
    return encoder(ensure_bytes(body)), content_type


def _bounded_decompress(create_decompressor, body, max_size):
    """Stream ``body`` through a decompressor while capping its output.

    The decompressed payload is never materialized past ``max_size``
    bytes, so a small compressed message ("decompression bomb") cannot
    exhaust memory when the limit is enforced.

    Raises
    ------
        LimitExceeded: if more than ``max_size`` bytes are produced.
        EOFError: if the compressed stream ends before the frame is
            complete (i.e. the payload is truncated/corrupt).
        Exception: any error raised by the underlying decompressor for
            corrupt input is propagated unchanged.
    """
    decompressor = create_decompressor()
    chunks = []
    total = 0
    pending = body

    while True:
        # Request one byte more than the remaining budget: if the
        # decompressor can produce it the message is over the limit and
        # processing stops without buffering any more output.
        chunk = decompressor.decompress(pending, max_size - total + 1)
        chunks.append(chunk)
        total += len(chunk)
        if total > max_size:
            raise LimitExceeded(
                f'Decompressed message body exceeds the maximum allowed '
                f'size of {max_size} bytes')
        if getattr(decompressor, 'eof', False):
            break
        if getattr(decompressor, 'needs_input', True) is False:
            # The decompressor still has output buffered (zstd): drain
            # it without providing additional compressed input.
            pending = b''
            continue
        unconsumed = getattr(decompressor, 'unconsumed_tail', b'') or b''
        if unconsumed:
            # zlib keeps input it could not consume because the output
            # buffer was full.
            pending = unconsumed
        else:
            pending = getattr(decompressor, 'unused_data', b'') or b''
            if not chunk and not pending:
                # All input has been consumed but the stream is not at
                # its end: the payload is truncated or corrupt.
                raise EOFError(
                    'Compressed stream ended before the end-of-stream '
                    'marker was reached')

    flush = getattr(decompressor, 'flush', None)
    if flush is not None:
        tail = flush()
        total += len(tail)
        if total > max_size:
            raise LimitExceeded(
                f'Decompressed message body exceeds the maximum allowed '
                f'size of {max_size} bytes')
        chunks.append(tail)
    return b''.join(chunks)


def decompress(body, content_type, max_size=None):
    """Decompress compressed text.

    Arguments:
    ---------
        body (AnyStr): Previously compressed text to uncompress.
        content_type (str): mime-type of compression method used.
        max_size (int): If set to a positive integer, the message is
            decompressed incrementally and decompression fails as soon as
            the decompressed payload would exceed this many bytes.
            When :const:`None` (default) the whole payload is decompressed
            without enforcing a limit.

    Raises
    ------
        ValueError: if ``max_size`` is not a positive integer.
        DecodeError: if the compression method is unknown or cannot
            enforce a decompression size limit.
        LimitExceeded: if the decompressed payload exceeds ``max_size``.
    """
    if max_size is None:
        return get_decoder(content_type)(body)

    if (isinstance(max_size, bool)
            or not isinstance(max_size, int) or max_size <= 0):
        raise ValueError(
            'max_size must be a positive integer or None')

    content_type = _aliases.get(content_type, content_type)
    bounded_decoder = _bounded_decoders.get(content_type)
    if bounded_decoder is None:
        raise DecodeError(
            f'Cannot enforce a decompression size limit for '
            f'{content_type!r}: no bounded decoder is available')
    return bounded_decoder(body, max_size)


register(zlib.compress,
         zlib.decompress,
         'application/x-gzip', aliases=['gzip', 'zlib'],
         bounded_decoder=lambda body, max_size: _bounded_decompress(
             zlib.decompressobj, body, max_size))

try:
    import bz2
except ImportError:  # pragma: no cover
    pass  # No bz2 support
else:
    register(bz2.compress,
             bz2.decompress,
             'application/x-bz2', aliases=['bzip2', 'bzip'],
             bounded_decoder=lambda body, max_size: _bounded_decompress(
                 bz2.BZ2Decompressor, body, max_size))

try:
    import brotli
except ImportError:  # pragma: no cover
    pass
else:
    # brotli's streaming API cannot cap the produced output, so a
    # decompression size limit cannot be enforced for these messages and
    # ``decompress`` with ``max_size`` raises DecodeError instead.
    register(brotli.compress,
             brotli.decompress,
             'application/x-brotli', aliases=['brotli'])

try:
    import lzma
except ImportError:  # pragma: no cover
    pass  # no lzma support
else:
    register(lzma.compress,
             lzma.decompress,
             'application/x-lzma', aliases=['lzma', 'xz'],
             bounded_decoder=lambda body, max_size: _bounded_decompress(
                 lzma.LZMADecompressor, body, max_size))

try:
    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
except ImportError:  # pragma: no cover
    pass  # no zstd support
else:
    register(zstd.compress,
             zstd.decompress,
             'application/zstd', aliases=['zstd', 'zstandard'],
             bounded_decoder=lambda body, max_size: _bounded_decompress(
                 zstd.ZstdDecompressor, body, max_size))
