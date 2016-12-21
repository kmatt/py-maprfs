# -*- coding: utf-8 -*-
"Main module defining filesystem and file classes"
from __future__ import absolute_import

import ctypes
import logging
import os
import sys
import re
import warnings
from collections import deque
from .lib import _lib

PY3 = sys.version_info.major > 2

from .compatibility import FileNotFoundError, urlparse, ConnectionError
from .utils import read_block


logger = logging.getLogger(__name__)


def hdfs_conf():
    """ Load HDFS config from default locations. """
    confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get('HADOOP_INSTALL',
                           '') + '/hadoop/conf')
    files = 'core-site.xml', 'hdfs-site.xml'
    conf = {}
    for afile in files:
        try:
            conf.update(conf_to_dict(os.sep.join([confd, afile])))
        except FileNotFoundError:
            pass
    if 'fs.defaultFS' in conf:
        u = urlparse(conf['fs.defaultFS'])  # pragma: no cover
        conf['host'] = u.hostname  # pragma: no cover
        conf['port'] = u.port  # pragma: no cover
    return conf


def conf_to_dict(fname):
    """ Read a hdfs-site.xml style conf file, produces dictionary """
    name_match = re.compile("<name>(.*?)</name>")
    val_match = re.compile("<value>(.*?)</value>")
    conf = {}
    for line in open(fname):
        name = name_match.search(line)
        if name:
            key = name.groups()[0]
        val = val_match.search(line)
        if val:
            val = val.groups()[0]
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass
            if val == 'false':
                val = False
            if val == 'true':
                val = True
            conf[key] = val
    return conf


conf = hdfs_conf()
# DEFAULT_HOST = conf.get('host', 'localhost')
# DEFAULT_PORT = conf.get('port', 8020)
DEFAULT_HOST = conf.get('host', 'default')
DEFAULT_PORT = conf.get('port', 0)


def ensure_bytes(s):
    """ Give strings that ctypes is guaranteed to handle """
    if PY3 and isinstance(s, bytes):
        return s
    if not PY3 and isinstance(s, str):
        return s
    if hasattr(s, 'encode'):
        return s.encode()
    if hasattr(s, 'tobytes'):
        return s.tobytes()
    if isinstance(s, bytearray):
        return bytes(s)
    if not PY3 and hasattr(s, 'tostring'):
        return s.tostring()
    if isinstance(s, dict):
        return {k: ensure_bytes(v) for k, v in s.items()}
    else:
        # Perhaps it works anyway - could raise here
        return s


def ensure_string(s):
    """ Ensure that the result is a string

    >>> ensure_string(b'123')
    '123'
    >>> ensure_string('123')
    '123'
    >>> ensure_string({'x': b'123'})
    {'x': '123'}
    """
    if isinstance(s, dict):
        return {k: ensure_string(v) for k, v in s.items()}
    if hasattr(s, 'decode'):
        return s.decode()
    else:
        return s


def ensure_trailing_slash(s, ensure=True):
    """ Ensure that string ends with a slash

    >>> ensure_trailing_slash('/user/directory')
    '/user/directory/'
    >>> ensure_trailing_slash('/user/directory/')
    '/user/directory/'
    >>> ensure_trailing_slash('/user/directory/', False)
    '/user/directory'
    """
    slash = '/' if isinstance(s, str) else b'/'
    if ensure and not s.endswith(slash):
        s += slash
    if not ensure and s.endswith(slash):
        s = s[:-1]
    return s


class HDFileSystem(object):
    """ Connection to an HDFS namenode

    >>> hdfs = HDFileSystem(host='127.0.0.1', port=8020)  # doctest: +SKIP
    """
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, user=None,
                 ticket_cache=None, token=None, pars=None, connect=True):
        """
        Parameters
        ----------
        host : str (default from config files)
            namenode hostname or IP address, in case of HA mode it is name
            of the cluster that can be found in "fs.defaultFS" option.
        port : int (8020)
            namenode RPC port usually 8020, in HA mode port mast be None
        user, ticket_cache, token : str
            kerberos things
        pars : {str: str}
            other parameters for hadoop, that you can find in hdfs-site.xml,
            primary used for enabling secure mode or passing HA configuration.
        """
        self.host = host
        self.port = port
        self.user = user
        self._handle = None

        if ticket_cache and token:
            m = "It is not possible to use ticket_cache and token in same time"
            raise RuntimeError(m)

        self.ticket_cache = ticket_cache
        self.token = token
        self.pars = pars or {}
        if connect:
            self.connect()

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['_handle']
        logger.debug("Serialize with state: %s", d)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._handle = None
        self.connect()

    def __repr__(self):
        state = ['Disconnected', 'Connected'][self._handle is not None]
        return 'hdfs://%s:%s, %s' % (self.host, self.port, state)

    def __del__(self):
        if self._handle:
            self.disconnect()

    def _ensure_connected(self):
        """
        Raise an exception if not connected

        Calling library functions when ``self._handle`` is None can cause the
        library to print warning messages or, in some cases, a segmentation
        fault. (``hdfsExists`` is one example of this.) Use this function as a
        guard before calling library functions.
        """
        if not self.connected():
            raise IOError("File system is not connected.")

    def connect(self):
        """ Connect to the name node

        This happens automatically at startup
        """
        if self.connected():
            return

        o = _lib.hdfsNewBuilder()
        if self.port is not None:
            _lib.hdfsBuilderSetNameNodePort(o, self.port)
        _lib.hdfsBuilderSetNameNode(o, ensure_bytes(self.host))
        if self.user:
            _lib.hdfsBuilderSetUserName(o, ensure_bytes(self.user))

        if self.ticket_cache:
            _lib.hdfsBuilderSetKerbTicketCachePath(o, ensure_bytes(self.ticket_cache))

        if self.token:
            _lib.hdfsBuilderSetToken(o, ensure_bytes(self.token))

        for par, val in self.pars.items():
            if not  _lib.hdfsBuilderConfSetStr(o, ensure_bytes(par), ensure_bytes(val)) == 0:
                warnings.warn('Setting conf parameter %s failed' % par)

        fs = _lib.hdfsBuilderConnect(o)
        if fs:
            logger.debug("Connect to handle %d", fs.contents.filesystem)
            self._handle = fs
            #if self.token:   # TODO: find out what a delegation token is
            #    self._token = _lib.hdfsGetDelegationToken(self._handle,
            #                                             ensure_bytes(self.user))
        else:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise ConnectionError('Connection Failed: {}'.format(msg))

    def disconnect(self):
        """ Disconnect from name node """
        if self.connected():
            logger.debug("Disconnect from handle %d", self._handle.contents.filesystem)
            _lib.hdfsDisconnect(self._handle)
            self._handle = None

    def connected(self):
        """Indicates whether this object is currently connected."""
        return self._handle is not None

    def open(self, path, mode='rb', replication=0, buff=0, block_size=0):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on HDFS
        mode: string
            One of 'rb', 'wb', or 'ab'
        replication: int
            Replication factor; if zero, use system default (only on write)
        block_size: int
            Size of data-node blocks if writing
        """
        self._ensure_connected()
        if block_size and mode != 'wb':
            raise ValueError('Block size only valid when writing new file')
        if ('a' in mode and self.exists(path) and
            replication !=0 and replication > 1):
            raise IOError("Appending to an existing file with replication > 1"
                    " is unsupported")
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                    " and manage bytes" % (mode + 'b'))
        return HDFile(self, path, mode, replication=replication, buff=buff,
                block_size=block_size)

    def du(self, path, total=False, deep=False):
        """Returns file sizes on a path.

        Parameters
        ----------
        path : string
            where to look
        total : bool (False)
            to add up the sizes to a grand total
        deep : bool (False)
            whether to recurse into subdirectories
        """
        fi = self.ls(path)
        if deep:
            for apath in fi:
                if apath['kind'] == 'directory':
                    fi.extend(self.ls(apath['name']))
        if total:
            return {path: sum(f['size'] for f in fi)}
        return {p['name']: p['size'] for p in fi}

    def df(self):
        """ Used/free disc space on the HDFS system """
        self._ensure_connected()
        cap = _lib.hdfsGetCapacity(self._handle)
        used = _lib.hdfsGetUsed(self._handle)
        return {'capacity': cap, 'used': used, 'percent-free': 100*(cap-used)/cap}

    def get_hosts(self, path, start=None, length=None):
        """ Fetch the physical locations of file chunks

        Returns a list of lists of hostnames for each chunk.

        Ex:
            h = get_hosts('foo')
            len(h) # The number of chunks
            h[2]   # List of hosts that have a copy of the third chunk
        """

        # Let ``p`` be the return value from ``_lib.hdfsGetHosts``. ``p`` is a
        # pointer to a dynamically allocated array of dynamically allocated
        # arrays of c_char_p.
        #
        # p[block_index][host_index]
        # If there was a library error, p.contents, p[0], and p[0].contents -> the same ValueError for NULL pointer access
        # p[block_index + 1].contents raises ValueError -> block_index is the last block
        # p[block_index][host_index + 1] is None -> host_index is the last host
        #
        # The following two helpers are for processing this data structure.

        def until_null(p):
            ix = 0
            while True:
                try:
                    p[ix].contents
                    yield p[ix]
                    ix += 1
                except ValueError:
                    raise StopIteration()

        def until_none(p):
            ix = 0
            while True:
                if p[ix] is None:
                    raise StopIteration()
                else:
                    yield p[ix]
                    ix += 1

        if start is None:
            start = 0
        if length is None:
            length = self.info(path)['size']

        self._ensure_connected()
        out = _lib.hdfsGetHosts(self._handle, ensure_bytes(path), start, length)

        # ``out`` is a pointer; if the library call failed (for example, the
        # file doesn't exist), then trying to access ``out.contents`` will
        # raise a ValueError for trying to access a null pointer
        try:
            out.contents
        except ValueError:
            # TODO Collect the error message
            raise

        hostnames = list(list(until_none(blockp)) for blockp in until_null(out))

        _lib.hdfsFreeHosts(out)

        return hostnames

    def get_block_locations(self, path, start=None, length=None):
        """ Fetch physical locations of blocks """

        pathinfo = self.info(path)

        if start is None:
            start = 0
        if length is None:
            length = pathinfo['size']

        assert start >= 0, "Invalid byte range"
        assert length >= 0, "Invalid length"

        pathsize = pathinfo['size']

        assert (start + length) <= pathsize, "File is smaller than requested range"

        # MapR allows for setting the chunk size at the file level
        chunksize = pathinfo['block_size']

        # Which chunk does offset ``start`` fall into?
        first_chunk_index = start // chunksize
        # What about offset ``start`` + ``length``? (Like Python lists. If
        # chunksize == 10, start == 9, length == 1, we're talking about the
        # slice that includes only byte number 9 in the file, which is only the
        # first chunk.) (Need a special case to handle getting chunks for
        # zero-length file.)
        last_chunk_index = max(0, start + length - 1) // chunksize
        # How many chunks are in this file? (This number is the largest chunk
        # index for the file; the number of chunks minus one.)
        max_chunk_index = max(0, pathsize - 1) // chunksize

        # We need a list of the byte indexes where the chunks start
        chunk_starts = list(range(first_chunk_index * chunksize, (last_chunk_index + 1) * chunksize, chunksize))
        # And a list of how many bytes are contained in each chunk. This is
        # just chunksize, except for the last chunk. We need to check if the
        # request byte range includes the last chunk in the file.
        chunk_lengths = [chunksize] * len(chunk_starts)
        if last_chunk_index == max_chunk_index:
            chunk_lengths[-1] = pathsize % chunksize

        chunk_hosts = self.get_hosts(path, start, length)

        locs = [
            {'hosts': names, 'offset': strt, 'length': lngth}
            for names, strt, lngth in zip(chunk_hosts, chunk_starts, chunk_lengths)
        ]

        return locs

    def info(self, path):
        """ File information (as a dict) """
        if not self.exists(path):
            raise FileNotFoundError(path)
        self._ensure_connected()
        fi = _lib.hdfsGetPathInfo(self._handle, ensure_bytes(path)).contents
        out = info_to_dict(fi)
        _lib.hdfsFreeFileInfo(ctypes.byref(fi), 1)
        return ensure_string(out)

    def walk(self, path):
        """ Get all file entries below given path """
        return ([ensure_trailing_slash(ensure_string(path), False)]
                + list(self.du(path, False, True).keys()))

    def glob(self, path):
        """ Get list of paths mathing glob-like pattern (i.e., with "*"s).

        If passed a directory, gets all contained files; if passed path
        to a file, without any "*", returns one-element list containing that
        filename. Does not support python3.5's "**" notation.
        """
        path = ensure_string(path)
        try:
            f = self.info(path)
            if f['kind'] == 'directory' and '*' not in path:
                path = ensure_trailing_slash(path) + '*'
            else:
                return [f['name']]
        except IOError:
            pass
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind+1]
        else:
            root = '/'
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                                        .rstrip('/')
                                        .replace('*', '[^/]*')
                                        .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        return out

    def ls(self, path, detail=True):
        """ List files at path

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        detail : bool (=True)
            if True, each list item is a dict of file properties;
            otherwise, returns list of filenames
        """
        if not self.exists(path):
            raise FileNotFoundError(path)

        pathinfo = self.info(path)

        if pathinfo['kind'] == 'file':
            out = [pathinfo]
        elif pathinfo['kind'] == 'directory':
            self._ensure_connected()
            num = ctypes.c_int(0)
            fi = _lib.hdfsListDirectory(self._handle, ensure_bytes(path), ctypes.byref(num))
            out = [ensure_string(info_to_dict(fi[i])) for i in range(num.value)]
            # If the directory is empty, then ``fi`` does not need to be freed
            _lib.hdfsFreeFileInfo(fi, num.value) if num.value > 0 else None
        else:
            raise TypeError("Path has an unknown type: %s" % pathinfo['kind'])

        if detail:
            return out
        else:
            return [o['name'] for o in out]

    def mkdir(self, path):
        """ Make directory at path """
        self._ensure_connected()
        out = _lib.hdfsCreateDirectory(self._handle, ensure_bytes(path))
        if out != 0:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError('Create directory failed: {}'.format(msg))

    def set_replication(self, path, replication):
        """ Instruct HDFS to set the replication for the given file.

        If successful, the head-node's table is updated immediately, but
        actual copying will be queued for later. It is acceptable to set
        a replication that cannot be supported (e.g., higher than the
        number of data-nodes).
        """
        if replication < 0:
            raise ValueError('Replication must be positive, or 0 for system default')
        self._ensure_connected()
        out = _lib.hdfsSetReplication(self._handle, ensure_bytes(path),
                                     ctypes.c_int16(int(replication)))
        if out != 0:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError('Set replication failed: {}'.format(msg))

    def mv(self, path1, path2):
        """ Move file at path1 to path2 """
        if not self.exists(path1):
            raise FileNotFoundError(path1)
        self._ensure_connected()
        out = _lib.hdfsRename(self._handle, ensure_bytes(path1), ensure_bytes(path2))
        return out == 0

    def rm(self, path, recursive=True):
        "Use recursive for `rm -r`, i.e., delete directory and contents"
        if not self.exists(path):
            raise FileNotFoundError(path)
        self._ensure_connected()
        out = _lib.hdfsDelete(self._handle, ensure_bytes(path), bool(recursive))
        if out != 0:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError('Remove failed on %s %s' % (path, msg))

    def exists(self, path):
        """ Is there an entry at path? """
        self._ensure_connected()
        out = _lib.hdfsExists(self._handle, ensure_bytes(path) )
        return out == 0

    def chmod(self, path, mode):
        """Change access control of given path

        Exactly what permissions the file will get depends on HDFS
        configurations.

        Parameters
        ----------
        path : string
            file/directory to change
        mode : integer
            As with the POSIX standard, each octal digit refers to
            user-group-all, in that order, with read-write-execute as the
            bits of each group.

        Examples
        --------
        >>> hdfs.chmod('/path/to/file', 0o777)  # make read/writeable to all # doctest: +SKIP
        >>> hdfs.chmod('/path/to/file', 0o700)  # make read/writeable only to user # doctest: +SKIP
        >>> hdfs.chmod('/path/to/file', 0o100)  # make read-only to user # doctest: +SKIP
        """
        if not self.exists(path):
            raise FileNotFoundError(path)
        self._ensure_connected()
        out = _lib.hdfsChmod(self._handle, ensure_bytes(path), ctypes.c_short(mode))
        if out != 0:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError("chmod failed on %s %s" % (path, msg))

    def chown(self, path, owner, group):
        """ Change owner/group """
        if not self.exists(path):
            raise FileNotFoundError(path)
        self._ensure_connected()
        out = _lib.hdfsChown(self._handle, ensure_bytes(path), ensure_bytes(owner),
                            ensure_bytes(group))
        if out != 0:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError("chown failed on %s %s" % (path, msg))

    def cat(self, path):
        """ Return contents of file """
        if not self.exists(path):
            raise FileNotFoundError(path)
        with self.open(path, 'rb') as f:
            result = f.read()
        return result

    def get(self, hdfs_path, local_path, blocksize=2**16):
        """ Copy HDFS file to local """
        #TODO: _lib.hdfsCopy() may do this more efficiently
        if not self.exists(hdfs_path):
            raise FileNotFoundError(hdfs_path)
        with self.open(hdfs_path, 'rb') as f:
            with open(local_path, 'wb') as f2:
                out = 1
                while out:
                    out = f.read(blocksize)
                    f2.write(out)

    def getmerge(self, path, filename, blocksize=2**16):
        """ Concat all files in path (a directory) to output file """
        files = self.ls(path)
        with open(filename, 'wb') as f2:
            for apath in files:
                with self.open(apath['name'], 'rb') as f:
                    out = 1
                    while out:
                        out = f.read(blocksize)
                        f2.write(out)

    def put(self, filename, path, chunk=2**16):
        """ Copy local file to path in HDFS """
        with self.open(path, 'wb') as f:
            with open(filename, 'rb') as f2:
                while True:
                    out = f2.read(chunk)
                    if len(out) == 0:
                        break
                    f.write(out)

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.info(path)['size']
        if size > length:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(length - size)
            return f.read(size)

    def head(self, path, size=1024):
        """ Return first bytes of file """
        with self.open(path, 'rb') as f:
            return f.read(size)

    def touch(self, path):
        """ Create zero-length file """
        self.open(path, 'wb').close()

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an HDFS file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned will not include the surrounding delimiter strings.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on HDFS
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> hdfs.read_block('/data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> hdfs.read_block('/data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200'

        See Also
        --------
        maprfs.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['size']
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


def struct_to_dict(s):
    """ Return dictionary vies of a simple ctypes record-like structure """
    return dict((ensure_string(name), getattr(s, name)) for (name, p) in s._fields_)


def info_to_dict(s):
    """ Process data returned by hdfsInfo """
    d = struct_to_dict(s)
    d['kind'] = {68: 'directory', 70: 'file'}[d['kind']]
    return d


mode_numbers = {'w': 1, 'r': 0, 'a': 1025,
                'wb': 1, 'rb': 0, 'ab': 1025}

class HDFile(object):
    """ File on HDFS

    Matches the standard Python file interface.

    Examples
    --------
    >>> with hdfs.open('/path/to/hdfs/file.txt') as f:  # doctest: +SKIP
    ...     bytes = f.read(1000)  # doctest: +SKIP
    >>> with hdfs.open('/path/to/hdfs/file.csv') as f:  # doctest: +SKIP
    ...     df = pd.read_csv(f, nrows=1000)  # doctest: +SKIP
    """
    def __init__(self, fs, path, mode, replication=0, buff=0, block_size=0):
        """ Called by open on a HDFileSystem """
        if 't' in mode:
            raise NotImplementedError("Opening a file in text mode is not yet supported")
        self.fs = fs
        self.path = path
        self.replication = replication  # ignored in mapr
        self.buff = buff  # ignored in mapr
        self._fs = fs._handle
        self.buffers = []
        self._handle = None
        self.mode = mode
        self.block_size = block_size
        self.lines = deque([])
        self._set_handle()

    def _set_handle(self):
        out = _lib.hdfsOpenFile(self._fs, ensure_bytes(self.path),
                                mode_numbers[self.mode], self.buff,
                                ctypes.c_short(self.replication),
                                ctypes.c_int64(self.block_size))
        if not out:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError("Could not open file: %s, mode: %s %s" %
                          (self.path, self.mode, msg))
        self._handle = out

    def read(self, length=None):
        """ Read bytes from open file """
        if not _lib.hdfsFileIsOpenForRead(self._handle):
            raise IOError('File not read mode')
        buffers = []

        if length is None:
            out = 1
            while out:
                out = self.read(2**16)
                buffers.append(out)
        else:
            while length:
                bufsize = min(2**16, length)
                p = ctypes.create_string_buffer(bufsize)
                ret = _lib.hdfsRead(self._fs, self._handle, p, ctypes.c_int32(bufsize))
                if ret == 0:
                    break
                if ret > 0:
                    if ret < bufsize:
                        buffers.append(p.raw[:ret])
                    elif ret == bufsize:
                        buffers.append(p.raw)
                    length -= ret
                else:
                    raise IOError('Read file %s Failed:' % self.path, -ret)

        return b''.join(buffers)

    def readline(self, chunksize=2**16, lineterminator='\n'):
        """ Return a line using buffered reading.

        Reads and caches chunksize bytes of data, and caches lines
        locally. Subsequent readline calls deplete those lines until
        empty, when a new chunk will be read. A read and readline are
        not therefore generally pointing to the same location in the file;
        `seek()` and `tell()` will give the true location in the file,
        which will be one chunk in even after calling `readline` once.

        Line iteration uses this method internally.
        """
        lineterminator = ensure_bytes(lineterminator)
        if len(self.lines) < 2:
            buffers = []
            while True:
                out = self.read(chunksize)
                buffers.append(out)
                if lineterminator in out:
                    break
                if not out:
                    remains = list(self.lines)
                    self.lines = deque([])
                    return b''.join(remains + buffers)
            self.lines = deque(b''.join(list(self.lines) +
                                        buffers).split(lineterminator))
        return self.lines.popleft() + lineterminator

    def _genline(self):
        while True:
            out = self.readline()
            if out:
                yield out
            else:
                raise StopIteration

    def __iter__(self):
        """ Enables `for line in file:` usage """
        return self._genline()

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def tell(self):
        """ Get current byte location in a file """
        out = _lib.hdfsTell(self._fs, self._handle)
        if out == -1:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError('Tell Failed on file %s %s' % (self.path, msg))
        return out

    def seek(self, offset, from_what=0):
        """ Set file read position. Read mode only.

        Attempt to move out of file bounds raises an exception. Note that,
        by the convention in python file seek, offset should be <=0 if
        from_what is 2.

        Parameters
        ----------
        offset : int
            byte location in the file.
        from_what : int 0, 1, 2
            if 0 (befault), relative to file start; if 1, relative to current
            location; if 2, relative to file end.

        Returns
        -------
        new position
        """
        if from_what not in {0, 1, 2}:
            raise ValueError('seek mode must be 0, 1 or 2')
        info = self.info()
        if from_what == 1:
            offset = offset + self.tell()
        elif from_what == 2:
            offset = info['size'] + offset
        if offset < 0 or offset > info['size']:
            raise ValueError('Attempt to seek outside file')
        out = _lib.hdfsSeek(self._fs, self._handle, ctypes.c_int64(offset))
        if out == -1:
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError('Seek Failed on file %s' % (self.path, msg))  # pragma: no cover
        return self.tell()

    def info(self):
        """ Filesystem metadata about this file """
        return self.fs.info(self.path)

    def write(self, data):
        """ Write bytes to open file (which must be in w or a mode) """
        data = ensure_bytes(data)
        if not data:
            return
        if not _lib.hdfsFileIsOpenForWrite(self._handle):
            # msg = ensure_string(_lib.hdfsGetLastError())
            msg = ''
            raise IOError('File not write mode: {}'.format(msg))
        write_block = 64 * 2**20
        for offset in range(0, len(data), write_block):
            d = ensure_bytes(data[offset:offset + write_block])
            if not _lib.hdfsWrite(self._fs, self._handle, d, len(d)) == len(d):
                # msg = ensure_string(_lib.hdfsGetLastError())
                msg = ''
                raise IOError('Write failed on file %s, %s' % (self.path, msg))
        return len(data)

    def flush(self):
        """ Send buffer to the data-node; actual write to disc may happen later """
        if 'w' in self.mode:
            _lib.hdfsFlush(self._fs, self._handle)

    def close(self):
        """ Flush and close file, ensuring the data is readable """
        if self.mode == 'closed':
            # Prevent multiple attempts to close the file, which will cause libhdfs to throw errors
            pass
        elif self._handle is None:
            # HDFile( ... ) threw an IOError; we never got a handle and we're cleaning up an empty object
            self.mode = 'closed'
        else:
            self.flush()
            _lib.hdfsCloseFile(self._fs, self._handle)
            self._handle = None  # _libhdfs releases memory
            self.mode = 'closed'

    @property
    def read1(self):
        return self.read

    @property
    def closed(self):
        return self.mode == 'closed'

    def writable(self):
        return self.mode.startswith('w') or self.mode.startswith('a')

    def seekable(self):
        return self.readable()

    def readable(self):
        return self.mode.startswith('r')

    def __del__(self):
        self.close()

    def __repr__(self):
        return 'hdfs://%s:%s%s, %s' % (self.fs.host, self.fs.port,
                                            self.path, self.mode)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
