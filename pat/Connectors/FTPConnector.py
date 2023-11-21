from ftplib import Error
from fsspec.implementations.ftp import TransferDone, FTPFile


def _fetch_range(self, start, end):
    """Get bytes between given byte limits

    Implemented by raising an exception in the fetch callback when the
    number of bytes received reaches the requested amount.

    Will fail if the server does not respect the REST command on
    retrieve requests.
    """
    out = []
    total = [0]

    def callback(x):
        total[0] += len(x)
        if total[0] > end - start:
            out.append(x[: (end - start) - total[0]])
            if end < self.size:
                raise TransferDone
        else:
            out.append(x)

        if total[0] == end - start and end < self.size:
            raise TransferDone

    try:
        self.fs.ftp.retrbinary(
            "RETR %s" % self.path,
            blocksize=self.blocksize,
            rest=None,
            callback=callback,
        )
    except TransferDone:
        try:
            # stop transfer, we got enough bytes for this block
            self.fs.ftp.abort()
            self.fs.ftp.getmultiline()
        except Error:
            self.fs._connect()

    return b"".join(out)


FTPFile._fetch_range = _fetch_range
