"""
CSV reader and writer for unicode strings, from
http://docs.python.org/2/library/csv.html#examples with the addition of
UnicodeDictReader, UnicodeDictWriter, and optimizations for passing through
UTF-8.
"""

from __future__ import absolute_import

import cStringIO as StringIO
import codecs
import csv


class Recoder:
    """
    Iterator that decodes an input stream and reencodes on output.
    """
    def __init__(self, f, input_encoding="utf-8", output_encoding="utf-8"):
        if input_encoding == output_encoding:
            self.passthrough = True
            self.reader = iter(f)
        else:
            self.passthrough = False
            self.reader = codecs.getreader(input_encoding)(f)
            self.encoder = codecs.getincrementalencoder(output_encoding)()

    def __iter__(self):
        return self

    def next(self):
        if self.passthrough:
            return self.reader.next()
        else:
            return self.encoder.encode(self.reader.next())


class UnicodeReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """
    csv_reader = csv.reader

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = Recoder(f, input_encoding=encoding, output_encoding="utf-8")
        self.reader = self.csv_reader(f, dialect=dialect, **kwds)

    def _decode_row_utf8(self, row):
        return [unicode(s, "utf-8") for s in row]

    def next(self):
        row = self.reader.next()
        return self._decode_row_utf8(row)

    def __iter__(self):
        return self

    def __getattr__(self, name):
        return getattr(self.reader, name)


class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """
    csv_writer = csv.writer

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        if encoding == "utf-8":
            self.passthrough = True
            self.writer = self.csv_writer(f, dialect=dialect, **kwds)
        else:
            # Redirect output to a queue
            self.queue = StringIO.StringIO()
            self.writer = self.csv_writer(self.queue, dialect=dialect, **kwds)
            self.stream = f
            self.encoder = codecs.getincrementalencoder(encoding)()

    def _encode_row_utf8(self, row):
        return [s.encode("utf-8") for s in row]

    def writerow(self, row):
        self.writer.writerow(self._encode_row_utf8(row))
        if not self.passthrough:
            # Fetch UTF-8 output from the queue, reencode to the target
            # encoding.
            data = self.queue.getvalue().decode("utf-8")
            data = self.encoder.encode(data)
            self.stream.write(data)
            self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def __getattr__(self, name):
        return getattr(self.writer, name)


class UnicodeDictReader(UnicodeReader):
    csv_reader = csv.DictReader

    def _decode_row_utf8(self, row):
        return dict((unicode(k, "utf-8"), unicode(v, "utf-8")) for k, v in row.items())


class UnicodeDictWriter(UnicodeWriter):
    csv_writer = csv.DictWriter

    def __init__(self, f, fieldnames, **kwds):
        kwds['fieldnames'] = fieldnames
        super(UnicodeDictWriter, self).__init__(f, **kwds)

    def _encode_row_utf8(self, row):
        return dict((k.encode("utf-8"), v.encode("utf-8")) for k, v in row.items())
