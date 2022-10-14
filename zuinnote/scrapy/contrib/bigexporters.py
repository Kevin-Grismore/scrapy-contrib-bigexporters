"""
Copyright (c) 2020 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


"""
   Contains various formats for exporting data from the web crawling framework scrapy
"""


"""
Parquet exporter
Write export as parquet file (based on fastparquet - you need to add the latest version as a dependency)
Custom parquet feed exporter
FEED_EXPORTERS={'parquet': 'zuinnote.scrapy.contrib.bigexporters.ParquetItemExporter'} # register additional format
Example local file, e.g. data-quotes-2020-01-01T10-00-00.parquet
FEEDS = {
'data-%(name)s-%(time)s.parquet': {
        'format': 'parquet',
        'encoding': 'utf8',
        'store_empty': False,
        'item_export_kwargs': {
           'compression': 'GZIP', # compression to be used in Parquet, UNCOMPRESSED, GZIP, SNAPPY (package: python-snappy), LZO (package: lzo), BROTLI (package: brotli), LZ4 (package: lz4), ZSTD (package: zstandard) note: compression may require additional libraries
           'times': 'int64', # type for times int64 or int96, spark is int96 only
           'hasnulls': True,# can contain nulls
           'convertallstrings': False,# convert all values to string. recommended for compatibility reasons, conversion to native types is suggested as part of the ingestion in the processing platform
           'writeindex': False, # write index as extra column
           'objectencoding': 'infer', # schema of data
           'rowgroupoffset': 50000000, # offset row groups
           'items_rowgroup': 10000  # how many items per rowgroup, should be several thousands, e.g. between 5,000 and 30,000. The more rows the higher the memory consumption and the better the compression on the final parquet file
        },
    }
}

Example s3 file, e.g. s3://mybucket/data-quotes-2020-01-01T10-00-00.parquet
FEEDS = {
's3://aws_key:aws_secret@mybucket/data-%(name)s-%(time)s.parquet': {
        'format': 'parquet',
        'encoding': 'utf8',
        'store_empty': False,
        'item_export_kwargs': {
           'compression': 'GZIP', # compression to be used in Parquet, UNCOMPRESSED, GZIP, SNAPPY (package: python-snappy), LZO (package: lzo), BROTLI (package: brotli), LZ4 (package: lz4), ZSTD (package: zstandard) note: compression may require additional libraries
           'times': 'int64', # type for times int64 or int96, spark is int96 only
           'hasnulls': True,# can contain nulls
           'convertallstrings': False,# convert all values to string. recommended for compatibility reasons, conversion to native types is suggested as part of the ingestion in the processing platform
           'writeindex': False, # write index as extra column
           'objectencoding': 'infer', # schema of data
           'rowgroupoffset': 50000000, # offset row groups
           'items_rowgroup': 10000  # how many items per rowgroup, should be several thousands, e.g. between 5,000 and 30,000. The more rows the higher the memory consumption and the better the compression on the final parquet file
        },
    }
}

see: https://docs.scrapy.org/en/latest/topics/exporters.html
"""

import typing as t
import logging

from scrapy.exporters import BaseItemExporter

MAX_FILE_SIZE = int(1.2e8)

SUPPORTED_EXPORTERS = {}
### Check which libraries are available for the exporters
try:
    import pyarrow as pa
    from pyarrow import fs

    SUPPORTED_EXPORTERS["parquet"] = True
    logging.getLogger().info(
        "Successfully imported pyarrow. Export to parquet supported."
    )
except ImportError:
    SUPPORTED_EXPORTERS["parquet"] = False


class ParquetItemExporter(BaseItemExporter):
    """
    Parquet exporter
    """

    def __init__(self, file, dont_fail=False, **kwargs):
        """
        Initialize exporter
        """
        super().__init__(**kwargs)
        self.file = file  # file name
        self.item_count = 0
        self.file_count = 0
        self.item_batch = []
        self.logger = logging.getLogger()
        self._configure(kwargs, dont_fail=dont_fail)
        self.filesystem = self._create_filesystem()
        self.records = PyarrowBatchBuffer()

    def _configure(self, options, dont_fail=False):
        """Configure the exporter by poping options from the ``options`` dict.
        If dont_fail is set, it won't raise an exception on unexpected options
        (useful for using with keyword arguments in subclasses ``__init__`` methods)
        """
        # Inherited options
        self.encoding = options.pop("encoding", None)
        self.fields_to_export = options.pop("fields_to_export", None)
        self.export_empty_fields = options.pop("export_empty_fields", False)
        # Options passed through feed settings
        # Params for write_table
        self.compression = options.pop("compression", "SNAPPY")
        # Other control options
        self.items_per_file = options.pop("items_per_file", 1000)
        self.gcp_credentials_block = options.pop("gcp_credentials_block", None)
        self.aws_credentials_block = options.pop("aws_credentials_block", None)

    def _create_filesystem(self):
        """
        Check for a prefix on the file name string and create the corresponding
        pyarrow filesystem, to be used in the filesystem arg in pa.parquet.write_table()

        Remote filesystems should be instantiated with their credentials if not running
        in an environment where the credentials exist as environemnt variables.
        """

        if self.file.startswith("gcs://") and self.gcp_credentials_block:
            service_account_info = self.gcp_credentials_block.get('service_account_info', None)
            return fs.GcsFileSystem(
                target_service_account=service_account_info,
            )

        # TODO: it seems like a good idea to support the S3 filesystem here as well
        # elif file starts with "s3://":
        #     return an S3 filesystem built with the required credentials as if
        #     they were passed in from a prefect-aws aws credentials block
        #     https://prefecthq.github.io/prefect-aws/credentials/

        else:
            return fs.LocalFileSystem()

    def export_item(self, item):
        """
        Add a record to the PyarrowBatchBuffer. Write a file if items_per_file or
        MAX_FILE_SIZE is exceeded.
        """
        # Write the table if the batch buffer has reached or exceeded the max
        # file size. This is a backup method if a record batch becomes too large,
        # since the batch size is user-configurable.
        if self.records.should_flush():
            self._write_table_to_file()

        # Or, if the batch is full, add it to the batch buffer, clear the
        # batch, then write the file.
        elif len(self.item_batch) > self.items_per_file:
            self.records.insert(self.item_batch)
            self.item_batch = []
            self._write_table_to_file()

        record = dict(self._get_serialized_fields(item))

        self.item_batch.append(record)
        self.item_count += 1

    def _write_table_to_file(self):
        """
        Turn the PyarrowBatchBuffer into a Table, then write it to a file.
        """
        table = pa.Table.from_batches(self.records.flush())
        pa.parquet.write_table(
            table=table,
            where=f"{self.file}-{self.file_count}",
            compression=self.compression,
            filesystem=self.filesystem,
        )
        
        self.item_count = 0
        self.file_count += 1

    def start_exporting(self):
        """
        Triggered when Scrapy starts exporting. Useful to configure headers etc.
        """
        if not SUPPORTED_EXPORTERS["parquet"]:
            raise RuntimeError(
                "Error: Cannot export to parquet. Cannot import fastparquet. Have you installed it?"
            )

    def finish_exporting(self):
        """
        Triggered when Scrapy ends exporting. Useful to shutdown threads, close files etc.
        """
        self._write_table_to_file()


class PyarrowBatchBuffer:
    """
    Used to track state during a transformation from an iterable of dictionaries to an iterable of pyarrow tables
    """

    def __init__(self, target_size: int = MAX_FILE_SIZE):
        self._size: int = 0
        self._batches: t.List[pa.RecordBatch] = []
        self.target_size = target_size

    def clear(self):
        self._size = 0
        self._batches = []

    def insert(self, records: t.Iterable[t.Dict]):
        batch = pa.RecordBatch.from_struct_array(pa.array(records))
        self._size += batch.nbytes
        self._batches.append(batch)

    def should_flush(self) -> bool:
        return self._size >= self.target_size

    def flush(self, clear: bool = True) -> t.List[pa.RecordBatch]:
        # returns current batches in buffer and optionally calls self.clear()
        _ = self._batches.copy()
        if clear:
            self.clear()
        return _

    def __bool__(self):
        return bool(self._batches)
