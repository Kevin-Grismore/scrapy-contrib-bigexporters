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

from scrapy.exporters import BaseItemExporter
from scrapy.utils.project import get_project_settings


import logging

SUPPORTED_EXPORTERS = {}
### Check which libraries are available for the exporters
try:
    #from fastparquet import write as fp_write
    #import pandas as pd

    SUPPORTED_EXPORTERS["parquet"] = True
    logging.getLogger().info(
        "Successfully imported fastparquet. Export to parquet supported."
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
        self.itemcount = 0  # initial item count
        self.columns = []  # initial columns to export
        self.logger = logging.getLogger()
        self._configure(kwargs, dont_fail=dont_fail)

    def _configure(self, options, dont_fail=False):
        """Configure the exporter by poping options from the ``options`` dict.
        If dont_fail is set, it won't raise an exception on unexpected options
        (useful for using with keyword arguments in subclasses ``__init__`` methods)
        """
        self.encoding = options.pop("encoding", None)
        self.fields_to_export = options.pop("fields_to_export", None)
        self.export_empty_fields = options.pop("export_empty_fields", False)
        # Read settings
        self.pq_compression = options.pop("compression", "GZIP")
        self.pq_times = options.pop("times", "int64")
        self.pq_objectencoding = options.pop("objectencoding", "infer")
        self.pq_convertstr = options.pop("convertallstrings", False)
        self.pq_hasnulls = options.pop("hasnulls", True)
        self.pq_writeindex = options.pop("writeindex", False)
        self.pq_items_rowgroup = options.pop("items_rowgroup", 10000)
        self.pq_rowgroupoffset = options.pop("rowgroupoffset", 50000000)

    def export_item(self, item):
        """
        Export a specific item to the file
        """
        # Initialize writer
        if len(self.columns) == 0:
            self._init_table(item)
        # Create a new row group to write
        if self.itemcount > self.pq_items_rowgroup:
            self._flush_table()
        # Add the item to data frame
        self.df = pd.concat([self.df, self._get_df_from_item(item)])
        self.itemcount += 1
        return item

    def start_exporting(self):
        """
        Triggered when Scrapy starts exporting. Useful to configure headers etc.
        """
        if not SUPPORTED_EXPORTERS["parquet"]:
            raise RuntimeError(
                "Error: Cannot export to parquet. Cannot import fastparquet. Have you installed it?"
            )
        self.firstBlock = True  # first block of parquet file

    def finish_exporting(self):
        """
        Triggered when Scrapy ends exporting. Useful to shutdown threads, close files etc.
        """
        self._flush_table()

    def _get_columns(self, item):
        """
        Determines the columns of an item
        """
        if isinstance(item, dict):
            # for dicts try using fields of the first item
            self.columns = list(item.keys())
        elif hasattr(item, "__dataclass_fields__"):
             # for dataclasses
            self.columns = list(getattr(item, "__dataclass_fields__").keys())
        else:
            # use fields declared in Item
            self.columns = list(item.fields.keys())

    def _init_table(self, item):
        """
        Initializes table for parquet file
        """
        # initialize columns
        self._get_columns(item)
        self._reset_rowgroup()

    def _get_df_from_item(self, item):
        """
        Get the dataframe from item
        """
        row = {}
        fields = dict(
            self._get_serialized_fields(item, default_value="", include_empty=True)
        )
        for column in self.columns:
            if self.pq_convertstr == True:
                row[column] = str(fields.get(column, None))
            else:
                value = fields.get(column, None)
                row[column] = value
        if self.pq_convertstr == True:
            return pd.DataFrame(row, index=[0]).astype(str)
        return pd.DataFrame.from_dict([row])

    def _reset_rowgroup(self):
        """
        Reset dataframe for writing
        """
        if self.pq_convertstr == False:  # auto determine schema
            # initialize df
            self.df = pd.DataFrame(columns=self.columns)
        else:
            # initialize df with zero strings to derive correct schema
            self.df = pd.DataFrame(columns=self.columns).astype(str)

    def _flush_table(self):
        """
        Writes the current row group to parquet file
        """
        if len(self.df.index) > 0:
            # reset written entries
            self.itemcount = 0
            # write existing dataframe as rowgroup to parquet file
            papp = True
            if self.firstBlock == True:
                self.firstBlock = False
                papp = False
            fp_write(
                self.file.name,
                self.df,
                append=papp,
                compression=self.pq_compression,
                has_nulls=self.pq_hasnulls,
                write_index=self.pq_writeindex,
                file_scheme="simple",
                object_encoding=self.pq_objectencoding,
                times=self.pq_times,
                row_group_offsets=self.pq_rowgroupoffset,
            )
            # initialize new data frame for new row group
            self._reset_rowgroup()
