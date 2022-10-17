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
   Contains parquet exporter class for exporting data from the web crawling framework scrapy
"""

import typing as t
import logging

from scrapy.exporters import BaseItemExporter

MAX_FILE_SIZE = int(1.2e8)

SUPPORTED_EXPORTERS = {}
### Check which libraries are available for the exporters
try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    SUPPORTED_EXPORTERS["parquet"] = True
    logging.getLogger().info(
        "Successfully imported pyarrow. Export to parquet supported."
    )
except ImportError:
    SUPPORTED_EXPORTERS["parquet"] = False


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
        self.writer = None
        self.logger = logging.getLogger()
        self._configure(kwargs, dont_fail=dont_fail)
        self.records = PyarrowBatchBuffer()

    def _configure(self, options, dont_fail=False):
        """
        Configure the exporter by popping options from the ``options`` dict.
        If dont_fail is set, it won't raise an exception on unexpected options
        (useful for using with keyword arguments in subclasses ``__init__`` methods)
        """
        # --- Inherited options ---
        self.encoding = options.pop("encoding", None)
        self.fields_to_export = options.pop("fields_to_export", None)
        self.export_empty_fields = options.pop("export_empty_fields", False)

        # --- Options passed through feed settings ---
        # Params for write_table
        self.compression = options.pop("compression", "SNAPPY")

        # Other control options
        self.items_batch_size = options.pop("items_batch_size", 1000)

    def export_item(self, item):
        """
        Add a record to the PyarrowBatchBuffer. Write a file if
        MAX_FILE_SIZE is exceeded.
        """
        # Write the table if the batch buffer has reached or exceeded the max
        # file size. This is a backup method if a record batch becomes too large,
        # since the batch size is user-configurable.
        if self.records.should_flush():
            self._write_batches_to_file()

        # Or, if the batch is full, add it to the batch buffer and
        # clear the current batch.
        elif len(self.item_batch) > self.items_batch_size:
            self.records.insert(self.item_batch)
            self.item_batch = []

        record = dict(self._get_serialized_fields(item))

        self.item_batch.append(record)
        self.item_count += 1

    def _write_batches_to_file(self):
        """
        Turn the PyarrowBatchBuffer into a Table, then write it to a file.
        """
        if not self.writer:
            self.writer = pq.ParquetWriter(
                    where=self.file.name,
                    schema=self.records._batches[0].schema,
                    compression=self.compression,
            )

        for batch in self.records.flush():
            self.writer.write_batch(batch)
        
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
        self.records.insert(self.item_batch)
        self.item_batch = []
        self._write_batches_to_file()
        self.writer.close()
