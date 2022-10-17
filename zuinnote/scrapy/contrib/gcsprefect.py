import os
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from google.oauth2 import service_account
from twisted.internet import threads
from zope.interface import implementer, Interface


class IFeedStorage(Interface):
    """Interface that all Feed Storages must implement"""

    def __init__(uri, *, feed_options=None):
        """Initialize the storage with the parameters given in the URI and the
        feed-specific options (see :setting:`FEEDS`)"""

    def open(spider):
        """Open the storage for the given spider. It must return a file-like
        object that will be used for the exporters"""

    def store(file):
        """Store the given file stream"""


@implementer(IFeedStorage)
class BlockingFeedStorage:
    def open(self, spider):
        path = spider.crawler.settings["FEED_TEMPDIR"]
        if path and not os.path.isdir(path):
            raise OSError("Not a Directory: " + str(path))

        return NamedTemporaryFile(prefix="feed-", dir=path)

    def store(self, file):
        return threads.deferToThread(self._store_in_thread, file)

    def _store_in_thread(self, file):
        raise NotImplementedError


class GCSFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, project_id, acl, credentials):
        self.project_id = project_id
        self.acl = acl
        self.credentials = service_account.Credentials.from_service_account_info(
            credentials
        )
        u = urlparse(uri)
        self.bucket_name = u.hostname
        self.blob_name = u.path[1:]  # remove first "/"

    @classmethod
    def from_crawler(cls, crawler, uri):
        return cls(
            uri,
            crawler.settings["GCS_PROJECT_ID"] or None,
            crawler.settings["FEED_STORAGE_GCS_ACL"] or None,
            crawler.settings["GCP_CREDENTIALS"] or None,
        )

    def _store_in_thread(self, file):
        file.seek(0)
        from google.cloud.storage import Client

        client = Client(project=self.project_id, credentials=self.credentials)
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)
        blob.upload_from_file(file, predefined_acl=self.acl)
