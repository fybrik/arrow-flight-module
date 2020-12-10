import urllib.request as urllib2
from pyarrow.fs import FileSystem, FileInfo, FileType

def rest_filesystem_from_config(rest_config):
    url = rest_config.get('url')
    return RestFileSystem(url)

class RestFileSystem(FileSystem):
    def __init__(self, url):
        self.url = url

    def get_file_info(self, url):
        f = urllib2.urlopen(self.url)
        print("QQQ " + str([FileInfo(self.url, FileType.File, size=f.length)]))
        return [FileInfo(self.url, FileType.Unknown, size=f.length)]

    def normalize_path(self, url):
        pass
