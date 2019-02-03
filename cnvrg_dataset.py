import requests
import os
import threading
import netrc
import yaml
from os.path import expanduser
from concurrent.futures import ThreadPoolExecutor
class CnvrgDataset:
    def __init__(self, dataset: str, token: str = None, owner: str = None, path: str = None, threads:int = 4, chunk_size:int = 1000, timeout: int = 3600, api: str = None):
        """
        Creates cnvrg dataset class
        :param token: api_token for the user, (if empty: taken from ~/.netrc)
        :param dataset: dataset id
        :param owner: owner's username (if empty: taken from ~/.cnvrg/config.yml)
        :param path: path to download the dataset's files to
        :param threads: max amount of active threads
        :param chunk_size: the size for each call
        :param timeout: max amount of time to wait for file to be downloaded
        :param api: cnvrg api endpoint (if empty: taken from ~/.cnvrg/config.yml)
        """
        self.home = expanduser("~")
        self.__init_local_vars()
        self.__dataset = dataset
        self.__token = token or self.__token
        self.__owner = owner or self.__owner
        self.__api = api or self.__api
        self.__base_url = "{0}/v1/users/{1}/datasets/{2}".format(self.__api, self.__owner, dataset)
        sess = requests.Session()
        sess.headers.update({'Auth-Token': self.__token, 'Content-Type': 'application/json'})
        self.__session = sess
        self.__number_of_files = None
        self.__threads = threads
        self.__path = path or "/data/{0}".format(dataset)
        self.__files_count = 0
        self.__offset = 0
        self.__chunk_size = chunk_size
        self.__commit_sha1 = None
        self.__files = []
        self.__downloaded = 0
        self.__timeout = timeout



    def download_dataset(self, commit_sha1: str = None, block: bool = False) -> threading.Thread:
        if not block:
            t = threading.Thread(target=self.download_dataset, args=(commit_sha1, True, ))
            t.start()
            return t
        self.__commit_sha1 = commit_sha1
        stats = self.__get_data_stats()
        self.__files_count = stats['commit_files']
        while self.__offset < self.__files_count:
            self.__download_chunk()


    def progress(self) -> float:
        if(self.__files_count == 0):
            return 0.0
        return self.__downloaded / self.__files_count


    def __init_local_vars(self):
        with open(os.path.join(self.home, '.netrc'), 'r') as f:
            try:
                self.__token = netrc.netrc().authenticators("cnvrg.io")[2]
            except Exception as e:
                print(e)
        with open(os.path.join(self.home, '.cnvrg/config.yml'), 'r') as f:
            try:
                config = yaml.load(f)
                owner = config[':username']
                api = config[':api']
                self.__api = api
                self.__owner = owner
            except Exception as e:
                print(e)


    def __download_chunk(self):
        data = self.__session.get("{0}/files".format(self.__base_url), params={"limit": self.__chunk_size, "offset": self.__offset, "commit_sha1": self.__commit_sha1}).json()
        new_files = data['files']
        self.__offset += len(new_files)
        self.__files += new_files
        self.__download_files(new_files)


    def __download_files(self, files):
        with ThreadPoolExecutor(self.__threads) as executor:
            for file in files:
                executor.submit(self.__download_file, file)




    def __download_file(self, file_obj):
        r = self.__session.get(file_obj['url'])
        path = os.path.join(self.__path, file_obj['path'])
        fullpath = os.path.join(self.__path, file_obj['fullpath'])
        os.makedirs(path, exist_ok=True)
        open(fullpath, 'wb').write(r.content)
        self.__downloaded += 1

    def __get_data_stats(self):
        r = self.__session.get("{0}/files".format(self.__base_url), params={"limit": 0, "commit_sha1": self.__commit_sha1})
        return r.json()


## usage: CnvrgDataset(dataset='birot').download_all_files()
