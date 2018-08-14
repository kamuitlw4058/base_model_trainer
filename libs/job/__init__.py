from libs.collection.utils import dict_merge
from libs.collection import AttributeDict
import json


class Job(AttributeDict):
    def to_file(self, filename):
        f = open(filename, 'w', encoding='utf-8')
        json.dump(self, f, ensure_ascii=False)

    def from_file(self, filename):
        f = open(filename, 'r', encoding='utf-8')
        bak = json.load(f)
        merged = dict_merge(bak, self)
        self.__init__(merged)
