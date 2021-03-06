# -*- encoding: utf-8 -*-
'''
@File        :build_qa_database.py
@Time        :2020/12/07 19:48:28
@Author      :xiaoqifeng
@Version     :1.0
@Contact:    :unknown
'''

import os
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ProcessIntoES:
    def __init__(self):
        self._index = 'crime_data'
        self.es = Elasticsearch([{"host": "127.0.0.1", "port": 9220}])
        self.doc_type = "crime"
        cur = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.crime_file = os.path.join(cur, 'data/qa_corpus.json')
    
    def create_mapping(self):
        '''创建ES索引，确定分词类型，类似在建立数据库的时候先设置表的字段类型等等。
        '''
        node_mapping = {
            "mapping":{
                self.doc_type:{ # type
                    "properties":{
                        "questions":{    #field：问题
                            "type": "text", #lxw NOTE: cannot be string
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "index": "true" # The index option controls whether field values are indexed.
                        },
                        "answers":{    #field: 答案
                            "type": "text", #lxw NOTE: cannot be string 
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "index": "true" # The index option controls whether field values are indexed.
                        }
                    }
                }
            }
        }
        if not self.es.indices.exists(index=self._index):
            self.es.indices.create(index=self._index, body=node_mapping)
            print(f"Create {self._index} mapping successfully")
        else:
            print(f"index({self._index}) already exists.")

    def insert_data_bulk(self, action_list):
        '''批量插入数据
        '''
        success, _ = bulk(self.es, action_list, index=self._index, raise_on_error=True)
        print(f"Performed {success} actions._:{_}")


def init_ES():
    '''初始化ES，将数据插入到ES数据库中
    '''
    pie = ProcessIntoES()
    #创建ES的index
    pie.create_mapping()
    start_time = time.time()
    index = 0
    count = 0
    action_list = []
    BULK_COUNT = 1000 #每BULK_COUNT个句子一起插入到ES中

    for line in open(pie.crime_file):
        if not line:
            continue
        item = json.loads(line)
        index += 1
        action = {
            "_index": pie._index,
            "_type": pie.doc_type,
            "_source":{
                "question": item['question'],
                "answers": '\n'.join(item['anwers']),
            }
        }
        action_list.append(action)
        if index > BULK_COUNT:
            pie.insert_data_bulk(action_list=action_list)
            index = 0
            count += 1
            print(count)
            action_list = []
        end_time = time.time()

    print(f"Time Cost:{end_time-start_time}")


if __name__ == '__main__':
    # 将数据库插入到elasticsearch当中
    # init_ES()
    # 按照标题进行查询
    question = '我老公要起诉离婚 我不想离婚怎么办'