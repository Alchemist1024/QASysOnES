# -*- encoding: utf-8 -*-
'''
@File        :crime_qa.py
@Time        :2020/12/07 21:12:29
@Author      :xiaoqifeng
@Version     :1.0
@Contact:    :unknown
'''

import os
import time
import json
from elasticsearch import Elasticsearch
import numpy as np


class CrimeQA:
    def __init__(self):
        self._index = "crime_data"
        self.es = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
        self.doc_type = "crime"
        cur = '/'.join(os.path.abspath(__file__).split('/')[:-1])

    def search_specific(self, val, key='question'):
        '''根据question进行事件的匹配查询
        '''
        query_body = {
            "query":{
                "match":{
                    key: val,
                }
            }
        }
        searched = self.es.search(index=self._index, doc_type=self.doc_type, body=query_body, size=20)
        #输出查询到的结果
        return searched['hits']['hits']

    def search_es(self, question):
        '''基于ES的问题查询
        '''
        answers = []
        res = self.search_specific(question)
        for hit in res:
            answer_dict = {}
            answer_dict['score'] = hit['_score']
            answer_dict['sim_question'] = hit['_source']['question']
            answer_dict['answers'] = hit['_source']['answers'].split('\n')
            answers.append(answer_dict)
        return answers

    def ranking(self, question, candidates):
        '''根据句向量的cos_sim对候选结果进行排序，返回最后结果
        '''
        pass

    def search_main(self, question):
        '''问答主函数
        '''
        pass

if __name__ == '__main__':
    handler = CrimeQA()
    while 1:
        question = input('question:')
        final_answer = handler.search_main(question)
        print('answers:', final_answer)