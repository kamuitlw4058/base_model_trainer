#!/usr/bin/env python
# -*- coding: utf-8 -*-
import queue as Queue
import threading
import time

Thread_id = 1
Thread_num = 3
class queue_thread(threading.Thread):
    def __init__(self, q,hander):
        global Thread_id
        threading.Thread.__init__(self)
        self.q = q
        self.Thread_id = Thread_id
        self._hander = hander
        Thread_id = Thread_id + 1
        self._is_stop =False

    def run(self):
        while not self._is_stop:
            try:
                data = self.q.get(block = True, timeout = 1) #不设置阻塞的话会一直去尝试获取资源
            except Queue.Empty:
                #print(f'Thread {self.Thread_id} get none data!')
                continue
            #取到数据，开始处理（依据需求加处理代码）
            #print(f"queue size:{self.q.qsize} " , self.Thread_id)
            self._hander(data)
            self.q.task_done()
            #print("Ending " , self.Thread_id)
    def stop(self):
        self._is_stop =True



class queue_thread_pool:
    def __init__(self,queue_size=200,thread_num=3,handler=None):
        self._queue = Queue.Queue(queue_size)
        self._thread_num = thread_num
        self._handler = handler
        self._works = []


    def put(self,value):
        self._queue.put(value)

    def start(self):
        for i in range(0, self._thread_num):
            worker = queue_thread(self._queue,self._handler)
            worker.start()
            self._works.append(worker)

    def stop(self):
        for worker in self._works:
            worker.stop()

    def queue_size(self):
        return self._queue.qsize()



def sample_hander(x):
    print(x)

# pool =  queue_thread_pool(handler=sample_hander)
# pool.start()
#
# for i in range(100):
#     time.sleep(1)
#     pool.put("123")
#

