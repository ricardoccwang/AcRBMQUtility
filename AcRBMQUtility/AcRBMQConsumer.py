# -*- coding: utf-8 -*-
# @Time     : 2019/10/25 17:53
# @Author   : wangwei
# @FileName : AcRBMQConsumer.py.py
# @Software : PyCharm
# @email    : wangwei@realtoptv.com
from typing import Optional

from . AcRBMQBase import AcRBMQBase, SubscribeUnit
import threading
class _AcRBMQConsumer(threading.Thread):
    def __init__(self, mq):
        super(_AcRBMQConsumer, self).__init__()
        self._MQ_Channel = mq
        self._callback = None
    def set_callback(self, callback):
        self._callback = callback

    def Bind_Queue(self, queue, exchange, sub):
        self._MQ_Channel.Bind_Queue(queue=queue, exchange=exchange, routing_key=sub)

    def UnBind_Queue(self, queue, exchange, sub):
        self._MQ_Channel.UnBind_Queue(queue=queue, exchange=exchange, routing_key=sub)

    def run(self):
        print("callback = ", self._callback)
        self._MQ_Channel.start(self._callback)

    def Connect(self):
        self._MQ_Channel.Connect()

class AcRBMQConsumer(AcRBMQBase):
    def __init__(self, mq_channel):
        super(AcRBMQConsumer, self).__init__()
        self._consumer = _AcRBMQConsumer(mq_channel)
        self._subscribe: Optional[SubscribeUnit] = None

    def Connect(self):
        self._consumer.Connect()

    def begin_consume(self):
        self._consumer.start()

    def Subscribe(self, subscribe: SubscribeUnit):
        if isinstance(subscribe, SubscribeUnit) and self._subscribe is None:
            subs = subscribe.subscribes
            exchange = subscribe.exchange
            for sub in subs:
                self._consumer.Bind_Queue(None, exchange, sub)
            self._subscribe = subscribe
            self._consumer.set_callback(self._subscribe.callback)

    def Unsubscribe(self, subscribe: SubscribeUnit):
        if isinstance(subscribe, SubscribeUnit) and self._subscribe is not None:
            subs = subscribe.subscribes
            exchange = subscribe.exchange
            for sub in subs:
                self._consumer.UnBind_Queue(None, exchange, sub)

            self._subscribe = None
            self._consumer.set_callback(None)

