# -*- coding: utf-8 -*-
# @Time     : 2019/10/25 17:52
# @Author   : wangwei
# @FileName : AcRBMQPublisher.py
# @Software : PyCharm
# @email    : wangwei@realtoptv.com

from .AcRBMQBase import AcRBMQBase, MQChannel, SubscribeUnit, ExchangeUnit, QueueUnit




class AcRBMQPublisher(AcRBMQBase):
    def __init__(self, mq_channel: MQChannel):
        super(AcRBMQPublisher, self).__init__()
        self._MQ_Channel = mq_channel
        self._subscribe_list = []

    def Publish(self, message, subscribe: SubscribeUnit):
        subs = subscribe._subscribe_list;
        for sub in subs:
            self._MQ_Channel.channel().basic_publish(exchange=subscribe.exchange,
                                   routing_key=sub,
                                   body=message)

    def Connect(self):
        self._MQ_Channel.Connect()

    def Close(self):
        self._MQ_Channel.Close()

__all__ = ['AcRBMQPublisher']