# -*- coding: utf-8 -*-
# @Time     : 2019/10/28 11:45
# @Author   : wangwei
# @FileName : AcRBMQBase.py
# @Software : PyCharm
# @email    : wangwei@realtoptv.com
import abc
class AcRBMQBase:
    def __init__(self):
        pass

class MQChannel:

    @abc.abstractmethod
    def Set_credentials(self, **kwargs):
        pass

    @abc.abstractmethod
    def Connect(self):
        pass

    @abc.abstractmethod
    def Close(self):
        pass

    @abc.abstractmethod
    def Subscribe(self, subscribe):
        pass

    @abc.abstractmethod
    def Unsubscribe(self, **kwargs):
        pass

    @abc.abstractmethod
    def channel(self):
        pass

class ExchangeUnit:
    def __init__(self, exchange = "", exchange_type = "direct", passive = False, durable = True, auto_delete = False):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete


class QueueUnit:
    def __init__(self):
        self.queue = ""
        self.auto_delete = True
        self.exclusive = True


class SubscribeUnit:
    def __init__(self, exchange = "", subscribe_list = list(), delegate = None, callback = None):
        self._exchange = exchange
        self._callback = callback
        self._delegate = delegate
        self._subscribe_list = []
        for sub in subscribe_list:
            self.subscribes = sub

    def AddSubscribe(self, exchange, subscribe, callback = None, delegate = None):
        self._callback = self.wrap(callback, delegate)
        self._delegate = delegate
        self._exchange = exchange
        self._subscribe_list = subscribe

    @property
    def callback(self):
        return self._callback

    @property
    def subscribes(self):
        return self._subscribe_list

    @subscribes.setter
    def subscribes(self, subscribe):
        if subscribe not in self._subscribe_list:
            self._subscribe_list.append(subscribe)

    @property
    def exchange(self):
        return self._exchange

    @exchange.setter
    def exchange(self, exchange):
        self._exchange =exchange

    def wrap(self, callback, delegate):
        def w(*args, **kwargs):
            if callback is None:
                # 没有callback则不做任何事情
                return
            else:
                if delegate is None:
                    callback(*args, **kwargs)
                else:
                    callback(delegate(*args, **kwargs))

        return w
