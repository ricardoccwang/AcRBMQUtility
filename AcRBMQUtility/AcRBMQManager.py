# -*- coding: utf-8 -*-
# @Time     : 2019/10/25 17:54
# @Author   : wangwei
# @FileName : AcRBMQManager.py
# @Software : PyCharm
# @email    : wangwei@realtoptv.com
from typing import Optional, List

import pika

from .AcRBMQConsumer import AcRBMQConsumer
from .AcRBMQPublisher import AcRBMQPublisher
from .AcRBMQBase import MQChannel, SubscribeUnit, ExchangeUnit
import random
import string
import collections


Default_exchange = [
    ExchangeUnit("AcMD.FullData"),
    ExchangeUnit("AcMD.Monitor"),
    ExchangeUnit("AcMD.L2")
    ]

class AcRBMQChannel(MQChannel):
    def __init__(self):
        super(AcRBMQChannel, self).__init__()
        self._user = "admin"
        self._password = "admin"
        self._host = "localhost"
        self._port: int = 5672
        self._vhost = "/"
        self._exchange_list: List[ExchangeUnit] = Default_exchange
        self._subscribe: SubscribeUnit = None
        self._channel_number = None
        self._queue = ""
        self._channel = None
        self._create_queue = True

    def Set_credentials(self, **kwargs):
        if "user" in kwargs:
            self._user = kwargs.get("user")
        if "password" in kwargs:
            self._password = kwargs.get("password")
        if "host" in kwargs:
            self._host = kwargs.get("host")
        if "port" in kwargs:
            self._port = kwargs.get("port")
        if "vhost" in kwargs:
            self._vhost = kwargs.get("vhost")
        if "channel_number" in kwargs:
            self._channel_number = kwargs.get("channel_number")
        if "queue" in kwargs:
            self._queue = kwargs.get("queue")
        if "create_queue" in kwargs:
            self._create_queue = (kwargs.get("create_queue") == "1")

    def Connect(self):
        credentials = pika.PlainCredentials(
            self._user, self._password, erase_on_connect=True)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, virtual_host=self._vhost,
                                      credentials=credentials, heartbeat=0, socket_timeout=5,
                                      )
        )
        self._channel = self.connection.channel(channel_number=self._channel_number)
        # self._PreDeclareExchange()
        self._PreDeclareQueue()

    def _PreDeclareExchange(self):
        '''
        初始化默认Exchange-我们称呼为服务者
        :return:
        '''
        for exchange_unit in self._exchange_list:
            self.Declare_Exchange(exchange_unit.exchange,
                                   exchange_type=exchange_unit.exchange_type,
                                   passive=exchange_unit.passive,
                                   durable=exchange_unit.durable,
                                   auto_delete=exchange_unit.auto_delete)

    def _PreDeclareQueue(self):
        '''
        初始化一个随机名字的队列
        :return:
        '''
        if self._queue == "":
            self._queue = ''.join(random.sample(string.ascii_letters + string.digits + string.punctuation, 32))
        if self._create_queue:
            self._Declare_Queue(self._queue)

    def Declare_Exchange(self, exchange,
                          exchange_type='direct',
                          passive=False,
                          durable=False,
                          auto_delete=True):
        self._channel.exchange_declare(exchange, exchange_type, passive, durable, auto_delete)

    def Bind_Queue(self, queue, exchange, routing_key):
        '''
        将客户端和服务绑定在一起
        :param queue:  视为客户端
        :param exchange: 视为服务者
        :param routing_key: 视为服务
        :return:
        '''
        if queue is None:
            queue = self._queue
        self._channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    def _Declare_Queue(self, queue, auto_delete=True, exclusive=True):
        if queue is None:
            queue = self._queue
        self.queue = self._channel.queue_declare(
            queue=queue, auto_delete=auto_delete, exclusive=exclusive).method.queue

    def UnBind_Queue(self, queue, exchange, sub_name):
        if queue is None:
            queue = self._queue
        self._channel.queue_unbind(queue=queue, exchange=exchange, routing_key=sub_name)

    def Close(self):
        self.connection.close()

    def channel(self):
        return self._channel

    def start(self, callback):
        self._channel.basic_consume(self._queue, callback, auto_ack=True)
        self._channel.start_consuming()


class AcRBMQManager():
    def __init__(self):
        super(AcRBMQManager, self).__init__()
        self._mq: Optional[MQChannel] = None

    def Set_MQ(self, mq: MQChannel):
        self._mq = mq

    def Set_Credential(self, **kwargs):
        '''
        kwargs 应该是参数的字典，而且必须是user='quanter' 这样的格式
        :param kwargs:
        :return:
        '''
        self._mq.set_credentials(**kwargs)

    @staticmethod
    def GetConsumer(channel: MQChannel):
        consumer = AcRBMQConsumer(channel)
        return consumer

    @staticmethod
    def GetConsumerRabbitMQ(host: str, port: int, user: str, password: str, vhost: str):
        channel = AcRBMQChannel()
        channel.Set_credentials(host=host, port=port, user=user, password=password, vhost=vhost)
        return AcRBMQManager.GetConsumer(channel)

    @staticmethod
    def GetPublisher(channel: MQChannel):
        publisher = AcRBMQPublisher(channel)
        return publisher

    @staticmethod
    def GetPublisherRabbitMQ(host: str, port: int, user: str, password: str, vhost: str):
        channel = AcRBMQChannel()
        channel.Set_credentials(host=host, port=port, user=user, password=password, vhost=vhost)
        return AcRBMQManager.GetPublisher(channel)
