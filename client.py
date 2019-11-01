# -*- coding: utf-8 -*-
# @Time     : 2019/10/28 15:33
# @Author   : wangwei
# @FileName : client.py
# @Software : PyCharm
# @email    : wangwei@realtoptv.com
import json

from AcRBMQUtility.AcRBMQManager import AcRBMQManager, AcRBMQChannel, SubscribeUnit



class FullData: # 全推
    def __init__(self):
        self.marketFullName = ""
        self.products: list(FullDataProduct) = []

    def set_data(self, raw_data):
        self.marketFullName = raw_data.get("MarketFullName")
        product_list = raw_data.get("products")
        for product in product_list:
            p = FullDataProduct()
            p.set_data(product)
            self.products.append(p)

class FullDataProduct: # 全推
    def __init__(self):
        self.BuyPrices = []
        self.BuyVols = []
        self.SellPrices = []
        self.SellVols = []
        self.market = ""
        self.code = ""
        self.amount = 0
        self.open = 0.0
        self.close = 0.0
        self.high = 0.0
        self.low = 0.0
        self.productid = 0
        self.tickcount = 0
        self.time = ""
        self.vol = 0

    def set_data(self, raw_data):
        self.BuyPrices = raw_data.get("BuyPrices")
        self.BuyVols = raw_data.get("BuyVols")
        self.SellPrices = raw_data.get("SellPrices")
        self.SellVols = raw_data.get("SellVols")
        self.amount = raw_data.get("amount")
        self.close = raw_data.get("close")
        self.code = raw_data.get("code")
        self.high = raw_data.get("hight")
        self.low = raw_data.get("low")
        self.market = raw_data.get("market")
        self.open = raw_data.get("open")
        self.productid = raw_data.get("productid")
        self.tickcount = raw_data.get("tickcount")
        self.time = raw_data.get("time")
        self.vol = raw_data.get("vol")

class MonitorData:
    def __init__(self):
        pass


def delegate_全推(chan, method_frame, _header_frame, body, userdata=None):
    # 预处理数据的地方
    raw_data = json.loads(body, encoding='utf-8')
    data = FullDataProduct()
    data.set_data(raw_data)
    return data


def callback_全推(data):
    # 实际处理数据的地方
    if isinstance(data, FullDataProduct):
        print("全推")
        print(data)

def delegate_监控(chan, method_frame, _header_frame, body, userdata=None):
    # 预处理数据的地方
    return json.loads(body, encoding='utf-8')

def callback_监控(data):
    # 实际处理数据的地方
    print('监控')
    print(data)

def delegate_L2(chan, method_frame, _header_frame, body, userdata=None):
    # 预处理数据的地方
    return json.loads(body, encoding='utf-8')

def callback_L2(data):
    # 实际处理数据的地方
    print("L2")
    print(data)

def delegate_委托(chan, method_frame, _header_frame, body, userdata=None):
    return json.loads(body, encoding='utf-8')

def callback_委托(data):
    print("委托")
    for key, value in data.items():
        print(key, value)

def get_全推_server_name():
    return "AcMD.FullData"

def get_全推_subcribe_list(subscribe_list):
    return subscribe_list

def get_L2_server_name():
    return "AcMD.L2"

def get_委托_server_name():
    return "AcMD.Entrust"

def get_L2_subscribe_list(subscribe_list: list):
    "list = ['SZ000001', 'SH600001']"
    return subscribe_list

def get_委托_subscribe_list(subscribe_list: list):
    return subscribe_list

def get_监控_server_name():
    return "AcMD.Monitor"

def get_监控_subscribe_list(subscribe_list):
    return subscribe_list

def get_default_channel():
    channel = AcRBMQChannel()
    channel.Set_credentials(user="admin",
                            password="admin",
                            host="localhost",
                            port=5672,
                            vhost="/dhhf",
                            create_queue="1", # 这个为1则表示会自动创建队列，否则会使用已有的队列
                            queue="FullData133") # 这个队列名是自定义自己的队列的名字
    return channel

def get_全推_subscribe(subscribe_list, callback, delegate):
    server_name = get_全推_server_name()
    subscribe_list = get_全推_subcribe_list(subscribe_list)
    sub = SubscribeUnit()
    sub.AddSubscribe(server_name, subscribe_list, callback=callback, delegate=delegate)
    return sub

def get_监控_subscribe(subscribe_list, callback, delegate):
    server_name = get_监控_server_name()
    subscribe_list = get_监控_subscribe_list(subscribe_list)
    sub = SubscribeUnit()
    sub.AddSubscribe(server_name, subscribe_list, callback=callback, delegate=delegate)
    return sub

def get_L2_subscribe(subscribe_list, callback, delegate):
    server_name = get_L2_server_name()
    subscribe_list = get_L2_subscribe_list(subscribe_list)
    sub = SubscribeUnit()
    sub.AddSubscribe(server_name, subscribe_list, callback=callback, delegate=delegate)
    return sub

def get_委托_subscribe(subscribe_list, callback, delegate):
    server_name = get_委托_server_name()
    subscribe_list = get_委托_subscribe_list(subscribe_list)
    sub = SubscribeUnit()
    sub.AddSubscribe(server_name, subscribe_list, callback=callback, delegate=delegate)
    return sub

def get_default_consumer():
    consumer = AcRBMQManager.GetConsumer(get_default_channel())
    consumer.Connect()
    return consumer

def test_全推(subscribe_list, callback, delegate):
    consumer = get_default_consumer()
    consumer.Subscribe(
        get_全推_subscribe(
            subscribe_list=subscribe_list,
            callback=callback, # 这里是实际处理数据的地方
            delegate=delegate # 这里是预处理数据的地方
        )
    )
    consumer.begin_consume()

def test_监控(subscribe_list, callback, delegate):
    consumer = get_default_consumer()
    consumer.Subscribe(
        get_监控_subscribe(
            subscribe_list=subscribe_list,
            callback=callback, # 这里是实际处理数据的地方
            delegate=delegate # 这里是预处理数据的地方
        )
    )
    consumer.begin_consume()

def test_L2(subscribe_list, callback, delegate):
    consumer = get_default_consumer()
    consumer.Subscribe(
        get_L2_subscribe(
            subscribe_list=subscribe_list, # 这里给出了，你要订阅哪些订单
            callback=callback, # 这里是实际处理数据的地方
            delegate=delegate # 这里是预处理数据的地方
        )
    )
    consumer.begin_consume()

def test_委托(subscribe_list, callback, delegate):
    consumer = get_default_consumer()
    consumer.Subscribe(
        get_委托_subscribe(
            subscribe_list=subscribe_list,
            callback=callback,
            delegate=delegate
        )
    )
    consumer.begin_consume()




if __name__ == '__main__':
    kind = 0
    if kind == 0:
        # 修改"callback_全推" 可以自定义自己的全推数据处理地点
        # test_全推(["All"], callback=callback_全推, delegate=delegate_全推)
        test_全推(["SZ000001", "SZ000002"], callback=callback_全推, delegate=delegate_全推)
    elif kind == 1:
        # 修改"callback_监控" 可以自定义自己的监控数据处理地点
        test_监控(["All"], callback=callback_监控, delegate=delegate_监控)
        # test_全推(["SZ000001", "SZ300001"], callback=callback_监控, delegate=delegate_监控)
    elif kind == 2:
        # 修改"callback_L2" 可以自定义自己的L2数据处理地点
        test_L2(["All"], callback=callback_L2, delegate=delegate_L2)
        # test_全推(["SZ000001", "SZ300001"], callback=callback_L2, delegate=delegate_L2)
    elif kind == 3 or kind == 4:
        if kind == 3:
            # "需要所有委托数据"
            test_委托(["All"], callback=callback_委托, delegate=delegate_委托)
        else:
            # "需要指定的委托数据"
            test_委托(["ZS0", "SH17355", "ZS8578"], callback=callback_委托, delegate=delegate_委托)



