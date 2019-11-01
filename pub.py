# -*- coding: utf-8 -*-
# @Time     : 2019/10/28 15:33
# @Author   : wangwei
# @FileName : pub.py.py
# @Software : PyCharm
# @email    : wangwei@realtoptv.com
import json

from AcRBMQUtility.AcRBMQManager import AcRBMQManager, AcRBMQChannel, SubscribeUnit


server_name_group ={}

credentials = {}

def get_default_channel():
    channel = AcRBMQChannel()
    channel.Set_credentials(**credentials)
    return channel

def get_全推_server_name():
    return server_name_group.get("全推")

def get_监控_server_name():
    return server_name_group.get("监控")

def get_L2_server_name():
    return server_name_group.get("L2")

def get_委托_server_name():
    return server_name_group.get("委托")

def get_全推_subscribe_list(subscribe_list):
    return subscribe_list

def get_监控_subscribe_list(subscribe_list):
    return subscribe_list

def get_L2_subscribe_list(subscribe_list: list):
    "list = ['SZ000001', 'SH600001']"
    return subscribe_list

def get_委托_subscribe_list(subscribe_list: list):
    "list = ['SZ000001', 'SH600001']"
    return subscribe_list

def get_default_publisher():
    publisher = AcRBMQManager.GetPublisher(get_default_channel())
    publisher.Connect()
    return publisher

def get_全推_subscribe(subscribe_list):
    return SubscribeUnit(
        exchange=get_全推_server_name(),
        subscribe_list=get_全推_subscribe_list(subscribe_list)
    )

def get_监控_subscribe(subscribe_list):
    return SubscribeUnit(
        exchange=get_监控_server_name(),
        subscribe_list=get_监控_subscribe_list(subscribe_list)
    )

def get_L2_subscribe(subscribe_list):
    return SubscribeUnit(
        exchange=get_L2_server_name(),
        subscribe_list=get_L2_subscribe_list(subscribe_list)
    )

def get_委托_subscribe(subscribe_list):
    return SubscribeUnit(
        exchange=get_委托_server_name(),
        subscribe_list=get_委托_subscribe_list(subscribe_list)
    )

def get_json(data):
    return json.dumps(data)

if __name__ == '__main__':
    server_name_group = {
        "全推": "AcMD.FullData",
        "监控": "AcMD_Monitor",
        "L2": "AcMD.L2",
        "委托": "AcMD_Entrust"
    }
    credentials = {
        "user": "admin",
        "password": "admin",
        "host": "localhost",
        "port": 5672,
        "vhost": "/dhhf"
    }
    kind = 0
    data = {"word": "Hello"}
    subscribe_list = ["SZ000001", "SZ000002"]
    # subscribe_list = ["All"]
    if kind == 0:
        sub = get_全推_subscribe(subscribe_list)
    elif kind == 1:
        sub = get_监控_subscribe(subscribe_list)
    elif kind == 2:
        sub = get_L2_subscribe(subscribe_list)
    elif kind == 3:
        sub = get_委托_subscribe(subscribe_list)
    else:
        raise NotImplementedError()
    publisher = get_default_publisher()
    publisher.Publish(get_json(data), sub)
    publisher.Close()