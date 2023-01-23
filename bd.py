import grpc
from concurrent import futures
import time
import unary_pb2_grpc as pb2_grpc
import unary_pb2 as pb2
import interface as I
from threading import Thread
import json
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse, ParseDict
import plyvel
from pysyncobj import SyncObj, replicated
import socket

import sys
import threading
import json

databaseCID = dict()
databasePID = dict()
databaseOID = dict()
nextOID = dict()
I.fillDatabasePID(databasePID)
I.fillDatabaseCID(databaseCID)
I.fillDatabaseOID(databaseOID)

######################## MOSQUITTO PUB
import random
import time

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'emqx'
password = 'public'

class Replica(SyncObj):

    def __init__(self, port, lider, parceiras):
        super(Replica, self).__init__(lider, parceiras)
        self.dbUsers = './dbUser/' + str(port) + '/'
        self.dbProducts = './dbProd/' + str(port) + '/'
        self.dbOrders = './dbOrder/' + str(port) + '/'
        
    @replicated
    def createUser(self, key, value):
        db = plyvel.DB(self.dbUsers, create_if_missing=True)
        key = bytes(key, 'utf-8')
        value = MessageToJson(value.user)
        value = bytes(value,'utf-8')
        db.put(key, value)
        db.close()
    
    def hasUser(self, key):
        db = plyvel.DB(self.dbUsers, create_if_missing=True)
        key = bytes(key, 'utf-8')
        ok = db.get(key) != None
        db.close()
        return ok == True
    
    def getUser(self, key):
        db = plyvel.DB(self.dbUsers, create_if_missing=True)
        key = bytes(key, 'utf-8')
        user = json.loads(db.get(key).decode())
        db.close()
        user = Parse(json.dumps(user), pb2.User())
        return user
    
    @replicated
    def removeUser(self, key):
        db = plyvel.DB(self.dbUsers, create_if_missing=True)
        key = bytes(key, 'utf-8')
        db.delete(key) #todo: nao sei se da problema se o db nao tiver a chave, testar isso
        db.close()
    
    @replicated
    def updateUser(self, key, value):
        db = plyvel.DB(self.dbUsers, create_if_missing=True)
        key = bytes(key, 'utf-8')
        value = MessageToJson(value.user)
        value = bytes(value,'utf-8')
        db.put(key, value) #nao sei se subtitui a chave já existente no banco de dados ou se precisa deletar antes e inserir uma nova
        db.close()
        
    def getAllProducts(self): #pega todos os produtos
        db = plyvel.DB(self.dbProducts, create_if_missing=True)
        products = []
        for key, value in db:
            product = json.loads(value.decode())
            products.append(Parse(json.dumps(product), pb2.Product()))
        db.close()
        return products
    
    def getProduct(self, key):
        db = plyvel.DB(self.dbProducts, create_if_missing=True)
        key = bytes(key, 'utf-8')
        product = json.loads(db.get(key).decode())
        db.close()
        product = Parse(json.dumps(product), pb2.Product())
        return product

    
    @replicated
    def createProduct(self, key, value):
        db = plyvel.DB(self.dbProducts, create_if_missing=True)
        key = bytes(key, 'utf-8')
        value = MessageToJson(value)
        value = bytes(value,'utf-8')
        db.put(key, value)
        db.close()

    @replicated
    def updateProduct(self, key, value):
        db = plyvel.DB(self.dbProducts, create_if_missing=True)
        key = bytes(key, 'utf-8')
        value = MessageToJson(value)
        value = bytes(value,'utf-8')
        db.put(key, value)
        db.close()

        
    def hasProduct(self, key):
        db = plyvel.DB(self.dbProducts, create_if_missing=True)
        key = bytes(key, 'utf-8')
        ok = db.get(key) != None
        db.close()
        return ok == True
    
    @replicated
    def removeProduct(self, key):
        db = plyvel.DB(self.dbProducts, create_if_missing=True)
        key = bytes(key, 'utf-8')
        db.delete(key) #todo: nao sei se da problema se o db nao tiver a chave, testar isso
        db.close()
        
    def getOrderes(self, cid):
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        orderes = []
        for key, value in db:
            if key.decode() == "currentOid":
                continue
            CreateOrder = json.loads(value.decode())
            CreateOrder = Parse(json.dumps(CreateOrder), pb2.CreateOrder())
            if CreateOrder.cid.cid == cid:
                orderes.append(I.newArrayOrderes(I.newOrderOID(key.decode()), CreateOrder.cid, CreateOrder.orderProduct))
        db.close()
        return orderes
    
    def getOrder(self, key):
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        key = bytes(key, 'utf-8')
        order = db.get(key)
        CreateOrder = json.loads(order.decode())
        CreateOrder = Parse(json.dumps(CreateOrder), pb2.CreateOrder())
        db.close()
        return CreateOrder
    
    def updateOrder(self, key, value):
        order = replica.getOrder(key)
        orderesProducts = []
        for idx in range(len(order.orderProduct)):
            if order.orderProduct[idx].pid.pid == value.pid.pid:
                dif = order.orderProduct[idx].quantity - value.quantity
                order.orderProduct[idx].quantity = value.quantity
                flag = self.setNewQuantityProduct(order.orderProduct[idx].pid.pid, dif)
                if flag == 0:
                    return 0
            if order.orderProduct[idx].quantity != 0:
                orderesProducts.append(order.orderProduct[idx])
        
        order = I.newCreateOrder(order.cid, orderesProducts)
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        key = bytes(key, 'utf-8')
        order = MessageToJson(order)
        order = bytes(order,'utf-8')
        db.put(key, order)
        db.close()
        return 1
        
        
    def setNewQuantityProduct(self, key, value):
        prod = self.getProduct(key)
        if prod.stock + value < 0:
            return 0
        prod.stock += value
        self.updateProduct(key, prod)
        return 1
        
    @replicated
    def createOrder(self, key, value):
        orderesProducts = value.orderProduct
        for orderProduct in orderesProducts:
            qtd = orderProduct.quantity
            flag = self.setNewQuantityProduct(orderProduct.pid.pid, -qtd)
            if flag == 0:
                return False
        value = MessageToJson(value)
        value = bytes(value, 'utf-8')
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        key = bytes(str(key), 'utf-8')
        db.put(key, value)
        db.close()
        return True
    
    def hasOrder(self, key):
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        key = bytes(str(key), 'utf-8')
        ok = db.get(key) != None
        db.close()
        return ok
    
    @replicated
    def cancelOrder(self, key):
        if self.hasOrder(key) == False:
            return False
        order = self.getOrder(key)
        for orderProduct in order.orderProduct:
            flag = self.setNewQuantityProduct(orderProduct.pid.pid, orderProduct.quantity)
            if flag == 0:
                return False
        
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        key = bytes(str(key), 'utf-8')
        db.delete(key)
        db.close()
        return True
    
    def getCurrentOid(self):
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        key = "currentOid"
        key = bytes(key, 'utf-8')
        if db.get(key) != None:
            oid = db.get(key)
            oid = int(oid.decode())
        else:
            oid = 0
        db.close()
        return oid
    
    @replicated
    def setCurrentOid(self, oid):
        db = plyvel.DB(self.dbOrders, create_if_missing=True)
        oid = bytes(str(oid), 'utf-8')
        key = "currentOid"
        key = bytes(key, 'utf-8')
        db.put(key, oid)
        db.close()
        return oid
        
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, I.PORT_MQTT)
    return client

def sendOnTopic(topic, msg):
    client = connect_mqtt()
    client.loop_start()
    for i in range(3):
        result = client.publish(topic, msg)
        status = result[0]
        if status == 0:
            print(f"{msg} enviada com sucesso no topico {topic}")
            break
    client.disconnect()
        
###############################

class UnaryClient(object):
    """
    Client for gRPC functionality
    """

    def __init__(self, port):
        self.host = 'localhost'
        self.server_port = port

        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        self.stub = pb2_grpc.UnaryStub(self.channel)

    def createUser(self, user):
        return self.stub.GetServerResponseCreateUser(user)

class UnaryService(pb2_grpc.UnaryServicer):
    def __init__(self, *args, **kwargs):
        pass

    def GetServerResponseCreateUser(self, request, context): #feito
        print(f'BD recebeu {request}\n')
        cid = request.cid.cid
        if replica.hasUser(cid) == True:
            print('Erro: Cliente já existente')
            return I.newOk(0) #Erro
        replica.createUser(cid, request)
        return I.newOk(1) #usuario criado com sucesso
    
    def GetServerResponseGetUser(self, request, context): 
        print(f"entrou na getUser BD")
        ci = request.cid
        user = replica.getUser(ci)
        return I.newGetUser(request, user) #usuario encontrado com sucesso
    
    def GetServerResponseLoginUser(self, request, context): 
        print(f"Login => BD recebeu {request}")
        cid = request.cid.cid
        if replica.hasUser(cid) == True:
            user = replica.getUser(cid)
            if user.password == request.password:
                print(f"Dados do usuario batem com o banco de dados, login feito com sucesso")
                return I.newOk(1) #dados informados batem com o banco de dados
        return I.newOk(0) #nao ha cadastro correspondente
    
    def GetServerResponseRemoveUser(self, request, context): 
        cid = request.cid
        if replica.hasUser(cid) == False:
            return I.newOk(0) #usuario nao encontrado
        toSend = I.newRemoveCacheUser(request)
        user_update_json = MessageToJson(toSend)
        sendOnTopic(I.TOPIC, user_update_json)
        replica.removeUser(cid)
        #listarClientes()
        return I.newOk(1) #usuario removido
    
    def GetServerResponseRecoverUser(self, request, context): #feito, falta testar
        print(f"entrou na recoverUser BD")
        cid = request.cid
        if replica.hasUser(cid) == False:
            return I.newUser("-1", "-1", "-1", "-1", "-1")
        return replica.getUser(cid)

    def GetServerResponseUpdateUser(self, request, context): #feito, falta testar
        cid = request.cid.cid
        if replica.hasUser(cid) == False:
            return I.newOk(0) #usuario nao encontrado
        toSend = I.newUpdateCacheUser(request)
        user_update_json = MessageToJson(toSend)
        sendOnTopic(I.TOPIC, user_update_json)
        replica.updateUser(cid, I.newCreateUser(request.cid, request.user))
        return I.newOk(1) #cadastrado modificado
    
    def GetServerResponseListProducts(self, request, context):
        products = replica.getAllProducts()
        return I.newArrayProducts(products)

    def GetServerResponseListOrderes(self, request, context):
        cid = request.cid
        orderes = replica.getOrderes(request.cid)
        
        return I.newResultListOrderes(orderes)
    
    def GetServerResponseProduct(self, request, context):
        pid = request.pid
        if replica.hasProduct(pid) == False:
            return I.newProduct(I.newProductPID("-1"), "", 0, 0)
        
        product = replica.getProduct(pid)
        return product

    def GetServerResponseCreateOrder(self, request, context):
        oid = replica.getCurrentOid()
        oid += 1
        ok = replica.createOrder(oid, request)
        replica.setCurrentOid(oid)
        return I.newOk(1)
    
    def GetServerResponseCancelOrder(self, request, context):
        oid = request.oid.oid
        if replica.hasOrder(oid) == False:
            return I.newOk(0) #pedido nao existe
        replica.cancelOrder(oid)
        return I.newOk(1) #pedido cancelado com sucesso
    
    def GetServerResponseCreateProduct(self, request, context):
        pid = request.pid.pid
        if replica.hasProduct(pid) == True:
            return I.newOk(0) #produto ja existe
        replica.createProduct(pid, request.product)
        return I.newOk(1) #produto criado com sucesso
    
    def GetServerResponseRemoveProduct(self, request, context):
        #print(f"chegou aq no bd, request = {request}")
        pid = request.pid.pid
        if replica.hasProduct(pid) == False:
            return I.newOk(0) #pedido nao existe
        toSend = I.newRemoveCacheProduct(request)
        user_update_json = MessageToJson(toSend)
        sendOnTopic(I.TOPIC, user_update_json)
        replica.removeProduct(pid)
        return I.newOk(1) #produto removido com sucesso
    
    def GetServerResponseUpdateProduct(self, request, context):
        pid = request.pid.pid
        product = request.product
        if replica.hasProduct(pid) == False:
            return I.newOk(0) #produto nao existe
        toSend = I.newUpdateCacheProduct(request)
        user_update_json = MessageToJson(toSend)
        sendOnTopic(I.TOPIC, user_update_json)
        replica.updateProduct(pid, product)
        return I.newOk(1) #produto atualizado com sucesso
    
    def GetServerResponseRecoverProduct(self, request, context):
        pid = request.pid
        if replica.hasProduct(pid) == False:
            return I.newProduct(I.newProductPID("-1"), "-1", -1, -1)
        return replica.getProduct(pid)
    
    def GetServerResponseUpdateOrder(self, request, context):
        oid = request.oid.oid
        if replica.hasOrder(oid) == False:
            return I.newOk(0)
        ok = replica.updateOrder(oid, request)
        return I.newOk(ok)

def runUnary(port): #cria server grpc unario
    server = I.createServer(UnaryService(), I.MAX_WORKERS, port)  

# funçoes para fins de teste
def listarClientes():
    print('Clients: ', end="{")
    for client in databaseCID.items():
        print(client, end=" , ")
    print('}')

def listarProdutos():
    print('Produtos: ', end="{")
    for product in databasePID.items():
        print(product, end=" , ")
    print('}')

def listarPedidos():
    print('Pedidos: ', end="{")
    for order in databaseOID.items():
        print(order, end=" , ")
    print('}')
    
###########################
# main

def getReplica(port):
    listPartners = []
    for x in I.PORTAS_REPLICAS:
        if x != port:
            listPartners.append('localhost:' + str(x))
    replica = Replica(port, 'localhost:'+str(port), listPartners)
    print(f"porta = {port}")
    print(f"Lista Parceiros = {listPartners}")
    return replica
        
if __name__ == '__main__':
    port = int(input("Digite porta GRPC: "))
    port_replica = int(input("Digite porta replica: "))
    global replica
    replica = getReplica(port_replica)
    runUnary(port)