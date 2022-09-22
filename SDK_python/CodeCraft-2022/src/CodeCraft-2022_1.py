import os
import numpy as np
import configparser
import time

class streamObject:
    def __init__(self,clientName,streamId,demand):
        self.clientName = clientName
        self.streamId = streamId
        self.demand = demand

class serverObject:
    def __init__(self,name,bandWidth):
        self.bandWidth = bandWidth
        self.name = name

#分配方案
#qosDic是每个client连接的server的一个list
#cost是现在
def assignBandwidth(demandInfoList,qosDic,serverList,clientList,outputPath):
    if os.path.exists(outputPath):
        os.remove(outputPath)
    sort_time=0
    max_time=0
    write_time=0
    for i in range(0,len(demandInfoList)):
        #定义这个时刻的cost
        cost = {}
        for j in range(0,len(serverList)):
            cost[serverList[j]]=0
        #定义这个时刻的output
        output = {}
        for j in range(0,len(clientList)):
            conServerList = qosDic[clientList[j]]
            server_stream_dic={}
            for k in range(0,len(conServerList)):
                server_stream_dic[conServerList[k].name]=[]
            output[clientList[j]]=server_stream_dic.copy()
        time_start = time.time()
        clockDemandList = sorted(demandInfoList[i],key=lambda x: x.demand, reverse=True)
        time_end = time.time()
        sort_time += (time_end-time_start)

        time_start = time.time()
        for j in range(0,len(clockDemandList)):
            clientDemand = clockDemandList[j]
            conServerList = qosDic[clientDemand.clientName]
            maxServer = None
            #找到剩余最多带宽的server
            for k in range(0,len(conServerList)):
                serverBand = conServerList[k].bandWidth
                costBand = cost[conServerList[k].name]
                remainBand = serverBand-costBand
                if remainBand>clientDemand.demand:
                    maxServer = conServerList[k]
                    break
            if maxServer==None:
                print('不够分配')
            if maxServer!=None:
                cost[maxServer.name] += clientDemand.demand
                output[clientDemand.clientName][maxServer.name].append(clientDemand.streamId)
                clientDemand.demand = 0
        time_end = time.time()
        max_time += (time_end-time_start)

        time_start = time.time()
        outputTotxt(i,serverList,output,outputPath)
        time_end = time.time()
        write_time += (time_end-time_start)
    print(sort_time)
    print(max_time)
    print(write_time)


def outputTotxt(i,serverList,output,outputPath):
    solution = ''
    if i==0:
        for j in range(0,10):
            solution += serverList[j]
            if j!=9:
                solution += ','
            else:
                solution += '\n'
    for client in output:
        clientServer = output[client]
        solution = solution + client + ':'
        serverFirst = 1
        for server in clientServer:
            streamList = clientServer[server]
            if len(streamList)==0:
                continue
            if serverFirst==1:
                solution = solution + '<' + server
                serverFirst = 0
            else:
                solution = solution + ',<' + server
            for i in range(0,len(streamList)):
                if i!= len(streamList)-1:
                    solution = solution + ','+streamList[i]
                else:
                    solution = solution + ','+streamList[i] + '>'
        solution = solution + '\n'
    with open(outputPath, 'a+') as f:
        f.write(solution)


if __name__ == '__main__':
    time_start = time.time()
    '''
    clientList:客户端列表
    serverList:服务器列表
    demandInfo:矩阵
    qosInfo:矩阵
    bandWidthInfo:矩阵
    '''
    basedir = '/home/ubuntu/penggan/PycharmProjects/huawei'
    # basedir = ''
    outputPath = basedir+'/output/solution.txt'
    config = configparser.ConfigParser()
    config.read(basedir+'/data_f/config.ini')
    qos_constraint = config.getint('config','qos_constraint')
    demanInfoDir = basedir+'/data_f/demand.csv'
    qosInfoDir = basedir+'/data_f/qos.csv'
    bandwidthInfoDir = basedir+'/data_f/site_bandwidth.csv'
    #读取demand文件，生成一个对所有client请求从大到小排序的的bucketList,桶的数量为100
    with open(demanInfoDir, encoding='utf-8') as f:
        demandInfo = np.loadtxt(demanInfoDir, dtype=str, delimiter=',')
        clientInfo = demandInfo[0:1,2:].tolist()[0]
        demandInfo = demandInfo[1:,:]
        demandInfoList = []

        clockDemandList=[]

        for i in range(0,demandInfo.shape[0]):
            stream_id=demandInfo[i][1]
            #如果这个时刻和上个时刻不同,就新建一个
            if i>=1 and demandInfo[i-1][0]!=demandInfo[i][0]:
                demandInfoList.append(clockDemandList.copy())
                clockDemandList = []
            for j in range(2,demandInfo.shape[1]):
                clockDemandList.append(streamObject(clientInfo[j-2],stream_id,int(demandInfo[i][j])))
            if i==demandInfo.shape[0]-1:
                demandInfoList.append(clockDemandList.copy())
        clockNums = len(demandInfoList)

    #读取bandwidth文件，生成一个字典{server:band_width}
    with open(bandwidthInfoDir, encoding='utf-8') as f:
        bandWidthDic={}
        bandwidthInfo = np.loadtxt(bandwidthInfoDir, dtype=str, delimiter=',', skiprows=1)
        for i in range(0,bandwidthInfo.shape[0]):
            bandWidthDic[bandwidthInfo[i][0]]=int(bandwidthInfo[i][1])

    #读取qos文件，生成一个字典 {client:[conServerList]}
    with open(qosInfoDir, encoding='utf-8') as f:
        qosInfo = np.loadtxt(qosInfoDir, dtype=str, delimiter=',')
        clientList = qosInfo[0][1:].tolist()
        serverList = qosInfo[1:,0:1].T.tolist()[0]
        qosInfo = qosInfo[1:,1:]
        qosInfo = qosInfo.astype(int)
        #求出每个server可以连通的client的数量
        serverConDic={}
        serverConclient={}
        for i in range(0,len(serverList)):
            connectClienNum = 0
            connectClienList = []
            for j in range(0,len(clientList)):
                if qosInfo[i][j]<qos_constraint:
                    connectClienNum+=1
                    connectClienList.append(clientList[j])
            serverConDic[serverList[i]]=connectClienNum
            serverConclient[serverList[i]]=connectClienList
        qosDic={}
        qosServerRemainDic={}
        qosInfo = qosInfo.T
        for i in range(0,len(clientList)):
            conServerList=[]
            for j in range(0,len(serverList)):
                if qosInfo[i][j]<qos_constraint:
                    conServerList.append(serverObject(serverList[j],bandWidthDic[serverList[j]]))
            qosDic[clientList[i]]=sorted(conServerList, key=lambda x: x.bandWidth, reverse=True)

    assignBandwidth(demandInfoList,qosDic,serverList,clientList,outputPath)
    time_end = time.time()
    print('time_cost1', time_end - time_start, 's')
