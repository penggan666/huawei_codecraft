import os
import numpy as np
import configparser
import math
import random

class demandObject:
    def __init__(self,clock,name,demand):
        self.clock = clock
        self.name = name
        self.demand =demand

class demandBucket:
    def __init__(self,sortList,minValue,maxValue):
        self.maxValue = maxValue
        self.minValue = minValue
        self.sortList = sortList
    #对bucket重新进行排序，并重新确定最小值和最大值
    def reSort(self):
        self.sortList = sorted(self.sortList,key=lambda x:x.demand,reverse=True)
        self.maxValue = self.sortList[0].demand
        self.minValue = self.sortList[len(self.sortList)-1].demand
    # def InfoPrint(self):
    #     print(self.minValue,' ',self.maxValue)
    #     for i in range(0,len(self.sortList)):
    #         print(i," ",self.sortList[i].demand)

class serverObject:
    def __init__(self,name,bandWidth):
        self.bandWidth = bandWidth
        self.name = name
    # def InfoPrint(self):
    #     print(self.name,":",self.connectNum)

def sortDemandInfoIntoBucket(totalList,bucketNum):
    res = []
    sortdemandInfo = sorted(totalList, key=lambda x: x.demand, reverse=True)
    numInbucket = int(len(totalList)/bucketNum)
    for i in range(0,bucketNum-1):
        onebucket=[]
        for j in range(0,numInbucket):
            index = i*numInbucket+j
            if j==0:
                maxValue = sortdemandInfo[index].demand
            if j==numInbucket-1:
                minValue = sortdemandInfo[index].demand
            onebucket.append(sortdemandInfo[index])
        res.append(demandBucket(onebucket.copy(),minValue,maxValue))
    maxValue = sortdemandInfo[(bucketNum-1)*numInbucket].demand
    minValue = sortdemandInfo[len(sortdemandInfo)-1].demand
    onebucket=[]
    for i in range((bucketNum-1)*numInbucket,len(sortdemandInfo)):
        onebucket.append(sortdemandInfo[i])
    res.append(demandBucket(onebucket.copy(),minValue,maxValue))
    return res

#插入值到某个桶中
def insertIntoBucket(clientInfo,bucketList):
    bucketListLen = len(bucketList)
    #如果demand比所有的最大值还大，就插入到第一个bucket
    if clientInfo.demand>=bucketList[0].maxValue:
        bucketList[0].sortList.append(clientInfo)
        bucketList[0].reSort()
        return
    #如果demand比所有的最小值还小，就插入到最后一个bucket
    if clientInfo.demand<bucketList[bucketListLen-1].minValue:
        bucketList[bucketListLen-1].sortList.append(clientInfo)
        bucketList[bucketListLen-1].reSort()
        return
    for i in range(0,len(bucketList)):
        #如果demand位于这个bucket的最小值和最大值之间
        #或者 demand位于上个bucket的最小值和这个bucket的最大值之间，统一插入到这个桶里面
        if (clientInfo.demand<=bucketList[i].maxValue and clientInfo.demand>=bucketList[i].minValue) or\
            (i>=1 and clientInfo.demand<bucketList[i-1].minValue and clientInfo.demand>bucketList[i].maxValue):
            bucketList[i].sortList.append(clientInfo)
            bucketList[i].reSort()
            return

#取出桶中的最大值
def getMaxDemand(demandBucketList):
    for i in range(0,len(demandBucketList)):
        if len(demandBucketList[i].sortList)>0:
            demandInfo = demandBucketList[i].sortList[0]
            demandBucketList[i].sortList.pop(0)
            #如果取完之后，这个桶的长度为0，那么就删除掉这个桶
            if len(demandBucketList[i].sortList)==0:
                demandBucketList.pop(i)
            #更改这个桶的最大值
            else:
                demandBucketList[i].maxValue = demandBucketList[i].sortList[0].demand
            return demandInfo

#判断是否有server溢出，如果有就返回一个server的name
def overFlowServer(cost,bandWidth):
    for server in cost:
        if cost[server] > bandWidth[server]:
            return server
    return ''

def averageValue(clock,clientname,demand,conServerList,output,cost,bandWidthDic):
    newconServerList = []
    for i in range(0,len(conServerList)):
        if cost[clock][conServerList[i].name]<bandWidthDic[conServerList[i].name]:
            newconServerList.append(conServerList[i])
    balanceWidth = int(demand / len(newconServerList))
    for i in range(0, len(newconServerList)):
        if i == len(newconServerList) - 1:
            balanceWidth = demand - (len(newconServerList) - 1) * balanceWidth
        curServer = newconServerList[i].name
        output[clock][clientname][curServer] += balanceWidth
        cost[clock][curServer] += balanceWidth

def sortConServerList(clock,conServerList,cost,output,bandWidthDic,serverConClient,originDemandInfo):
    for i in range(0,len(conServerList)):
        curServer = conServerList[i].name
        serverRemain = bandWidthDic[curServer]-cost[clock][curServer]

        # conClientList = serverConClient[curServer]
        # clientassign = 0
        # clientdemand = 0
        # #计算出所连通的client的已经分配的带宽
        # for j in range(0,len(conClientList)):
        #     clientName = conClientList[j]
        #     clientdemand += originDemandInfo[clock][clientName]
        #     clientOutput = output[clock][clientName]
        #     for key in clientOutput:
        #         clientassign+=clientOutput[key]
        # if clientdemand-clientassign<0:
        #     print(clientdemand-clientassign)

        conServerList[i].bandWidth = serverRemain
    finalconServerList=sorted(conServerList, key=lambda x: x.bandWidth, reverse=True)
    return finalconServerList

#方案分配
def assignBandwidth(demandBucketList,qosDic,bandWidthDic,is005,num005,cost,output,serverConClient,originDemandInfo):
    while(len(demandBucketList)!=0):
        demandInfo=getMaxDemand(demandBucketList)
        #极端server
        site_name=''
        #这个客户端连通的server
        conServerList=qosDic[demandInfo.name]
        clockIs005=is005[demandInfo.clock]
        #查看是否有极端边缘server
        for i in range(0,len(conServerList)):
            curServer = conServerList[i].name
            if clockIs005[curServer] and cost[demandInfo.clock][curServer]<bandWidthDic[curServer]:
                site_name=curServer
                break
        #没有就找一个极端边缘server
        if site_name=='':
            # preconServerList = conServerList
            conServerList=sortConServerList(demandInfo.clock, conServerList, cost, output, bandWidthDic, serverConClient,
                              originDemandInfo)
            for i in range(0,len(conServerList)):
                curServer = conServerList[i].name
                if num005[curServer]>0 and cost[demandInfo.clock][curServer]<bandWidthDic[curServer]:
                    site_name=curServer
                    is005[demandInfo.clock][curServer]=True
                    num005[curServer]-=1
                    break
        if site_name!='':
            if demandInfo.demand<=bandWidthDic[site_name]-cost[demandInfo.clock][site_name]:
                output[demandInfo.clock][demandInfo.name][site_name] += demandInfo.demand
                cost[demandInfo.clock][site_name] += demandInfo.demand
                demandInfo.demand = 0
            else:
                remainWidth = bandWidthDic[site_name]-cost[demandInfo.clock][site_name]
                output[demandInfo.clock][demandInfo.name][site_name] += remainWidth
                cost[demandInfo.clock][site_name] = bandWidthDic[site_name]
                demandInfo.demand -= remainWidth
                insertIntoBucket(demandInfo,demandBucketList)
        #没有极端节点可分，直接均分
        else:
            averageValue(demandInfo.clock,demandInfo.name,demandInfo.demand,conServerList,output,cost,bandWidthDic)
            #处理溢出
            ofServer = overFlowServer(cost[demandInfo.clock],bandWidthDic)
            while ofServer!='':
                randomChooseList = []
                conClientList=serverConClient[ofServer]
                for i in range(0,len(conClientList)):
                    randomChooseList.append(len(qosDic[conClientList[i]]))
                clientName = random.choices(conClientList,weights=randomChooseList,k=1)[0]
                overFlowValue = cost[demandInfo.clock][ofServer]-bandWidthDic[ofServer]
                returnValue = 0
                if output[demandInfo.clock][clientName][ofServer] >= overFlowValue:
                    output[demandInfo.clock][clientName][ofServer] -= overFlowValue
                    cost[demandInfo.clock][ofServer] -= overFlowValue
                    returnValue=overFlowValue
                else:
                    cost[demandInfo.clock][ofServer] -= output[demandInfo.clock][clientName][ofServer]
                    returnValue=output[demandInfo.clock][clientName][ofServer]
                    output[demandInfo.clock][clientName][ofServer] = 0
                averageValue(demandInfo.clock,clientName,returnValue,qosDic[clientName],output,cost,bandWidthDic)
                ofServer = overFlowServer(cost[demandInfo.clock],bandWidthDic)

# 对初次分配的结果进行优化
def optimizeResult(output,cost,serverList,clockNums,serverConclient,qosDic,bandWidthDic):
    up_limit = 0.95
    down_limit = 0.94
    up_bandWidth = {}
    down_bandWidth = {}
    sort_bandWidth = {}
    del_bandWidth = {}
    del1_bandWidth = {}
    prebandWidthSum = 0
    bandWidthSum = 1
    loopIndex = 0
    while prebandWidthSum!=bandWidthSum:
        for i in range(0,len(serverList)):
            sort_bandWidth[serverList[i]] = []
        for i in range(0,len(cost)):
            clockCost = cost[i]
            for server in clockCost:
                sort_bandWidth[server].append(clockCost[server])
        for key in sort_bandWidth:
            sort_bandWidth[key]=sorted(sort_bandWidth[key])
        up_index = math.ceil(clockNums*up_limit)-1
        down_index = math.ceil(clockNums*down_limit)-1
        for key in sort_bandWidth:
            up_bandWidth[key] = sort_bandWidth[key][up_index]
            down_bandWidth[key] = sort_bandWidth[key][down_index]
        prebandWidthSum = bandWidthSum
        bandWidthSum = 0
        for key in up_bandWidth:
            # del_bandWidth[key] = bandWidthDic[key]-sort_bandWidth[key][up_index+1]
            # del1_bandWidth[key] = sort_bandWidth[key][up_index]-sort_bandWidth[key][down_index]
            bandWidthSum+=up_bandWidth[key]
        # print(up_bandWidth)
        # print(del_bandWidth)
        # print(del1_bandWidth)
        # print(bandWidthSum)
        # print(down_bandWisdth)
        for i in range(0,len(cost)):
            clockCost = cost[i]
            for server in clockCost:
                if clockCost[server] > down_bandWidth[server] and clockCost[server] <= up_bandWidth[server]:
                    needReturnValue = clockCost[server] - down_bandWidth[server]
                    conClientList = serverConclient[server]
                    clientIndex = 0
                    maxOutput = 0
                    for k in range(0,len(conClientList)):
                        curClient = conClientList[k]
                        if output[i][curClient][server]>maxOutput:
                            maxOutput=output[i][curClient][server]
                            clientIndex=k
                    selClient = conClientList[clientIndex]
                    # selClient = conClientList[random.randint(0,len(conClientList)-1)]
                    needReturnValue = min(needReturnValue,output[i][selClient][server])
                    cost[i][server] -= needReturnValue
                    output[i][selClient][server] -= needReturnValue
                    conServerList = qosDic[selClient]

                    # 先尝试分配给up_limit以上的节点
                    if needReturnValue!=0:
                        for j in range (0,len(conServerList)):
                            if needReturnValue==0:
                                break
                            curServer = conServerList[j].name
                            if cost[i][curServer]>up_bandWidth[curServer]:
                                # 如果可以把退回的值全部分配给95以后的某个server
                                if cost[i][curServer]+needReturnValue<=bandWidthDic[curServer]:
                                    cost[i][curServer] += needReturnValue
                                    output[i][selClient][curServer] += needReturnValue
                                    needReturnValue = 0
                                else:
                                    output[i][selClient][curServer] += (bandWidthDic[curServer] - cost[i][curServer])
                                    needReturnValue -= (bandWidthDic[curServer] - cost[i][curServer])
                                    cost[i][curServer] = bandWidthDic[curServer]

                    # 再尝试分配给down_limit以下的节点
                    if needReturnValue!=0:
                        #对95以下的server进行均分
                        bandwidth95 = int(needReturnValue/len(conServerList))
                        for j in range (0,len(conServerList)):
                            curServer = conServerList[j].name
                            #然后处理down_limit以下
                            if cost[i][curServer]<down_bandWidth[curServer]:
                                if cost[i][curServer]+bandwidth95 < down_bandWidth[curServer]:
                                    cost[i][curServer] += bandwidth95
                                    output[i][selClient][curServer] += bandwidth95
                                    needReturnValue -= bandwidth95
                                else:
                                    output[i][selClient][curServer] += (down_bandWidth[curServer] - cost[i][curServer])
                                    needReturnValue -= (down_bandWidth[curServer] - cost[i][curServer])
                                    cost[i][curServer] += (down_bandWidth[curServer]-cost[i][curServer])
                        #处理均分完之后剩余的
                        for j in range (0,len(conServerList)):
                            if needReturnValue==0:
                                break
                            curServer = conServerList[j].name
                            #然后处理down_limit以下
                            if cost[i][curServer]<down_bandWidth[curServer]:
                                if cost[i][curServer]+needReturnValue < down_bandWidth[curServer]:
                                    cost[i][curServer] += needReturnValue
                                    output[i][selClient][curServer] += needReturnValue
                                    needReturnValue -= needReturnValue
                                else:
                                    output[i][selClient][curServer] += (down_bandWidth[curServer]-cost[i][curServer])
                                    needReturnValue -= (down_bandWidth[curServer]-cost[i][curServer])
                                    cost[i][curServer] += (down_bandWidth[curServer]-cost[i][curServer])
                    #如果还没有分配完，那就退回给原来的节点
                    if needReturnValue!=0:
                        # print(loopIndex," ",needReturnValue)
                        cost[i][server] += needReturnValue
                        output[i][selClient][server] += needReturnValue
        loopIndex+=1



def outputTotxt(output,outputPath):
    if os.path.exists(outputPath):
        os.remove(outputPath)
    for i in range(0,len(output)):
        clockOutPut = output[i]
        solution = ''
        for client in clockOutPut:
            strIndex = 0
            solution = solution + client + ':'
            for server in clockOutPut[client]:
                if clockOutPut[client][server]!=0:
                    if strIndex == 0:
                        solution = solution + '<' + server + ',' + str(clockOutPut[client][server]) + '>'
                        strIndex += 1
                    else:
                        solution = solution + ',' + '<' + server + ',' + str(clockOutPut[client][server]) + '>'
            solution = solution + '\n'
        if i==len(output)-1:
            solution = solution[0:len(solution)-1]
        with open(outputPath, 'a+') as f:
            f.write(solution)

# def testRight(cost,output,bandWidthDic,demandInfocopy):
#     #测试cost是否超过bandWidthDic
#     for i in range(0,len(cost)):
#         clockCost = cost[i]
#         for key in clockCost:
#             if clockCost[key]>bandWidthDic[key]:
#                 print('时刻',i,' ',key,':',clockCost[key],' ',bandWidthDic[key])
#     #测试output和cost应该一致
#     for i in range(0,len(cost)):
#         clockClient = output[i]
#         cost1 = cost[i].copy()
#         for client in clockClient:
#             for server in clockClient[client]:
#                 cost1[server]-=clockClient[client][server]
#         for key in cost1:
#             if cost1[key]!=0:
#                 print(key,cost1[key])
#     #测试分配与demand是否一致
#     index=0
#     for i in range(0,len(output)):
#         clockClient=output[i]
#         for client in clockClient:
#             sunAssign = 0
#             for server in clockClient[client]:
#                 sunAssign+=clockClient[client][server]
#             if demandInfocopy[index]!=sunAssign:
#                 print('error')
#             index+=1



if __name__ == '__main__':
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
    config.read(basedir+'/data_fake_small/config.ini')
    qos_constraint = config.getint('config','qos_constraint')
    demanInfoDir = basedir+'/data_fake_small/demand.csv'
    qosInfoDir = basedir+'/data_fake_small/qos.csv'
    bandwidthInfoDir = basedir+'/data_fake_small/site_bandwidth.csv'
    #读取demand文件，生成一个对所有client请求从大到小排序的的bucketList,桶的数量为100
    with open(demanInfoDir, encoding='utf-8') as f:
        demandInfo = np.loadtxt(demanInfoDir, dtype=str, delimiter=',')
        clientInfo = demandInfo[0:1,1:].tolist()[0]
        demandInfo = demandInfo[1:,1:]
        demandInfo = demandInfo.astype(int)
        # demandInfocopy=[]
        # for i in range(demandInfo.shape[0]):
        #     for j in range(demandInfo.shape[1]):
        #         demandInfocopy.append(demandInfo[i][j])
        originDemandInfo = []
        clockNums = demandInfo.shape[0]
        demandList = []
        for i in range(0,demandInfo.shape[0]):
            subDemandInfo = {}
            for j in range(0,demandInfo.shape[1]):
                subDemandInfo[clientInfo[j]] = demandInfo[i][j]
                demandList.append(demandObject(i,clientInfo[j],demandInfo[i][j]))
            originDemandInfo.append(subDemandInfo)
        demandBucketList = sortDemandInfoIntoBucket(demandList,100)

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
        qosInfo = qosInfo.T
        for i in range(0,len(clientList)):
            conServerList=[]
            for j in range(0,len(serverList)):
                if qosInfo[i][j]<qos_constraint:
                    conServerList.append(serverObject(serverList[j],bandWidthDic[serverList[j]]))
            qosDic[clientList[i]]=conServerList.copy()

    #is005,记录每个边缘节点的005
    #cost,记录每个时刻，每个server的分配值
    is005=[]
    cost=[]
    for i in range(0,clockNums):
        dic005={}
        costPart={}
        for j in range(0,len(serverList)):
            dic005[serverList[j]]=False
            costPart[serverList[j]]=0
        is005.append(dic005.copy())
        cost.append(costPart)

    #num005用来记录每个server可以被用来当作极端server的个数，也就是numberof005
    numberof005 = clockNums-math.ceil(clockNums*0.95)
    num005={}
    for i in range(0,len(serverList)):
        num005[serverList[i]]=numberof005

    #output用来记录每个时刻每个client和每个server的分配量
    output=[]
    for i in range(0,clockNums):
        clientOutPut={}
        for j in range(0,len(clientList)):
            serverOutput={}
            for k in range(0,len(serverList)):
                serverOutput[serverList[k]]=0
            clientOutPut[clientList[j]]=serverOutput.copy()
        output.append(clientOutPut.copy())
    # insertIntoBucket(demandObject(1,'A',130),demandBucketList)
    assignBandwidth(demandBucketList,qosDic,bandWidthDic,is005,num005,cost,output,serverConclient,originDemandInfo)
    optimizeResult(output,cost,serverList,clockNums,serverConclient,qosDic,bandWidthDic)
    outputTotxt(output,outputPath)

    # testRight(cost,output,bandWidthDic,demandInfocopy)