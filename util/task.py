from os import path
import os
import json
import ioutil
import fcntl
import logging
import dateutil

logger = logging.getLogger("Task")

class Task:
    '''
    group：如果存在分组那么只有所有的任务都已经完成后，这个任务才会任务都已经完成
    status:STATUS_WAITING,STATUS_FAILED,STATUS_RUNNING,STATUS_SUCCESS,STATUS_PIGEONHOLE
    '''

    STATUS_WAITING = 0 # 失败状态，在最大失败次数之前将会自动再词运行，前提是需要等待大致5秒的时间
    STATUS_FAILED = 1 # 等待状态 
    STATUS_RUNNING = 2 # 运行中
    STATUS_SUCCESS=3 # 任务成功运行
    STATUS_ARCHIVE=4 # 任务进行归档
    

    def __init__(self,taskDao,name,taskArgs,group=None,id=None,priority=0,status=STATUS_WAITING,retry=0,msg="",createTime=dateutil.now(),updateTime=dateutil.now()):
        self.updateTime = updateTime
        self.id=id
        self.taskArgs=taskArgs
        self.priority = priority
        self.status=status
        self.retry=retry
        self.msg=msg
        self.createTime=createTime
        self.name=name
        self.group=group
        self.taskDao = taskDao


    def comitTask(self,suc=True,msg=""):
        '''
        提交任务任务
        params suc: 任务是否成功，默认情况下提交成功任务
                msg: 对于起脚任务的说明
        '''
        if suc :
            self.status = Task.STATUS_SUCCESS
        else:
            self.status = Task.STATUS_FAILED
            self.retry += 1

        self.msg=msg
        self.updateTime = dateutil.now()
        self.taskDao.update(self)

    def archive(self):
        '''
        归档任务
        '''
        if self.status == Task.STATUS_SUCCESS:
            self.taskDao.archive(self)
        else:
            logger.warning("Task cant not archive because status is %s",self.status)
        

    def data(self,data=None):
        if data is None:
            data = {};
        
        data["id"] = self.id
        data["name"] = self.name
        data["taskArgs"] = self.taskArgs
        data["group"] = self.group
        data["priority"] = self.priority
        data["status"] = self.status
        data["retry"] = self.retry
        data["msg"] = self.msg
        data["createTime"] = self.createTime
        data["updateTime"] = self.updateTime
        return data

    def updateTime4Timestamp(self):
        return dateutil.timestamp(self.updateTime)


def loadTask(taskDao,data):
    return Task(taskDao = taskDao, \
        id=data.get("id",0), \
        name=data.get("name"), \
        taskArgs=data.get("taskArgs",{}), \
        group=data.get("group",None), \
        priority=data.get("priority",None), \
        status=data.get("status",None), \
        retry=data.get("retry",None), \
        msg=data.get("msg", None), \
        createTime=data.get("createTime",None), \
        updateTime=data.get("updateTime",None))



class FileTaskDao:
    '''
    使用文件来存储磁盘信息，需要一个磁盘文件目录，如果目录不存在将会自动创建
    '''

    dir = "" ## 任务文件存储的地址
    def __init__(self,dir=None):
        '''
        dir: 任务文件的保存路径
        '''
        self.dir = dir
        if not path.exists(self.dir):
            os.makedirs(self.dir)

        self.__idFile = path.join(self.dir,"id")
        if not path.exists(self.__idFile):
            ioutil.writeText(self.__idFile,"0")

        self.__taskFile = path.join(self.dir,"task")
        if not path.exists(self.__taskFile):
            ioutil.writeText(self.__taskFile,"[]")

        self.__archiveFile = path.join(self.dir,"archive")
        if not path.exists(self.__archiveFile):
            ioutil.writeText(self.__archiveFile,"[]")

        self.__lockFile = path.join(self.dir,"lock")
        if not path.exists(self.__lockFile):
            ioutil.writeText(self.__lockFile,"lock")



    def archive(self,task):
        lf = open(self.__lockFile,"w")
        fcntl.flock(lf,fcntl.LOCK_EX)
        try:
            taskArrStr = ioutil.readText(self.__taskFile)
            taskArr = json.loads(taskArrStr)
            data = None
            for index in range(len(taskArr)):
                if task.id == taskArr[index]["id"]:
                    data = taskArr.pop(index)
                    break
            ioutil.writeText(self.__taskFile,json.dumps(taskArr))

            if data is not None:
                archiveArrStr = ioutil.readText(self.__archiveFile)
                archiveArr = json.loads(archiveArrStr)
                archiveArr.append(data)
                ioutil.writeText(self.__archiveFile,json.dumps(archiveArr))

        finally:
            fcntl.flock(lf,fcntl.LOCK_UN)


    def genId(self):
        lf = open(self.__lockFile,"r+")
        fcntl.flock(lf,fcntl.LOCK_EX)
        try:
            
            idStr = ioutil.readText(self.__idFile)
            if idStr == None or idStr == "":
                idStr = "0"
            id = int(idStr)+1
            ioutil.writeText(self.__idFile,str(id))
            return id
        finally:
            fcntl.flock(lf,fcntl.LOCK_UN)
    
    

    def insert(self,task):
        '''
        插入一个新的任务
        '''
        task.id=self.genId()
        lf = open(self.__lockFile,"w")
        fcntl.flock(lf,fcntl.LOCK_EX)
        try:
            taskArrStr = ioutil.readText(self.__taskFile)
            taskArr = json.loads(taskArrStr)
            taskArr.append(task.data())
            taskArrStr = json.dumps(taskArr)
            ioutil.writeText(self.__taskFile,taskArrStr)
        finally:
            fcntl.flock(lf,fcntl.LOCK_UN)


    def update(self,task):
        lf = open(self.__lockFile,"w")
        fcntl.flock(lf,fcntl.LOCK_EX)
        try:
            taskArrStr = ioutil.readText(self.__taskFile)
            taskArr = json.loads(taskArrStr)
            for taskData in taskArr:
                if task.id == taskData["id"]:
                    task.data(taskData)
                    break
            ioutil.writeText(self.__taskFile,json.dumps(taskArr))
        finally:
            fcntl.flock(lf,fcntl.LOCK_UN)


    def dispatch(self):
        '''
        分派一个任务
        '''
        lf = open(self.__lockFile,"w")
        fcntl.flock(lf,fcntl.LOCK_EX)
        try:
            taskArrStr = ioutil.readText(self.__taskFile)
            taskArr = json.loads(taskArrStr)
            data = None
            for taskData in taskArr:
                # print(dateutil.now4Timestamp() > dateutil.timestamp(taskData["updateTime"])+5)
                if taskData.get("status",Task.STATUS_WAITING) == Task.STATUS_FAILED and dateutil.now4Timestamp() > dateutil.timestamp(taskData["updateTime"])+5:
                    data = taskData
                    break

                elif taskData.get("status",Task.STATUS_WAITING) == Task.STATUS_WAITING:
                    data = taskData
                    break

            
            if data is not None:
                logger.info("dispatcher as task, taskId:%s",data["id"])
                taskData["status"] = Task.STATUS_RUNNING
                taskData["updateTime"] = dateutil.now()
                ioutil.writeText(self.__taskFile,json.dumps(taskArr))
                return loadTask(self,data)   
            else:
                return None 
        finally:
            fcntl.flock(lf,fcntl.LOCK_UN)

    def list(self,status=None):
        '''
        '''
        lis = []
        taskArrStr = ioutil.readText(self.__taskFile)
        taskArr = json.loads(taskArrStr)
        for taskData in taskArr:
            if status is None or taskData["status"] == status:
                lis .append(loadTask(self,taskData))
        return lis;

        
class TaskManager:
    '''
    '''
    taskDao=None
    def __init__(self,file=None,retry=5):
        '''
        使用磁盘来存储任务
        '''
        
        self.taskDao = FileTaskDao(file)
        logger.info("retry:%s",retry)
    
    def dispatch(self):
        '''
        获得一个任务
        '''
        return self.taskDao.dispatch()

    def offer(self,name,taskArgs,priority=0):
        '''
        压入一个任务
        taskArgs: 任务的参数,字典
        priority: 任务优先及 1到5级等级越高优先级越高
        '''
        task = Task(taskDao=self.taskDao,name=name,taskArgs=taskArgs,priority=priority)
        self.taskDao.insert(task)

    def list(self,status=None):
        '''
        根据任务状态列出所有的任务
        return List<Task>
        '''
        return self.taskDao.list(status)
    

import cmdutil
import time

def printTaskList(lis):
    for task in lis:
        print(task.data())

if __name__ == '__main__':
    cmdutil.runCommandWithOutput("rm /www/tmp/task/*")
    taskManager = TaskManager(file="/www/tmp/task")
    taskManager.offer("name",{"url":"test"})
    taskManager.offer("name2",{"url":"test"})
    printTaskList(taskManager.list())
    task = taskManager.dispatch()
    printTaskList(taskManager.list(Task.STATUS_RUNNING))
    task.comitTask()
    task.archive()

    task = taskManager.dispatch()
    task.comitTask(suc=False,msg="io error")
    task = taskManager.dispatch()
    if task is not None:
        print("task dispache error");

    time.sleep(6)
    task = taskManager.dispatch()
    if task is None:
        print("task dispache error");

