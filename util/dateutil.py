from datetime import datetime

def now():
    '''
    获得获得带钱时间，%Y-%m-%d %H:%M:%S
    '''
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def now4Timestamp():
    '''
    '''
    return int(datetime.now().timestamp())

def timestamp(dateStr):
    '''
    dateStr: %Y-%m-%d %H:%M:%S的str
    return: 单位秒
    '''
    date = datetime.strptime(dateStr,'%Y-%m-%d %H:%M:%S')
    return int(date.timestamp())
    
if __name__ == '__main__':
    print(timestamp("2019-12-12 10:10:10"))
