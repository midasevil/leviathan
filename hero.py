import time
import datetime
import re 
import socket
from threading import Thread
import pymysql
import logging
import sys
import queue
import random

logging.basicConfig(level=logging.INFO)

def send_msg(data):
    pat1 = bytes(bytearray([len(data)+9,0x00,0x00,0x00]))
    pat2 = bytes(bytearray([0xb1, 0x02, 0x00, 0x00]))
    msg = bytes(data.encode('utf-8'))
    end=bytes(bytearray([0x00,]))
        
    try:
        client_sk.sendall(bytes(pat1+pat1+pat2+msg+end))
        logging.debug('成功发送消息。')
    except Exception as e:
        raise e
        logging.error('未成功发送消息!')


def clean(recv_msg):
    clean_msg = recv_msg[12:-1].decode('utf-8','ignore')
    return clean_msg



        
def req_bar(num):
    raw_msg_login = 'type@=loginreq/username@=/password@=/roomid@={}/\x00'.format(num)
    raw_msg_join = 'type@=joingroup/rid@={}/gid@=-9999/\x00'.format(num)
    
    send_msg(raw_msg_login)
    time.sleep(1)
    send_msg(raw_msg_join)
    
class Fetch_bar(Thread):
    def __init__(self,q):
        Thread.__init__(self)
        self.q = q
        
    def run(self):
        try:
            self._run()
        except Exception as e:
            self.q.put(str(e))
            logging.error('Fetch_bar:'+str(e))
            
        
    
    
    
            
    def _run(self):
        global table_name

        db = pymysql.connect("ip","root","password","barrage",charset="utf8mb4")
        cur = db.cursor()
        sql1 = "create table {}(id int not null auto_increment,nn varchar(100),uid int,txt text,level int,bnn varchar(60),bl int,time datetime,dtime         datetime,primary key(id))"
        sql2 = "INSERT INTO {}(nn,uid,txt,level,bnn,bl,time,dtime) values( %s,%s,%s,%s,%s,%s,%s,%s)"
        re_type = "type@=(.*?)/"
        re_nn ="nn@=(.*?)/"
        re_uid = "uid@=(.*?)/"
        re_txt = "txt@=(.*?)/"
        re_level = "level@=(.*?)/"
        re_bnn = "bnn@=(.*?)/"
        re_bl = "bl@=(.*?)/"
        
        p_type = re.compile(re_type)
        p_nn = re.compile(re_nn)
        p_uid = re.compile(re_uid)
        p_txt = re.compile(re_txt)
        p_level = re.compile(re_level)
        p_bnn = re.compile(re_bnn)
        p_bl = re.compile(re_bl) 
        
        try:  
        
#            cur.execute("DROP TABLE IF EXISTS {}".format(table_name))
#            cur.execute(sql1.format(table_name))
#            logging.info("表已成功创建！恭喜。")
        
        
        

    
            while 1:
            
                recv_msg = client_sk.recv(4000)
                if not recv_msg:
                     break
                clean_msg = clean(recv_msg)            
                if "type@" in clean_msg:
                    try:
            
                        type = p_type.findall(clean_msg)[0]
                    except Exception as e:
                        pass
                    if type == "chatmsg":
                        try:
                            nn = p_nn.findall(clean_msg)[0] 
                        except:
                            nn = ""
                        try:
                            
                            uid = p_uid.findall(clean_msg)[0]
                        except:
                            uid = ""
                        
                        try:
                            txt = p_txt.findall(clean_msg)[0]
                        except :
                            txt = ""
                        try:    
                            level = p_level.findall(clean_msg)[0]
                        except:   
                            level =""
                            
                        try:
                            bnn = p_bnn.findall(clean_msg)[0]
                        except:
                            bnn = ""
                        try:
                            bl = p_bl.findall(clean_msg)[0]
                        except:
                            bl =""
                        
                        time = datetime.datetime.now().strftime('%Y-%m-%d %H')
                        dtime = datetime.datetime.now().strftime('%Y-%m-%d')
                        global count
                        count=count+1
                
            
                        print(str(count)+">>"+time+":"+"[level-"+level+"]["+nn+"]-------"+txt)
                    
                
                 
                        cur.execute(sql2.format(table_name),(nn,uid,txt,level,bnn,bl,time,dtime))
                    
                        db.commit()
                        logging.info("成功获得应答，数据插入成功>>>>>>>>>>>>>>>")
    
                    
                    
        finally:
        
            db.close()
        

    
class Keep_alive(Thread):
    def __init__(self,q):
        Thread.__init__(self)
        self.q = q
    
    def run(self):
        try:
            while True:
                time.sleep(50)
                send_msg("type@=mrkl/")
                
        except Exception as e:
            self.q.put(str(e))
            logging.error('Keep_alive'+str(e))
            
            
            

    
    
if __name__ == '__main__':
    q = queue.Queue()
    num = input("请输入roomid:")
    global table_name
    table_name = input("请输入表名:")
    
    try:
        while 1:
        
            signal = 1
            while signal == 1:
                try:
                    ports = [8601,8602,12601,12602]
                    port = random.choice(ports)
                    client_sk= socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    client_sk.connect(("danmu.douyu.com",port))
                    signal = 0
                except Exception as e:
                    logging.error(str(e))
                
            logging.info('已创建新的套接字。')
            q = queue.Queue()
            
            
            
            req_bar(num)
            logging.debug('成功发送弹幕请求，请等待！')
            
            global count
            count = 0
            
            t1 = Keep_alive(q)
            t2 = Fetch_bar(q)
            t1.setDaemon(True)
            t2.setDaemon(True)
            t1.start()
            t2.start()
            try:
                e = q.get()
                
                logging.error(e)
            finally:
                client_sk.close()
                logging.info('将在一分钟后尝试重新连接。')
                time.sleep(60)
    except KeyboardInterrupt as e:
        logging.error(str(e))
        
            
    print('end')
            
        
    