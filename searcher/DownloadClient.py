import socket
import os
import re
import time
import struct
import json
import sys
import zipfile

source = './data'
buffsize = 1024

class DownloadClient:
    """
    本类为文件下载的客户端，供SearchEngine类调用
    """
    def __init__(self):
        pass

    def recv_file_server(self, client_socket, path):
        """
        向服务端请求最大文件夹数
        并请求自己还缺少的文件夹
        """
        while True:
            sendStr = 'Give the maxnum of the folders to me!'
            client_socket.send(sendStr.encode())
            msg = int(client_socket.recv(buffsize).decode())

            print('maxnum folders from Server: ', msg)
            maxnum_current = findMaxnum(source)
            print('current maxnum: ', maxnum_current)
            print('There are {} folders need to download...'.format(msg - maxnum_current))

            if msg > maxnum_current:
                for i in range(maxnum_current, msg):
                    downloadfold(client_socket, i + 1, path)
                    print('Get the {}th folder zip'.format(i + 1))
                    folder_path = path + '\\' + str(i + 1)
                    frzip = zipfile.ZipFile(folder_path + '.zip', 'r', zipfile.ZIP_DEFLATED)
                    # 将所有文件加压缩到指定目录
                    frzip.extractall(folder_path)
                    frzip.close()
                    os.remove(folder_path + '.zip')
            else:
                print('No folder needs to be downloaded........')
            time.sleep(5)

    def process_bar(self, precent, width=50):
        use_num = int(precent * width)
        space_num = int(width - use_num)
        precent = precent * 100
        #   第一个和最后一个一样梯形显示, 中间两个正确,但是在python2中报错
        #
        # print('[%s%s]%d%%'%(use_num*'#', space_num*' ',precent))
        # print('[%s%s]%d%%'%(use_num*'#', space_num*' ',precent), end='\r')
        print('[%s%s]%d%%' % (use_num * '#', space_num * ' ', precent), file=sys.stdout, flush=True, end='\r')
        # print('[%s%s]%d%%'%(use_num*'#', space_num*' ',precent),file=sys.stdout,flush=True)

    def downloadfold(self, client_socket, folder_th, path):
        client_socket.send(str(folder_th).encode())

        # 服务端信息接收器：甲
        head_struct = client_socket.recv(4)  # 接收报头的长度,
        head_len = struct.unpack('i', head_struct)[0]  # 解析出报头的字符串大小
        data = client_socket.recv(head_len)  # 接收长度为head_len的报头内容的信息 (包含文件大小,文件名的内容)

        head_dir = json.loads(data.decode('utf-8'))
        filesize_b = head_dir['filesize_bytes']

        #   接受真的文件内容
        recv_len = 0
        old = time.time()
        with open(path + '\\' + str(folder_th) + '.zip', 'wb') as f:
            while recv_len < filesize_b:
                percent = recv_len / filesize_b

                process_bar(percent)
                if filesize_b - recv_len > buffsize:

                    recv_mesg = client_socket.recv(buffsize)
                    f.write(recv_mesg)
                    recv_len += len(recv_mesg)
                else:
                    recv_mesg = client_socket.recv(filesize_b - recv_len)
                    recv_len += len(recv_mesg)
                    f.write(recv_mesg)

            now = time.time()
            stamp = int(now - old)
            print('Download {}th folder, use %ds'.format(folder_th, stamp))

    def findMaxnum(self, path):
        """
        找到Client 路径下文件夹个数
        :param path: Client 查找路径
        :return:
        """
        count = 0
        for _ in os.listdir(path):
            count += 1
        return count

    def download(self, ip, port, path=""):
        """
        向数据库服务器请求数据，获得的是一个压缩文件，然后解压到path下。
        :param ip: 数据库服务器ip
        :param port: 数据库服务器端口
        :param path: 下载后的存储路径，目前计划是包内的一个固定路径，因此以默认参数形式存在，不允许修改
        :return: 下载是否成功，使用布尔值表示
        """
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 连接服务器
        client_socket.connect((ip, port))
        # 调用接收函数
        recv_file_server(client_socket, path)
        # 关闭套接字
        client_socket.close()

