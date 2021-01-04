import os
import socket
import json
import struct
import zipfile

RECVSIZE = 1024
path = './data'

class DownloadServer:
    """
    本类为下载数据的服务器端，接收客户端发来的请求并返回相应的数据
    """
    def __init__(self, port):
        """
        :param port: 监听请求的端口
        """
        self.s_port = port

    
    def zip_ya(self, startdir):
        file_news = startdir + '.zip'  # 压缩后文件夹的名字
        z = zipfile.ZipFile(file_news, 'w', zipfile.ZIP_DEFLATED)  # 参数一：文件夹名
        for dirpath, dirnames, filenames in os.walk(startdir):
            fpath = dirpath.replace(startdir, '')  # 这一句很重要，不replace的话，就从根目录开始复制
            fpath = fpath and fpath + os.sep or ''  # 这句话理解我也点郁闷，实现当前文件夹以及包含的所有文件的压缩
            for filename in filenames:
                z.write(os.path.join(dirpath, filename), fpath + filename)
        print('压缩成功')
        z.close()

    

    def findMaxnum(self, path):
        """
        找到Server 路径下文件夹个数
        :param path:
        :return:
        """
        count = 0
        for _ in os.listdir(path):
            count += 1
        return count

    def send_file_client(self, dir_socket):
        """
        向客户端发送字节流
        :param dir_socket:
        :return:
        """
        send_str = dir_socket.recv(RECVSIZE).decode()
        if send_str.startswith('Give'):
            maxnum = self.findMaxnum(path)
            dir_socket.send(str(maxnum).encode())
        else:
            print('Transport the {}th folder'.format(str(send_str)))
            dir_name = path + str(send_str)
            self.zip_ya(dir_name)
            self.transport_zip(dir_socket, dir_name + '.zip')

    def transport_zip(self, dir_socket, filepath):
        """
        传输ZIP压缩文件
        :param dir_socket:
        :param filepath:
        :return:
        """
        filesize_bytes = os.path.getsize(filepath)  # 得到文件的大小,字节
        dirc = {
            'filename': filepath,
            'filesize_bytes': filesize_bytes,
        }
        head_info = json.dumps(dirc)  # 将字典转换成字符串
        head_info_len = struct.pack('i', len(head_info))  # 将字符串的长度打包
        #   先将报头转换成字符串(json.dumps), 再将字符串的长度打包
        #   发送报头长度,发送报头内容,最后放真是内容
        #   报头内容包括文件名,文件信息,报头
        #   接收时:先接收4个字节的报头长度,
        #   将报头长度解压,得到头部信息的大小,在接收头部信息, 反序列化(json.loads)
        #   最后接收真实文件
        dir_socket.send(head_info_len)  # 发送head_info的长度
        dir_socket.send(head_info.encode('utf-8'))

        #   发送真是信息
        with open(filepath, 'rb') as f:
            data = f.read()
            dir_socket.sendall(data)

        os.remove(filepath)

        print('发送成功')

    def listen(self):
        """
        监听客户端发来的请求并返回文件
        :return:
        """
        # 建立
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # 绑定
        server.bind(('127.0.0.1', self.s_port))

        # 通信循环
        server_socket.listen(128)

        while True:
            print('Waiting for a new connection........')
            # 生成一个面向请求客户端的套接字
            dir_socket, client_ip = server_socket.accept()
            # 调用发送数据函数
            while True:
                self.send_file_client(dir_socket)
            # 关闭套接字
            dir_socket.close()
        server_socket.close()
