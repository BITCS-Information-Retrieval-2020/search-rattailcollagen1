import socket
import struct
import json
import os


class DownloadClient:
    """
    本类为文件下载的客户端，供SearchEngine类调用
    """
    def __init__(self):
        self.f_tag = True
        pass

    def file_tag(self):
        """
        检查文件是否完成下载
        :return: True or False
        """
        return self.f_tag
        pass

    def download(self, ip, port, path=""):
        """
        向数据库服务器请求数据，获得的是一个压缩文件，然后解压到path下。
        :param ip: 数据库服务器ip
        :param port: 数据库服务器端口
        :param path: 下载后的存储路径，目前计划是包内的一个固定路径，因此以默认参数形式存在，不允许修改
        :return: 下载是否成功，使用布尔值表示
        """
        if not os.path.exists("./data/" + path):
            self.f_tag = False
            client = socket.socket()
            client.connect((ip, port))
            cmd = "get " + path
            client.send(cmd.encode('utf-8'))
            while True:
                # 1.先收报头长度
                obj = client.recv(4)
                header_size = struct.unpack('i', obj)[0]
                # 2.收报头
                '''
                        header_dic = {
                        'filename': filename,
                        'file_size': os.path.getsize(filename)
                    }
                '''
                header_bytes = client.recv(header_size)
                # 3.从报头中解析出数据的真实信息（报头字典）
                header_json = header_bytes.decode('utf-8')
                header_dic = json.loads(header_json)
                # 4.解析命令
                total_size = header_dic['file_size']

                # 5.接受真实数据
                with open("./data/" + path, 'wb') as f:
                    recv_size = 0
                    while recv_size < total_size:
                        line = client.recv(1024)
                        f.write(line)
                        recv_size += len(line)
                        print('总大小：%s kb    已下载：%s kb' % (total_size / 1024, recv_size / 1024))
                    self.f_tag = True
                    print('已下载完毕')
