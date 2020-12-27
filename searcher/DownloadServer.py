import socket
import json
import struct
import os


class DownloadServer:
    """
    本类为下载数据的服务器端，接收客户端发来的请求并返回相应的数据
    """
    def __init__(self, port):
        """
        :param port: 监听请求的端口
        """
        self.s_port = port

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

        # 监听
        server.listen(5)

        # 通信循环
        while True:
            # 接收客户端连接请求
            conn, client_addr = server.accept()
            while True:
                # 接收客户端数据/命令
                res = conn.recv(1024)
                if not res:
                    continue
                # 解析命令 'get 1.pdf'
                cmds = res.decode('utf-8').split()  # ['get','1.pdf']
                path = os.getcwd() + "/" + cmds[1]  # '1.pdf'
                # 以读的方式打开文件，提取文件内容发送给客户端
                # 1.制作固定长度的报头
                header_dic = {
                    'filename': cmds[1],
                    'file_size': os.path.getsize(path)
                }
                # 序列化报头
                header_json = json.dumps(header_dic)  # 序列化为byte字节流类型
                header_bytes = header_json.encode('utf-8')  # 编码为utf-8（Mac系统）
                # 2.先发送报头的长度
                # 2.1 将byte类型的长度打包成4位int
                conn.send(struct.pack('i', len(header_bytes)))
                # 2.2 再发报头
                conn.send(header_bytes)
                # 2.3 再发真实数据
                with open(path, 'rb') as f:
                    for line in f:
                        conn.send(line)
            # 结束连接
            conn.close()

        # 关闭套接字
        server.close()
