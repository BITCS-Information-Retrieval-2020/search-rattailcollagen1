import socket
import os
import sys
import struct
import requests
import json
import zipfile
from time import sleep
import threading


class DownloadClient:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.data_path = os.path.dirname(os.path.abspath(__file__)) + '/data/cache'
        print(self.data_path)
        self.compressed_path = self.data_path + '/to_send.zip'
        print(self.data_path)
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

    def find_max(self):
        dir_itr = iter(os.walk(self.data_path))
        _, dir_list, _ = dir_itr.__next__()
        dir_list = list(map(int, dir_list))
        if dir_list:
            sub_dir = max(dir_list)
        else:
            sub_dir = 0
        print("client max dir:{}".format(sub_dir))
        return int(sub_dir)

    def send(self, ip, port):
        while True:
            url = 'http://{}:{}/download'.format(ip, port)
            pc_name = socket.getfqdn(socket.gethostname())
            pc_ip = socket.gethostbyname(pc_name)
            max_dir = self.find_max()
            params = {"max_dir": max_dir,
                      "ip": pc_ip,
                      "port": self.port}
            res = requests.get(url, params=params)
            res = json.loads(res.text)
            send_flag = int(res['send_flag'])
            if send_flag:
                self.receive()
            else:
                print("no file to receive")
                pass
            sleep(43200)

    def receive(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)
        except socket.error as e:
            print(e)
            sys.exit(1)

        print("waiting for receiving...")
        conn, addr = s.accept()
        while True:
            fileinfo_size = struct.calcsize('128sl')
            buf = conn.recv(fileinfo_size)
            if buf:
                file_name, file_size = struct.unpack('128sl', buf)
                file_name = file_name.strip(b'\00')
                file_name = file_name.decode()
                print('file name is {}, file size is {}'.format(file_name, file_size))
                file_size = file_size + 4

                received_size = 0
                out_file = open(self.compressed_path, 'wb')
                print('start receiving')
                strip_flag = True
                while received_size != file_size:
                    if file_size - received_size > 1024:
                        data = conn.recv(1024)
                        if strip_flag:
                            data = data.lstrip(b'\00\00\00\00')
                            strip_flag = False
                            received_size += 4
                        received_size += len(data)
                    else:
                        data = conn.recv(file_size-received_size)
                        if strip_flag:
                            data = data.lstrip(b'\00\00\00\00')
                            strip_flag = False
                        received_size = file_size
                    out_file.write(data)
                out_file.close()
                print('receiving end')
            conn.close()
            break
        s.close()
        z = zipfile.ZipFile(self.compressed_path, 'r')
        z.extractall(self.data_path)
        z.close()
        os.remove(self.compressed_path)


if __name__ == "__main__":
    client = DownloadClient('127.0.0.1', 9001)
    t = threading.Thread(target=client.send, args=('127.0.0.1', 5000))
    t.setDaemon(True)
    t.start()
    sleep(20)

