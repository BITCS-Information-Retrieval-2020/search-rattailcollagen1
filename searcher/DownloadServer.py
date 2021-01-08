import threading
import socket
import struct
import sys
import os
import zipfile
from flask import Flask
from flask import request
from time import sleep

MAX_LENGTH = 1024


class DownloadServer:
    def __init__(self):
        self.data_path = os.path.dirname(os.path.abspath(__file__)) + '/data/cache'
        self.compressed_path = self.data_path + '/to_send.zip'

    def find_max(self):
        dir_itr = iter(os.walk(self.data_path))
        _, dir_list, _ = dir_itr.__next__()
        if dir_list:
            sub_dir = dir_list[-1]
        else:
            sub_dir = 0
        print("server max dir:{}".format(sub_dir))
        return int(sub_dir)

    def judge(self, client_max_dir):
        server_max_dir = self.find_max()
        if client_max_dir < server_max_dir:
            return 1, server_max_dir
        return 0, server_max_dir

    def compress(self, client_max_dir, server_max_dir):
        z = zipfile.ZipFile(self.compressed_path, 'w')
        for i in range(client_max_dir+1, server_max_dir+1):
            sub_path = self.data_path+'/'+str(i)
            z.write(sub_path, str(i))
            for path, sub_dir, files in os.walk(sub_path):
                relative_path = path.replace(sub_path, str(i))
                for cur_file in files:
                    z.write(path + '/' + cur_file, relative_path + '/' + cur_file)
        z.close()

    def send(self, client_max_dir, server_max_dir, client_ip, client_port):
        self.compress(client_max_dir, server_max_dir)
        sleep(1)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            print(e)
            sys.exit(1)
        s.connect((client_ip, client_port))
        fileinfo_size = struct.calcsize('128sl')
        file_head = struct.pack('128sl',
                                os.path.basename(self.compressed_path).encode('utf-8'),
                                os.stat(self.compressed_path).st_size)
        s.send(file_head)
        fp = open(self.compressed_path, 'rb')
        while True:
            data = fp.read(MAX_LENGTH)
            if not data:
                print("send over")
                break
            else:
                print("send")
                s.send(data)
        s.close()
        fp.close()
        os.remove(self.compressed_path)


app = Flask(__name__)
server = DownloadServer()


@app.route('/download', methods=['GET'])
def download():
    client_max_dir = int(request.args.get("max_dir"))
    target_ip = request.args.get("ip")
    target_port = int(request.args.get("port"))
    send_flag, server_max_dir = server.judge(client_max_dir)
    if send_flag:
        t = threading.Thread(target=server.send, args=(client_max_dir, server_max_dir, target_ip, target_port))
        t.start()
        print("thread start")
        return '{"send_flag":1}'
    else:
        return '{"send_flag":0}'


@app.route('/test', methods=['GET'])
def test():
    return "test"

if __name__ == "__main__":
    app.run(host='0.0.0.0')
