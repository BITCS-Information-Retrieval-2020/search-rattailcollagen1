from .DownloadClient import DownloadClient
from .ESClient import ESClient
import threading


class SearchEngine:
    def __init__(self, download_server_ip, download_server_port, download_client_ip, download_client_port, es_ip, es_port):
        """
        1.调用DowonloadClient中的file_tag()检查文件是否下好，
        如果没下好，先调用DownloadClient中的download()进行下载
        2.初始化一个ESClient对象
        :param download_server_ip: 数据服务器ip
        :param download_server_port: 数据服务器端口
        :param download_client_ip: 接收数据的ip
        :param download_client_port: 接收数据的端口
        :param es_ip: es服务的ip
        :param es_port: es服务的端口
        """
        self.client = DownloadClient(download_client_ip, download_client_port)
        t = threading.Thread(target=self.client.send, args=(download_server_ip, download_server_port))
        t.setDaemon(True)
        t.start()
        es_param = [{"host": es_ip,
                    "port": es_port}]
        self.es = ESClient(es_param)

    def search(self, query):
        """
        对ES的search进行封装，供展示组调用
        :param query: 展示组发送的查询
        :return: 结果列表
        """
        res = self.es.search(query)
        return res
