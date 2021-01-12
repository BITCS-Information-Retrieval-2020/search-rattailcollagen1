from searcher.DownloadClient import DownloadClient
from searcher.ESClient import ESClient
import threading
import os
import re


class SearchEngine:
    def __init__(self, download_server_ip, download_server_port, download_client_ip, download_client_port,
                 es_ip, es_port, index_name, video_index_name):
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
        self.es = ESClient([{"host": es_ip, "port": es_port}], index_name=index_name, video_index_name=video_index_name)
        self.titles = []

        # thread for update data
        t = threading.Thread(target=self.client.send, args=(download_server_ip, download_server_port))
        t.setDaemon(True)
        t.start()

        # thread for update title list
        t = threading.Thread(target=self.es.get_all_title, args=(self.titles,))
        t.setDaemon(True)
        t.start()

    def search(self, query):
        """
        对ES的search进行封装，供展示组调用
        :param query: 展示组发送的查询
        :return: 结果列表
        """
        res = self.es.search(query)
        processed_res = []
        abs_dirname = os.path.dirname(os.path.abspath(__file__))
        if query["type"] == 2:
            for item in res:
                content = dict()
                content["timeStart"] = item["timeStart"]
                content["timeEnd"] = item["timeEnd"]
                content["sentence"] = item["sentence"]
                paper = self.es.search_by_id(item['paper_id'])
                paper["videoStruct"] = [content]
                paper["pdfPath"] = abs_dirname + paper["pdfPath"]
                paper["videoPath"] = abs_dirname + paper["videoPath"]
                processed_res.append(paper)
        else:
            for item in res:
                item["pdfPath"] = abs_dirname + item["pdfPath"]
                item["videoPath"] = abs_dirname + item["videoPath"]
                processed_res.append(item)
        return processed_res

    def auto_complete(self, query):
        cnt = 0
        res = []
        for title in self.titles:
            if re.search(query, title, re.IGNORECASE):
                res.append(title)
                cnt += 1
            if cnt > 10:
                break
        return res

    def search_by_id(self, id_):
        res = self.es.search_by_id(id_)
        abs_dirname = os.path.dirname(os.path.abspath(__file__))
        for item in res:
            item["pdfPath"] = abs_dirname + item["pdfPath"]
            item["videoPath"] = abs_dirname + item["videoPath"]
        return res