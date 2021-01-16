from ScienceSearcher.DownloadClient import DownloadClient
from ScienceSearcher.ESClient import ESClient
from time import sleep
import threading
import os
import re


class SearchEngine:
    def __init__(self, download_server_ip, download_server_port, download_client_ip, download_client_port,
                 es_ip, es_port, index_name, video_index_name, group_name):
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
        self.client = DownloadClient(download_client_ip, download_client_port, group_name)
        self.es = ESClient([{"host": es_ip, "port": es_port}], index_name=index_name, video_index_name=video_index_name)
        self.titles = []
        self.send_flag = [False]

        # thread for update data
        t = threading.Thread(target=self.client.send, args=(download_server_ip, download_server_port, self.send_flag))
        t.setDaemon(True)
        t.start()
        sleep(1)
        if self.send_flag[0]:
            print("receiving data, please don't stop the process until \"receiving end\" appears in terminal")
            while self.send_flag[0]:
                sleep(1)

        # thread for update title list
        t = threading.Thread(target=self.es.get_all_title, args=(self.titles,))
        t.setDaemon(True)
        t.start()
        while not self.titles:
            sleep(1)

    def search(self, query):
        """
        对ES的search进行封装，供展示组调用
        :param query: 展示组发送的查询
        :return: 结果列表
        """
        if query["type"] == 1:
            complete_new_query = dict()
            new_query = dict()
            new_query["title"] = query["query_text"]["title"].lower()
            new_query["authors"] = query["query_text"]["authors"].lower()
            new_query["abstract"] = query["query_text"]["abstract"].lower()
            new_query["pdfText"] = query["query_text"]["content"].lower()
            new_query["year"] = query["query_text"]["year"].lower()

            complete_new_query["type"] = query["type"]
            complete_new_query["top_number"] = query["top_number"]
            complete_new_query["query_text"] = new_query
            complete_new_query["operator"] = query["operator"]

            query = complete_new_query

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
        try:
            res = self.es.search_by_id(id_)
        except BaseException as e:
            print(e)
            print("id doesn't exist")
            return {}
        abs_dirname = os.path.dirname(os.path.abspath(__file__))
        res["pdfPath"] = abs_dirname + res["pdfPath"]
        res["videoPath"] = abs_dirname + res["videoPath"]
        return res
