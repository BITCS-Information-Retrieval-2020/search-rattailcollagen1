class SearchEngine:
    def __init__(self, ip, port):
        """
        1.调用DowonloadClient中的file_tag()检查文件是否下好，
        如果没下好，先调用DownloadClient中的download()进行下载
        2.初始化一个ESClient对象
        :param ip: ES服务器的IP
        :param port: ES服务器对应的端口
        """

    def search(self, qeury):
        """
        对ES的search进行封装，供展示组调用
        :param qeury: 展示组发送的查询
        :return: 结果列表
        """