class DownloadClient:
    """
    本类为文件下载的客户端，供SearchEngine类调用
    """
    def __init__(self):
        pass

    def file_tag(self):
        """
        检查文件是否完成下载
        :return: True or False
        """
        pass

    def download(self, ip, port, path=""):
        """
        向数据库服务器请求数据，获得的是一个压缩文件，然后解压到path下。
        :param ip: 数据库服务器ip
        :param port: 数据库服务器端口
        :param path: 下载后的存储路径，目前计划是包内的一个固定路径，因此以默认参数形式存在，不允许修改
        :return: 下载是否成功，使用布尔值表示
        """