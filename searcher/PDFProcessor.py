class PDFProcessor:

    def __init__(self):
        pass

    def convert(self, pdf_path):
        '''
            将pdf转换为txt
            
            返回一个string，其中包含正文部分
            每一个section的内容。

            注意：在进行pdf转txt的时候，需要将
            论文中的“标题”“摘要”“作者”“参考文献”等信息过滤掉，
            只保留正文中每一个section中的文本信息
        '''