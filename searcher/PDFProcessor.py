class PDFProcessor:

    '''
        本模块用于将PDF转换为Python内置的字符串格式。

        使用方案是，上层调用程度首先实例化该类的一个对象，
        然后调用self.convert函数，并向其中传入一个pdf文件的
        绝对路径。该模块负责读取相应的pdf文件，并将其转换为文本。
        在转换的过程中，只保留pdf的正文部分，其他部分去掉。
    '''

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

            Parameters:
                pdf_path: absolute path to a pdf file
            Return: 
                a string of text
        '''