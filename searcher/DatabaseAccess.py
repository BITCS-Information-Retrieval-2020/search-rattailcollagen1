class DatabaseAccess:

    '''
        一点设想：希望部署MongoDB的同学可以将爬虫模块提供的MongoDB打包（拷贝）至
        我们的/data文件夹下某处，然后通过运行一些指令（可以用Python单独写一个简单的脚本）的方式
        来将这个MongoDB部署到本机。之后再利用该类（DatabaseAccess）来对MongoDB中的数据进行读取。
        外层调用程序将通过self.read函数来批量读取MongoDB中的dict数据。
    '''

    def __init__(self):
        pass
    
    def read(self, number):
        '''
            读取数据库，每次读取number个数据，
            并以list返回，list中的每一个元素为
            一个dict
        '''