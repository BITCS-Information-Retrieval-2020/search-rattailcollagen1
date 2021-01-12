import xml.etree.ElementTree as ET
import os

from .grobid_client import grobid_client as grobid


class PDFProcessor:

    '''
        本模块用于将PDF转换为Python内置的字符串格式。

        使用方案是，上层调用程度首先实例化该类的一个对象，
        然后调用self.convert函数，并向其中传入一个pdf文件的
        绝对路径。该模块负责读取相应的pdf文件，并将其转换为文本。
        在转换的过程中，只保留pdf的正文部分，其他部分去掉。
    '''

    def __init__(self):
        self.currentPath = '/'.join(os.path.split(
            os.path.realpath(__file__))[0].split('\\'))

    def PDFtoXML(self, server, port, pdf_dir):
        # xml_dir = f'{self.currentPath}/data/XMLs'
        if os.path.basename(pdf_dir) == '':
            xml_dir = os.path.join(os.path.dirname(
                os.path.dirname(pdf_dir)), 'XMLs')
        else:
            xml_dir = os.path.join(os.path.dirname(pdf_dir), 'XMLs')
        if not os.path.exists(xml_dir):
            print(xml_dir)
            os.makedirs(xml_dir)
        client = grobid.grobid_client(
            config_path=f'{self.currentPath}/grobid_client/config.json', grobid_server=server, grobid_port=port)
        client.process("processFulltextDocument", input_path=pdf_dir, output=xml_dir,
                       consolidate_citations=True, teiCoordinates=True, force=False)

    @staticmethod
    def parsexml(self, xml_path):
        tree = ET.parse(xml_path)
        root = tree.getroot()
        body = root.find('.//{http://www.tei-c.org/ns/1.0}body')
        i = 0
        flag = 0
        res = ''
        for div in body.findall('.//{http://www.tei-c.org/ns/1.0}div'):
            if 'CONCLUSION' in div[0].text or 'Conclusion' in div[0].text:
                print('CCC IS CONCLUSION')
                flag = 1
            s = ''
            i += 1
            for p in div.findall('.//{http://www.tei-c.org/ns/1.0}p'):
                s += p.text
                if len(p.findall('.//*')):
                    for child in p.findall('.//*'):
                        if child.tail:
                            s += child.tail
            # print(f'{i}:{s}')
            res += s
            if flag:
                break
        return res

    def convert(self, server, port, pdf_path):
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
        pdf_basename = os.path.basename(pdf_path)
        pdf_name = os.path.splitext(pdf_basename)[0]
        pdf_dir = os.path.dirname(pdf_path)
        xml_dir = os.path.join(os.path.dirname(pdf_dir), 'XMLs')
        if not os.path.exists(xml_dir):
            os.makedirs(xml_dir)
        xml_path = os.path.join(xml_dir, f'{pdf_name}.tei.xml')
        if not os.path.exists(xml_path):
            print('XML IS NOT FOUND!')
            print(xml_path)
            client = grobid.grobid_client(
                config_path=f'{self.currentPath}/grobid_client/config.json', grobid_server=server, grobid_port=port)
            client.process("processFulltextDocument", input_path=pdf_path, output=xml_dir,
                           consolidate_citations=True, teiCoordinates=True, force=False)
        res = self.parsexml(self, xml_path)
        return res


if __name__ == '__main__':
    pdf = PDFProcessor()
    pdf.PDFtoXML('localhost', '8070',
                 '/data/sfs/search-rattailcollagen1/searcher/data/3/PDFs')
    res = pdf.convert('localhost', '8070',
                      '/data/sfs/search-rattailcollagen1/searcher/data/2/PDFs/2009.14720.pdf')
    print(res)
