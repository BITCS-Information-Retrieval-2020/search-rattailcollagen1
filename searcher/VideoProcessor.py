class VideoProcessor:

    def __init__(self):
        pass

    def convert(self, video_path):
        '''
            将一个video转换为一系列的文本
            （以句为单位）以及出现该文本的
            开始时间与结束时间

            Parameters:
                video_path: absolute path to a video file
            Return: 一个list，其中每一个元素是一个dict，该dict的格式为 (下面这个仅是一个例子)：
                    {
                        "timeStart": "hh-mm-ss",
                        "timeEnd": "hh-mm-ss",
                        "sentence": "Only small clean accuracy drop is observed in the process."
                    }
        '''