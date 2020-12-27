import os
from moviepy.editor import VideoFileClip
from aip import AipSpeech
from pydub import AudioSegment
from pydub.silence import detect_nonsilent

APP_ID = '23399411'
API_KEY = 'hbczxy8HWuf0G98IPq8RW5eu'
SECRET_KEY = 'VDpW44pGMusAZ54uyzzGGDFgPRjhcnhd'


def split_on_silence(audio_seg, min_silence_len, thresh, keep_sil):

    not_silence_range = detect_nonsilent(audio_seg, min_silence_len, thresh, 1)

    chunks = []
    starttime = []
    endtime = []
    for start_i, end_i in not_silence_range:
        start_i = max(0, start_i - keep_sil)
        end_i += keep_sil

        chunks.append(audio_seg[start_i:end_i])
        starttime.append(start_i)
        endtime.append(end_i)

    return chunks, starttime, endtime


def speech_split(speech_file, save_dir):
    '''
    音频分割，并得到其时间轴位置

    参数
    ----------
    speech_file : str
        音频文件地址
    save_dir : str
        保存文件地址

    返回值
    -------
    chunkss
        分割音频文件地址
    chunk_lens
        音频起止时间

    '''
    sound = AudioSegment.from_mp3(speech_file)
    chunks, startt, endt = split_on_silence(sound, min_silence_len=350,
                                            thresh=-45, keep_sil=400)
    chunk_lens = []
    for i, chunk in enumerate(chunks):
        chunk_lens.append((startt[i] / 1000, endt[i] / 1000))

    # 放弃长度小于2秒的录音片段
    for i in list(range(len(chunks)))[::-1]:
        if len(chunks[i]) <= 1500 or len(chunks[i]) >= 10000:
            chunks.pop(i)
            chunk_lens.pop(i)

    chunkss = []
    for i, chunk in enumerate(chunks):
        chunk.export(save_dir + "/chunk{0}.wav".format(i), format="wav")
        chunkss.append("/chunk{0}.wav".format(i))

    return chunkss, chunk_lens


def format(filePath):
    sound1 = AudioSegment.from_file(filePath, format="wav")
    sound2 = sound1.set_frame_rate(16000)
    sound2 = sound2.set_channels(1)
    sound2 = sound2.set_sample_width(2)
    sound2.export('audio_tmp.wav', format='wav', )


def get_file_content(filePath):

    format(filePath)
    with open('audio_tmp.wav', 'rb') as fp:
        return fp.read()


def stt(audio_address):

    lang = 1737
    aipSpeech = AipSpeech(APP_ID, API_KEY, SECRET_KEY)

    audio = get_file_content(audio_address)
    json = aipSpeech.asr(audio, 'wav', 16000, {
            'dev_pid': lang,
    })
    return json.get(['result'][0])


def videoToSpeech(source, target):

    video = VideoFileClip(source)
    audio = video.audio
    audio.write_audiofile(target)
    return


def convert_time(seconds):

    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return str("%02d-%02d-%02d" % (h, m, s))


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
                        "sentence": "Only small clean accuracy drop
                        is observed in the process."
                    }
        '''

        videoToSpeech(video_path, target="tmp.wav")
        os.makedirs("splits")
        chunks, chunk_lens = speech_split("tmp.wav", "splits")

        lastend = 0
        text_list = list()
        for i, each in enumerate(chunks):
            start = convert_time(max(int(chunk_lens[i][0]), lastend))
            end = convert_time(int(chunk_lens[i][1]))
            lastend = int(chunk_lens[i][1])
            sentence = stt("./splits" + each)
            os.remove("splits/" + each)
            if sentence is None:
                continue
            tmp = {'timeStart': start, 'timeEnd': end,  'sentence': sentence}
            text_list.append(tmp)
        os.remove("tmp.wav")
        os.removedirs("splits")
        return text_list
