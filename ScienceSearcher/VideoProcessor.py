import os
import json
from moviepy.editor import VideoFileClip
from aip import AipSpeech
from pydub import AudioSegment
from pydub.silence import detect_nonsilent

APP_ID = '23396576'
API_KEY = 'K5u0EndzcGigK888uQr5oQ5R'
SECRET_KEY = 'ep66s5mpcLm7MOaZBsuEbFk53vu7Z4jm'

video_types = ['.264', '.3g2', '.3gp', '.3gp2', '.3gpp', '.3gpp2',
               '.3mm', '.3p2', '.60d', '.787', '.89', '.aaf', '.aec', '.aep', '.aepx',
               '.aet', '.aetx', '.ajp', '.ale', '.am', '.amc', '.amv', '.amx',
               '.anim', '.aqt', '.arcut', '.arf', '.asf', '.asx', '.avb',
               '.avc', '.avd', '.avi', '.avp', '.avs', '.avs', '.avv', '.axm',
               '.bdm', '.bdmv', '.bdt2', '.bdt3', '.bik', '.bin', '.bix',
               '.bmk', '.bnp', '.box', '.bs4', '.bsf', '.bvr', '.byu',
               '.camproj', '.camrec', '.camv', '.ced', '.cel', '.cine', '.cip',
               '.clpi', '.cmmp', '.cmmtpl', '.cmproj', '.cmrec', '.cpi', '.cst',
               '.cvc', '.cx3', '.d2v', '.d3v', '.dat', '.dav', '.dce',
               '.dck', '.dcr', '.dcr', '.ddat', '.dif', '.dir', '.divx', '.dlx',
               '.dmb', '.dmsd', '.dmsd3d', '.dmsm', '.dmsm3d', '.dmss',
               '.dmx', '.dnc', '.dpa', '.dpg', '.dream', '.dsy', '.dv',
               '.dv-avi', '.dv4', '.dvdmedia', '.dvr', '.dvr-ms', '.dvx', '.dxr',
               '.dzm', '.dzp', '.dzt', '.edl', '.evo', '.eye', '.ezt',
               '.f4p', '.f4v', '.fbr', '.fbr', '.fbz', '.fcp', '.fcproject',
               '.ffd', '.flc', '.flh', '.fli', '.flv', '.flx', '.gfp',
               '.gl', '.gom', '.grasp', '.gts', '.gvi', '.gvp', '.h264', '.hdmov',
               '.hkm', '.ifo', '.imovieproj', '.imovieproject', '.ircp',
               '.irf', '.ism', '.ismc', '.ismv', '.iva', '.ivf', '.ivr', '.ivs',
               '.izz', '.izzy', '.jss', '.jts', '.jtv', '.k3g', '.kmv',
               '.ktn', '.lrec', '.lsf', '.lsx', '.m15', '.m1pg', '.m1v', '.m21',
               '.m21', '.m2a', '.m2p', '.m2t', '.m2ts', '.m2v', '.m4e',
               '.m4u', '.m4v', '.m75', '.mani', '.meta', '.mgv', '.mj2', '.mjp',
               '.mjpg', '.mk3d', '.mkv', '.mmv', '.mnv', '.mob', '.mod',
               '.modd', '.moff', '.moi', '.moov', '.mov', '.movie', '.mp21',
               '.mp21', '.mp2v', '.mp4', '.mp4v', '.mpe', '.mpeg', '.mpeg1',
               '.mpeg4', '.mpf', '.mpg', '.mpg2', '.mpgindex', '.mpl',
               '.mpl', '.mpls', '.mpsub', '.mpv', '.mpv2', '.mqv', '.msdvd',
               '.mse', '.msh', '.mswmm', '.mts', '.mtv', '.mvb', '.mvc',
               '.mvd', '.mve', '.mvex', '.mvp', '.mvp', '.mvy', '.mxf',
               '.mxv', '.mys', '.ncor', '.nsv', '.nut', '.nuv', '.nvc', '.ogm',
               '.ogv', '.ogx', '.osp', '.otrkey', '.pac', '.par', '.pds',
               '.pgi', '.photoshow', '.piv', '.pjs', '.playlist', '.plproj',
               '.pmf', '.pmv', '.pns', '.ppj', '.prel', '.pro', '.prproj',
               '.prtl', '.psb', '.psh', '.pssd', '.pva', '.pvr', '.pxv',
               '.qt', '.qtch', '.qtindex', '.qtl', '.qtm', '.qtz', '.r3d',
               '.rcd', '.rcproject', '.rdb', '.rec', '.rm', '.rmd', '.rmd',
               '.rmp', '.rms', '.rmv', '.rmvb', '.roq', '.rp', '.rsx', '.rts',
               '.rts', '.rum', '.rv', '.rvid', '.rvl', '.sbk', '.sbt',
               '.scc', '.scm', '.scm', '.scn', '.screenflow', '.sec',
               '.sedprj', '.seq', '.sfd', '.sfvidcap', '.siv', '.smi', '.smi',
               '.smil', '.smk', '.sml', '.smv', '.spl', '.sqz', '.srt',
               '.ssf', '.ssm', '.stl', '.str', '.stx', '.svi', '.swf', '.swi',
               '.swt', '.tda3mt', '.tdx', '.thp', '.tivo', '.tix', '.tod',
               '.tp', '.tp0', '.tpd', '.tpr', '.trp', '.ts', '.tsp', '.ttxt',
               '.tvs', '.usf', '.usm', '.vc1', '.vcpf', '.vcr', '.vcv',
               '.vdo', '.vdr', '.vdx', '.veg', '.vem', '.vep', '.vf', '.vft',
               '.vfw', '.vfz', '.vgz', '.vid', '.video', '.viewlet', '.viv',
               '.vivo', '.vlab', '.vob', '.vp3', '.vp6', '.vp7', '.vpj',
               '.vro', '.vs4', '.vse', '.vsp', '.w32', '.wcp', '.webm',
               '.wlmp', '.wm', '.wmd', '.wmmp', '.wmv', '.wmx', '.wot', '.wp3',
               '.wpl', '.wtv', '.wve', '.wvx', '.xej', '.xel', '.xesc', '.xfl',
               '.xlmv', '.xmv', '.xvid', '.y4m', '.yog', '.yuv', '.zeg',
               '.zm1', '.zm2', '.zm3', '.zmv']


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
    os.remove('audio_tmp.wav')
    jsonn = aipSpeech.asr(audio, 'wav', 16000, {
                          'dev_pid': lang,
                          })
    return jsonn.get(['result'][0])


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

    def video2text(self, videos_path, force_process=False):
        parent_path = os.path.abspath(os.path.join(videos_path, '..'))
        struct_path = os.path.join(parent_path, 'videosstruct')
        if os.path.exists(struct_path) is False:
            os.makedirs(struct_path)

        video_files = os.listdir(videos_path)
        for video_file in video_files:
            tmp = '.'.join(video_file.split('.')[:-1]) + '.json'
            json_path = os.path.join(struct_path, tmp)
            _, file_extension = os.path.splitext(video_file)
            if file_extension not in video_types:
                continue
            if force_process is False and os.path.exists(json_path):
                continue
            video_path = os.path.join(videos_path, video_file)
            videoToSpeech(video_path, target="tmp.wav")
            if os.path.exists('splits'):
                files = os.listdir('splits')
                for file in files:
                    os.remove('./splits/' + file)
                os.removedirs('splits')
            os.makedirs("splits")
            chunks, chunk_lens = speech_split("tmp.wav", "splits")
            lastend = 0
            text_list = list()
            for i, each in enumerate(chunks):
                start = convert_time(max(int(chunk_lens[i][0]), lastend))
                end = convert_time(int(chunk_lens[i][1]))
                lastend = int(chunk_lens[i][1])
                sen = stt("./splits" + each)
                os.remove("splits/" + each)
                if sen is None:
                    continue
                tmp = {'timeStart': start, 'timeEnd': end, 'sentence': sen[0]}
                text_list.append(tmp)
            os.remove("tmp.wav")
            os.removedirs("splits")
            with open(json_path, 'w', encoding='utf-8') as file_obj:
                json.dump(text_list, file_obj)

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
                        "sentence": "Only small clean accuracy drop is observed in the process. "
                    }
        '''
        base_name = os.path.basename(video_path)
        base_name = base_name.split('.')[:-1]
        name = base_name[0]
        for i in range(1, len(base_name)):
            name = name + '.' + base_name[i]
        tmp = os.path.abspath(os.path.join(video_path, '..', '..'))
        struct_path = os.path.join(tmp, 'videosstruct')
        json_path = os.path.join(struct_path, name + '.json')
        if os.path.exists(json_path):
            with open(json_path, 'r', encoding='utf-8') as fp:
                json_data = json.load(fp)
                return json_data
        else:
            raise Exception('[!] Json not found: ', json_path)

        videoToSpeech(video_path, target="tmp.wav")
        if os.path.exists('splits'):
            files = os.listdir('splits')
            for file in files:
                os.remove('./splits/' + file)
            os.removedirs('splits')
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
            tmp = {'timeStart': start, 'timeEnd': end, 'sentence': sentence}
            text_list.append(tmp)
        os.remove("tmp.wav")
        os.removedirs("splits")
        return text_list
