# 检索模块

从MongoDB中读取爬虫爬到的数据，建立索引，实现综合检索。

## 队伍名称
**ratTailCollagen1**

<div style="text-align:center"><img src="./images/logo.png" height="400px" width="400px"></div>

## 项目成员

聂宇翔，王昊，兰天，程赛华，孙凡淑，公培元，孔祥宜，闻子涵

## 注意事项
1. 所有**大文件**（包括pdf，Video等）一律组织在`/search/data`文件夹下, 并将相应文件路径添加进`.gitignore`
2. 我们的所有代码均组织在`/search`文件夹中，之后将作为Python package打包封装。因此关于各个功能模块测试的工作均需要在`/search`这个文件夹之外进行
3. 测试代码文件请勿上传到github中（相应文件写入到`.gitignore`即可）
4. 约定好的接口调用方式请勿随意更改，如想要变动，请联系相应部分的负责同学沟通确认
5. github上的代码严格进行版本控制，如果是通过命令行执行版本控制，那么请首先git clone这个项目到本地，然后自建一个分支，在自建分支上进行修改，修改结束后先转回master分支，并从github上pull下来一个最新版本的master，然后在本地将自建分支merge到master分支上，以便解决潜在的冲突，最后再将本地的master分支push到github上；此外也可以选择直接编辑github中相应的.py文件。
6. `backup`分支请勿修改。
7. `/search`文件夹下仅保存所有的功能模块，如果要对自己的代码进行测试，请在该文件夹以外的地方进行。
8. 完成自己的模块之后，请自行使用`flake8`检测自己的代码风格是否符合规范。
9. 模块分配情况表（不同组的负责同学加粗表示）：

|   模块   | 编号 |          负责人          | 分工 |
| :------: | :------: | :----------------------: | :-------: |
| DatabaseAccess |    0     |       **程赛华**       | ... |
| DataProcess |    1     |      **聂宇翔**       | ... |
| PDFProcessor |    1     | 孙凡淑 | ... |
| VideoProcessor |    1     |   闻子涵          | ... |
| ESClient |    2     |           **兰天**           | ... |
| SearchEngine |    3     |     **王昊**     | ... |
| DownloadClient |    3     | 孔祥宜  | 辅助王昊同学 |
| DownloadServer |    3     | 公培元 | 辅助王昊同学 |

## 代码功能说明
### DatabaseAccess

#### 功能概述

    对接爬虫模块与检索模块

+ 连接本地、远程数据库
+ 支持爬虫增量更新数据
+ 支持批读取论文数据
+ 支持批导入论文数据
+ 支持本地数据库进行模块测试
+ 支持异常捕获

#### 模块内容及接口定义

1. 初始：连接数据库（本地、远程）

    |参数名|参数类型|参数含义|缺省值|
    | :-: | :-: | :-: | :-: |
    |service_path|'mongodb://user:password@address:port/service_name' | 数据库连接字串|'mongodb://127.0.0.1:27017'|
    |service_name|string|数据库名称|'mongodb'|
    |collection_name|string|文档集合名称|'papers'|

        其中对于service_path:

          + user: 数据库用户名
          + password: 数据库用户密码
          + address: 远程数据库IP地址
          + port: 数据库远程访问端口

2. 参数：增量数据区间（起始、终止索引）

    支持爬虫模块增量爬取数据，支持故障恢复

    |参数名|参数类型|参数含义|缺省值|
    | :-: | :-: | :-: | :-: |
    |increment_beginning_pointer|int|区间起始索引|-1|
    |increment_ending_pointer|int|区间终止索引|`None`|

3. 接口：在增量数据区间内按批读取数据

    ```python
        def read_batch(batch_size=1):
    ```

    |参数名|参数类型|参数含义|缺省值|
    | :-: | :-: | :-: | :-: |
    |batch_size|int|批大小|1|
    |end_flag|bool|本次增量的终止标志|`False`|

4. 接口：按文件导入本地json数据

    用于本地数据库测试模块功能

    ```python
        def import_json_db(db_path='./data/papers.json', drop_flag=False)
    ```

    |参数名|参数类型|参数含义|缺省值|
    | :-: | :-: | :-: | :-: |
    |db_path|string:path|本地json数据库的路径|`'./data/papers.json'`|
    |drop_flag|bool|是否重置demo数据库|`False`|

### PDFProcessor模块
#### 模块功能概述
从给定论文pdf文件中提取出从introduction到conclusion部分的全部文本内容，返回字符串。
#### 模块工作流程
1. 访问GROBID服务器将论文转换成对应的XML文件
2. 从XML文件中提取出论文introduction到conclusion的全部内容，以字符串形式返回。

### VideoProcessor模块
#### 模块功能概述
VideoProcessor模块用于对传入的视频进行转换，转换为一系列句子，包括每个句子的起始时间与结束时间。模块的输入是video_path，输出是一个list，其中每一个元素是一个dict，该dict的格式为 (下面这个仅是一个例子)：  
```json
    {
        "timeStart": "hh-mm-ss",
        "timeEnd": "hh-mm-ss",
        "sentence": "Only small clean accuracy drop is observed in the process. "
    }
```
#### 模块工作流程
1. 首先使用moviepy.editor中的VideoFileClip对视频提取音频
2. 使用pydub中的AudioSegment对上一步提取的音频进行按照句子进行分割，并给出每个句子的起始与结束时间
3. 使用百度AipSpeech对上一步提取的句子音频转化为文本
4. 按照dict形式构造每个句子并存入list中返回

### DataProcess模块
#### 模块功能概述
调用DatabaseAccess, PDFProcessor以及VideoProcessor模块，接收同这些模块中传递过来的数据，并将这些数据（通过ESClient模块）写入ElasticSearch中
#### 模块工作流程
1. 分别调用PDFProcessor和VideoProcessor，从而对现有的未被处理的pdf和videos进行处理。
2. 利用DatabaseAccess，从远程MongoDB获取数据，并对这三者数据进行整合，将整合后的结果送入到ESClient中
3. 检查是否存在新增的数据文件夹，如果存在，则进行增量式更新


### ESClient模块
#### 模块功能概述
ESClient通过使用elasticsearch搜索工具，根据用户指定的query查询出的搜索结果并返回给展示组。在ESClient中，包含有两个查询index，分别是papers index和video index，分别存储爬虫组收集到的论文的信息和每个论文对应的视频中的字幕信息。通过约束给定的搜索方式实现4种不同的查询功能：（1）按照id查询；（2）全部字段搜索；（3）bool检索；（4）视频字幕查询。
#### 模块工作流程
1. 分别构建paper index和video index存储论文的数据信息和对应的视频信息。
2. 按照id查询对应的论文：通过使用指定的paper id查询对应的论文。
3. 按照全部字段查询：通过对所有的文本field使用copy to属性，定义一个全局的查询域，实现对所有字段的查询，具有最多的相关文本信息的论文会得到更靠前的排名顺序
4. bool检索：bool检索分别知识AND, OR, NOT三种bool查询，并且可以针对不同的字段设置不同的bool约束。AND operator要求字段必须出现对应的关键词，OR operator要求字段可能出现对应的关键词，并且会根据频率进行打分，NOT operator用来拒绝某些查询结果。并且AND, OR, NOT三种查询结果都支持部份匹配，该功能通过使用通配符实现。
5. 视频字幕查询：每一个论文对应的视频中都有字幕信息，该接口支持查询匹配的字幕信息，获取到字幕信息之后可以获得对应的字幕的时间戳，字幕文本和对应的paper id，通过按照id查询接口可以获取到字幕对应的论文的更多信息。

### DownloadServer模块
#### 模块功能概述
向客户端传输数据。
#### 模块工作流程
1. 启动服务，监听客户端发送的查询请求。
2. 判断是否需要向客户端发送文件。
3. 如果需要传输文件，则新建一个子线程向客户端发送文件，主线程继续监听。

### DownloadClient模块
#### 模块功能概述
从存放数据的服务器同步数据至本地，同时定期进行查询以实现增量式更新。
#### 模块工作流程
1. 检查目标路径是否存在，不存在则递归创建路径
2. 检查是否需要对文件进行更新
3. 如果需要文件更新，则调用接收函数，准备接受服务器端传输的文件

### SearchEngine模块
#### 模块功能概述
封装对索引的访问，提供相应的查询接口供展示组使用；实现数据在服务器端和展示组本地的同步。
#### 模块工作流程
1. 初始化DownloadClient模块，以阻塞的方式将数据同步到本地。
2. 同步论文标题列表。
3. 初始化ESClient
4. 搜索
    - 调用ESClient进行检索
    - 将检索结果中的文件路径进行补全并返回
5. 自动补全
    - 根据query进行正则匹配，返回符合要求的标题列表

