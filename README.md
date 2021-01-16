# ScienceSearcher
[![Package](https://img.shields.io/pypi/v/ScienceSearcher)](https://pypi.org/project/ScienceSearcher)
[![Stars](https://img.shields.io/github/stars/BITCS-Information-Retrieval-2020/search-rattailcollagen1?style=social)](https://github.com/BITCS-Information-Retrieval-2020/search-rattailcollagen1/stargazers)
[![Forks](https://img.shields.io/github/forks/BITCS-Information-Retrieval-2020/search-rattailcollagen1?style=social)](https://github.com/BITCS-Information-Retrieval-2020/search-rattailcollagen1/network/members)

**系统介绍** | [项目信息](https://github.com/BITCS-Information-Retrieval-2020/Course-Project/blob/main/README.md)| [关于我们](./about.md)  

一款支持增量式数据更新的科技论文检索系统  

## 系统说明

### 系统特性
* **一行代码**即可完成服务端部署
* **一行代码**即可完成客户端安装
* 简易的客户端接口使用
* PDF**多线程**并发解析
* 检索系统增量式动态更新
* 检索系统快速恢复
* 可同时与**多个爬虫模块**实现交互
* PDF和视频文件采用“**offline处理+online取用**”方案，复用数据解析结果，利于系统的快速恢复

### 系统架构
#### 整体架构

本系统的整体架构如下图所示：
![系统架构](./images/architecture.png)

#### 接口约定情况

在接口约定方面，我们以数据流的形式为基础，通过json的格式实现了接口约定：

- 组内接口约定
  - [toES.json](./ScienceSearcher/data/dataFlow/toES.json): 该文件定义了数据在elasticsearch内索引中的存储结构

- 组间接口约定
  - [fromCrawler.json](./ScienceSearcher/data/dataFlow/fromCrawler.json): 该文件定义了从爬虫模块MongoDB server中取出来的数据结构
  - [fromWebsite.json](./ScienceSearcher/data/dataFlow/fromWebsite.json): 该文件定义了展示模块发出请求的数据类型
  - [toWebsite.json](./ScienceSearcher/data/dataFlow/toWebsite.json): 该文件定义了向展示模块反馈的数据类型


#### 组间协作情况
|   组名   |   组长   | 分工 |
| :------: | :------: | :------: |
| HandsomeGuys | 方致远 |    从各个论文网站上爬取多模态数据，将pdf和视频等磁盘文件按增量批次传递给检索模块，并将相关元数据同步更新给MongeDB服务   |
| ICanOnlyCrawl | 何亮丽 |    从各个论文网站上爬取多模态数据，将pdf和视频等磁盘文件按增量批次传递给检索模块，并将相关元数据同步更新给MongeDB服务     |
| ratTailCollagen1 | 聂宇翔 |    接收来自于爬虫模块的增量更新，并进行处理，将处理后的结果放入elasticsearch服务的相应索引中，并为展示组提供数据展示的接口     |
| day2night | 刘雨程 |    调用检索模块的接口，展示检索的结果     |

#### 代码目录结构

如下所示，为我们项目的组织情况。其中`ScienceSearcher`为我们的包名。其中第一层存储了所有的功能类。在第二层中`grobid-client`中放置了访问grobid server相关的所有功能模块。`data`文件夹里放置的是`cache`和`dataFlow`文件夹，其中`dataFlow`文件夹中存放了所有的接口数据类型约定信息，而`cache`文件夹中存放了所有的硬盘文件，它们首先按（爬虫）组进行了划分，在组文件夹的内部，又按照批次进行了划分，在每一批次内部有两个文件夹`PDFs`和`videos`，分别存放pdf文件和视频文件。
```shell
.
├── images
└── ScienceSearcher
    ├── data
    │   ├── cache
    │   │   ├── demo
    │   │   │   ├── 1
    │   │   │   │   ├── PDFs
    │   │   │   │   ├── videos
    │   │   │   │   ├── videosstruct
    │   │   │   │   └── XMLs
    │   │   │   └── 2
    │   │   │       ├── PDFs
    │   │   │       ├── videos
    │   │   │       ├── videosstruct
    │   │   │       └── XMLs
    │   │   ├── fzy
    │   │   │   ├── 1
    │   │   │   │   ├── PDFs
    │   │   │   │   ├── videos
    │   │   │   │   ├── videosstruct
    │   │   │   │   └── XMLs
    │   │   │   └── 2
    │   │   │       ├── PDFs
    │   │   │       ├── videos
    │   │   │       ├── videosstruct
    │   │   │       └── XMLs
    │   │   └── hll
    │   │       └── 1
    │   │           ├── PDFs
    │   │           ├── videos
    │   │           └── XMLs
    │   └── dataFlow
    └── grobid_client
```

### 部署方法

#### 部署前的准备工作

注：下列服务可在同一台服务器上部署，也可在不同服务器上分别部署  
* 在一台服务器上部署MongoDB服务，并获取访问该MongoDB服务所需要的`USER`, `PASSWORD`, `ADDRESS`, `PORT`, `SERVICE_NAME`以及`COLLECTION_NAME`。
* 在一台服务器上部署Elasticsearch服务，并获取访问该服务所需要的`ES_IP`和`ES_PORT`。
* 在一台服务器上部署Grobid Server，并获取该服务所在的IP地址`PDF_IP`以及端口`PDF_PORT`。

#### 服务端
推荐采用Linux系统，您也可以选择Windows系统来部署。  
首先从github上clone我们的项目，并安装依赖包
```shell
# If you want to obtain the latest version of our package, please clone the repository
git clone https://github.com/BITCS-Information-Retrieval-2020/search-rattailcollagen1.git
cd search-rattailcollagen1
# Make sure you have the versions of dependencies in requirements.txt
pip install -r requirements.txt
```
然后通过一行代码进行服务端的部署：
```shell
python run.py  --mode process --pdf_ip PDF_IP --pdf_port PDF_PORT --mongodb_service_path mongodb://USER:PASSWORD@ADDRESS:PORT/SERVICE_NAME --mongodb_service_name SERVICE_NAME --mongodb_collection_name COLLECTION_NAME --es_ip ES_IP --es_port ES_PORT --es_index_name ES_INDEX_NAME --cache_name GROUP_NAME
```
您可以选择使用`shell`文件来保存该行命令，从而省去手动输入代码的麻烦。然后将该命令中的**大写名称**替换为相应的数值。  
名称说明如下：
|   名称   | 说明 |
| :------: | :------: |
| USER |    ...     |
| PASSWORD |    ...     |
| ADDRESS |    ...     |
| PORT |    ...     |
| SERVICE_NAME |    ...     |
| COLLECTION_NAME |    ...     |
| ES_IP |    ...     |
| ES_PORT |    ...     |
| ES_INDEX_NAME |    所使用的Elasticsearch索引名称     |
| PDF_IP |    Grobid Server所在服务器的IP地址     |
| PDF_PORT |    Grobid Server所在服务器的端口号     |
| GROUP_NAME |    所对应的爬虫组名称     |
| SERVER_IP | 部署该系统的服务器的IP地址 |
| SERVER_PORT | 部署该系统的服务器的端口号 |

其中，当[部署前的准备工作](#部署前的准备工作)做好之后，用户只需要自行拟定`ES_INDEX_NAME`和`GROUP_NAME`两项即可。
这两项指定了我们的系统所要服务的crawler是哪一个。如[代码目录结构](#代码目录结构)一节中所示，其中`/ScienceSearcher/data/cache/fzy`，`/ScienceSearcher/data/cache/fzy`和`/ScienceSearcher/data/cache/hll`为我们建立的三个示例文件夹，它们服务于三个爬虫模块。您也可以选择建立其他的为爬虫服务的文件夹。如果我们的本次的运行服务于爬虫`demo`，那么我们需要将`ES_INDEX_NAME`指定为`demo_papers`, 并将`GROUP_NAME`指定为`demo`。

#### 客户端
只需要一行代码即可以实现客户端安装。
```shell
pip install ScienceSearcher -i https://pypi.python.org/simple
```
客户端使用的一个简单例子(以`demo`爬虫系统为例，完整的使用示例参见[example.py](./example.py))为：
```python
from ScienceSearcher.SearchEngine import SearchEngine
# initialization
se = SearchEngine(download_server_ip='SERVER_IP',
                  download_server_port=SERVER_PORT,
                  download_client_ip='0.0.0.0',
                  download_client_port=9001,
                  es_ip='ES_IP',
                  es_port=ES_PORT,
                  index_name='demo_papers',
                  video_index_name='demo_papers_video',
                  group_name='demo')

query = "machine"
# 查询含有"machine"的标题
res = se.auto_complete(query)
```


## 未来的工作
* 视频多线程并行处理
* 在服务端部署音频-文字转换模型，减少网络通信的开销
* 当MongoDB服务出现问题时，可实现检索系统按批次回滚操作
* PDF与视频文件利用脚本进行自动化传输（使整个系统完全自动化运行）