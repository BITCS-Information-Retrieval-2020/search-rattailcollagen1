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

|   模块   | 编号 |          负责人          |
| :------: | :------: | :----------------------: |
| DatabaseAccess |    0     |       **程赛华**       |
| DataProcess |    1     |      **聂宇翔**       |
| PDFProcessor |    1     | 孙凡淑 |
| VideoProcessor |    1     |   闻子涵          |
| ESClient |    2     |           **兰天**           |
| SearchEngine |    3     |     **王昊**     |
| DownloadClient |    3     | 孔祥宜  |
| DownloadServer |    3     | 公培元 |

## Installation
### Install
Our Python package is [here](https://pypi.org/project/ScienceSearcher/1.0.1/).
```shell
# Pip install the stable version of our package
pip install ScienceSearcher -i https://pypi.python.org/simple
```
### Get the latest version
```shell
# If you want to obtain the latest version of our package, please clone the repository
git clone https://github.com/BITCS-Information-Retrieval-2020/search-rattailcollagen1.git
cd search-rattailcollagen1
# Make sure you have the versions of dependencies in requirements.txt
pip install -r requirements.txt
python setup.py install
```
## Use the package in your Python
Please see `example.py`.
