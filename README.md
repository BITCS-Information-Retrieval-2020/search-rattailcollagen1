# 检索模块

从MongoDB中读取爬虫爬到的数据，建立索引，实现综合检索。

## 队伍名称
**ratTailCollagen1**

<div style="text-align:center"><img src="./images/logo.png" height="400px" width="400px"></div>

## 项目成员

聂宇翔，王昊，兰天，程赛华，孙凡淑，公培元，孔祥宜，闻子涵

## 注意事项
1. 所有**大文件**一律组织在`/search/data`文件夹下, 并将相应路径添加进`.gitignore`
2. 我们的所有代码均组织在`/search`文件夹中，之后将作为Python package打包封装。因此关于各个功能模块测试的工作均需要在`/search`这个文件夹之外进行
3. 测试代码文件请勿上传到github中（相应文件写入到`.gitignore`即可）
4. 约定好的接口调用方式请勿随意更改，如想要变动，请联系相应部分的负责同学沟通确认
5. github上的代码严格进行版本控制，如果是通过命令行执行版本控制，那么请首先git clone这个项目到本地，然后自建一个分支，在自建分支上进行修改，修改结束后先转回master分支，并从github上pull下来一个最新版本的master，最后在本地将自建分支merge到master分支上，以便解决潜在的冲突。最后再将本地的master push到github上。
6. 模块分配情况表（不同组的负责同学加粗表示）：

|   模块   | 编号 |          负责人          |
| :------: | :------: | :----------------------: |
| DatabaseAccess |    0     |       **程赛华**       |
| DataProcess |    1     |      **聂宇翔**       |
| PDFProcessor |    1     | XXX |
| VideoProcessor |    1     |   XXX          |
| ESClient |    2     |           **兰天**           |
| SearchEngine |    3     |     **王昊**     |
| DownloadClient |    3     | XXX  |
| DownloadServer |    3     | XXX |