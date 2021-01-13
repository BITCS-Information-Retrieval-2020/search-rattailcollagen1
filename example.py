from ScienceSearcher.SearchEngine import SearchEngine

# initialization
se = SearchEngine(download_server_ip='xxx.xxx.xxx.xxx',
                  download_server_port=5000,
                  download_client_ip='xxx.xxx.xxx.xxx',
                  download_client_port=9001,
                  es_ip='xxx.xxx.xxx.xxx',
                  es_port=9200,
                  index_name='xxx',
                  video_index_name='xxx')

# search
query = {}
res = se.search(query)

# search by id
id = 1
res = se.search_by_id(id)

# auto-complete
query = "xxx"
res = se.auto_complete(query)

