import datetime
import json
import functools
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraManager:

    def __init__(self):
        cloud_config= {
            'secure_connect_bundle': 'secure-connect-scoopt.zip'
        }
        auth_provider = PlainTextAuthProvider('XSuETAMimsIkAMLUsaFBClry', 'INUB9G-p6Gvq2QGdNKWwwNcDbnk2lqpnHz+yc0E.OiCQk8vmGXsJgQIr-ccgZw_.tzTnu1M6D20CoUXPQZCoXnn5WFB16ZwX0Za+e2.zqy1jqXMjaYDqUfZj,sp-e5FE')
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()
        self.sources = []
        self.refresher = ""

    def get_table_indexes(self, table_name):
        posts = self.session.execute("select reverse_index from god." + table_name).all()
        cleanedPosts = {}
        for post in posts:
            nameFound = False
            while(nameFound == False):
                for name in self.sources:
                    if name in post.reverse_index:
                        if name not in cleanedPosts.keys():
                            cleanedPosts[name] = []
                        cleanedPosts[name].append(post.reverse_index.split(name)[0])
                        nameFound = True
                        break
                
                if nameFound == False:
                    if(not self.refresh_sources(post.reverse_index)):
                        break
        return cleanedPosts
        
    def refresh_sources(self, refresher):
        if(self.refresher == refresher):
            return False

        self.sources = self.get_sources()
        self.refresher = refresher

    def get_sources(self):
        sources = self.session.execute("SELECT source from god.posts_by_source GROUP BY source").all()
        parsedSources = []
        for sourceRow in sources:
            parsedSources.append(sourceRow.source)
        return parsedSources

#print(manager.session.execute("SELECT * FROM god.scoopt_category_index where reverse_index='LifestyleYOUTUBE'").all())
#manager.session.execute("CREATE TABLE god.posts_by_url(category list<text>, date date, full_text text, image text, is_trending boolean, is_video boolean, source text, summary text, tags list<text>, title text, url text, vid_url text, PRIMARY KEY (url))")
#manager.session.execute("CREATE TABLE god.posts_by_source(category frozen<list<text>>, date date, full_text text, image text, is_trending boolean, is_video boolean, source text, summary text, tags frozen<list<text>>, title text, url text, vid_url text, PRIMARY KEY ((source), is_trending, date, category, tags, url))")

print("DONE")


#print(len(manager.session.execute("select * from god.posts_by_title WHERE title CONTAINS 'Ukraine'").all()))
'''
example:
examplePostDict = {
    'url': 'https://www.youtube.com/watch?v=2G3WzwBFCpc',
    'title': 'Chancellor - better (Official Video)',
    !'date': '28-10-2015',
    !'full_text': 'CHANCELLOR  - better OFFICIAL M/V\n\nProduced by: Chancellor, the channels\nDirected by Pdeebell \n\nFor all lovers in long distance relationships.\n멀리서 사랑하는 모든 연인들에게.\n\nLove, Chancellor\n\nFollow Chancellor on:\n\nInstagram - https://instagram.com/chancellorofficial\nFacebook - https://www.facebook.com/dearchancellor\nTwitter - https://twitter.com/dearchancellor\nSoundcloud - https://soundcloud.com/chancelloroffi...\nContact - chance0618@gmail.com\n\nChancellor "MY FULL NAME" EP Coming Soon.',
    !'tags': '[]',
    !'source': 'YOUTUBE',
    !'is_video': 'true',
    !'is_trending': 'false',
    'vid_url': 'https://www.youtube.com/embed/2G3WzwBFCpc',
    !'image': 'https://www.img.youtube/vi/2G3WzwBFCpc/0.jpg',
    !'summary': '',
    !'category': '["Sports", "Finance", "World"]'
}
manager = CassandraManager()
manager.addPost(examplePostDict)
'''

