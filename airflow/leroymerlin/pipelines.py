# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
import scrapy
from pymongo import MongoClient

class LeroymerlinPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongobase = client['LeroyMerlin']

    def process_item(self, item, spider):
        collection = self.mongobase[spider.name]
        collection.update_one({'link': item.get('link')}, {'$set': item},  upsert=True)
        return item

class LeroyMerlinPhotoPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        if item['images']:
            for img in item['images']:
                try:
                    yield scrapy.Request(img)
                except Exception as e:
                    print(e)

    def item_completed(self, results, item, info):
        if results:
            item['images'] = [itm[1] for itm in results if itm[0]]
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        
        return 'строка с путем, именем и расширением для сохранения файла'