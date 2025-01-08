# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from scrapy.exceptions import DropItem
import pymysql


class AppliedPdpPipeline:
    def open_spider(self, spider):
        self.connection = pymysql.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="applied_com"
        )
        self.cursor = self.connection.cursor()
        self.table_name = f"products_{datetime.datetime.today().strftime('%Y%m%d')}"

        # Create table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url TEXT,
            product_code VARCHAR(255),
            mpn VARCHAR(255),
            product_name VARCHAR(255),
            product_price VARCHAR(255),
            categories TEXT,
            product_image VARCHAR(255),
            product_specs JSON,
            availability VARCHAR(255)
        );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        index_id = item.get('index_id')
        item.pop('index_id')
        # Insert into database
        query = f"""
        INSERT INTO {self.table_name} 
        (url, mpn, product_name, product_code, product_image, product_price, product_specs, categories, availability)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cursor.execute(query, (
                item.get("url"),
                item.get("mpn"),
                item.get("product_name"),
                item.get("product_code"),
                # item.get("product_description"),
                item.get("product_image"),
                item.get("product_price"),
                json.dumps(item.get("product_specs", {})),
                item.get("categories"),
                item.get("availability"),
            ))
            self.cursor.execute('update links set status="done" where id=%s', index_id)
            self.connection.commit()
        except Exception as e:
            spider.logger.error(f"Database error: {e}")
            # raise DropItem(f"Failed to save item: {item}")
        return item
