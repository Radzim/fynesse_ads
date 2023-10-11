# from .config import *
import datetime
import pandas as pd
import yaml
import pymysql
import mysql.connector
import urllib.request
from math import cos, asin, sqrt, pi
import time

# This file accesses the data


def download_files(path='data/'):
    filenames = [f'pp-{i}-part{j}.csv' for i in range(1995, 2024) for j in [1, 2]]
    web_address = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/'
    for filename in filenames:
        print(web_address+filename)
        urllib.request.urlretrieve(web_address+filename, path+filename)


# def write_credentials(username, password):
#     with open("credentials.yaml", "w") as file:
#         yaml.dump({'username': username, 'password': password}, file)


def connect(url="database-ads.cgrre17yxw11.eu-west-2.rds.amazonaws.com", port=3306, database='property_prices'):
    with open("credentials.yaml") as file:
        credentials = yaml.safe_load(file)
    username = credentials["username"]
    password = credentials["password"]

    try:
        if database is None:
            return pymysql.connect(host=url, user=username, passwd=password, port=port, local_infile=True)
        else:
            return pymysql.connect(host=url, user=username, passwd=password, port=port, local_infile=True, database=database)
    except Exception as e:
        print(e)


def query(text, connection=None):
    close_connection = False
    if connection is None:
        connection = connect()
        close_connection = True
    cur = connection.cursor()
    cur.execute(text)
    connection.commit()
    query_results = cur.fetchall()
    if close_connection:
        cur.close()
        connection.close()
    return query_results


def create_db(name, connection=None):
    if connection is None:
        connection = connect(database=None)
    return query(f"CREATE DATABASE IF NOT EXISTS `{name}` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin", connection=connection)


def create_pp_data(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    query('USE `property_prices`;', connection=connection)
    query('DROP TABLE IF EXISTS `pp_data`;', connection=connection)
    query("""CREATE TABLE IF NOT EXISTS `pp_data` (
      `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
      `price` int(10) unsigned NOT NULL,
      `date_of_transfer` date NOT NULL,
      `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
      `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
      `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
      `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
      `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
      `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
      `street` tinytext COLLATE utf8_bin NOT NULL,
      `locality` tinytext COLLATE utf8_bin NOT NULL,
      `town_city` tinytext COLLATE utf8_bin NOT NULL,
      `district` tinytext COLLATE utf8_bin NOT NULL,
      `county` tinytext COLLATE utf8_bin NOT NULL,
      `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
      `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
      `db_id` bigint(20) unsigned NOT NULL
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;""", connection=connection)
    query('ALTER TABLE `pp_data` ADD PRIMARY KEY (`db_id`);', connection=connection)
    query('ALTER TABLE `pp_data` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;', connection=connection)


def create_postcode_data(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    query('USE `property_prices`;', connection=connection)
    query('DROP TABLE IF EXISTS `postcode_data`;', connection=connection)
    query("""CREATE TABLE IF NOT EXISTS `postcode_data` (
      `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
      `status` enum('live','terminated') NOT NULL,
      `usertype` enum('small', 'large') NOT NULL,
      `easting` int unsigned,
      `northing` int unsigned,
      `positional_quality_indicator` int NOT NULL,
      `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
      `latitude` decimal(11,8) NOT NULL,
      `longitude` decimal(10,8) NOT NULL,
      `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
      `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
      `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
      `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
      `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
      `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
      `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
      `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
      `db_id` bigint(20) unsigned NOT NULL
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;""", connection=connection)
    query('ALTER TABLE `postcode_data` ADD PRIMARY KEY (`db_id`);', connection=connection)
    query('ALTER TABLE `postcode_data` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;;', connection=connection)


def upload_files(path='data/'):
    filenames = [f'pp-{i}-part{j}.csv' for i in range(1995, 2024) for j in [1, 2]]
    for filename in filenames:
        print(filename)
        success = False
        while not success:
            try:
                query(f"""LOAD DATA LOCAL INFILE '{path+filename}' INTO TABLE `pp_data` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"' LINES STARTING BY '' TERMINATED BY '\n';""")
                success = True
            except Exception as e:
                print(e)
                time.sleep(10)


def upload_postcode_file(path='data/'):
    query(f"""LOAD DATA LOCAL INFILE '{path}open_postcode_geo.csv' INTO TABLE `postcode_data` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"' LINES STARTING BY '' TERMINATED BY '\n';""")


def make_pp_indices(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    query('USE `property_prices`;', connection=connection)
    query('CREATE INDEX `pp.postcode` USING HASH ON `pp_data` (postcode);', connection=connection)
    query('CREATE INDEX `pp.date` USING HASH ON `pp_data` (date_of_transfer);', connection=connection)


def make_postcode_indices(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    query('USE `property_prices`;', connection=connection)
    query('CREATE INDEX `po.postcode` USING HASH ON `postcode_data` (postcode);', connection=connection)
    query('CREATE INDEX `po.coordinates` USING BTREE ON `postcode_data` (`latitude`, `longitude`);', connection=connection)


def create_prices_coordinates_data(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    query('USE `property_prices`;', connection=connection)
    query('DROP TABLE IF EXISTS `prices_coordinates_data`;', connection=connection)
    query("""CREATE TABLE IF NOT EXISTS `prices_coordinates_data` (
      `price` int(10) unsigned NOT NULL,
      `date_of_transfer` date NOT NULL,
      `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
      `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
      `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
      `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
      `locality` tinytext COLLATE utf8_bin NOT NULL,
      `town_city` tinytext COLLATE utf8_bin NOT NULL,
      `district` tinytext COLLATE utf8_bin NOT NULL,
      `county` tinytext COLLATE utf8_bin NOT NULL,
      `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
      `latitude` decimal(11,8) NOT NULL,
      `longitude` decimal(10,8) NOT NULL,
      `db_id` bigint(20) unsigned NOT NULL
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;""", connection=connection)
    query('ALTER TABLE `prices_coordinates_data` ADD PRIMARY KEY (`db_id`);', connection=connection)
    query('ALTER TABLE `prices_coordinates_data` MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;;', connection=connection)


def populate_prices_coordinates(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    count = query("SELECT COUNT(*) FROM pp_data", connection=connection)[0][0]
    done_count = query("SELECT COUNT(*) FROM prices_coordinates_data", connection=connection)[0][0]
    print(count, done_count, ((done_count-1)//1000000+1)*1000000)
    done_count = ((done_count-1)//1000000+1)*1000000
    chunks = (count-done_count-1)//1000000+1
    print(time.time(), count, chunks)
    for i in range(chunks):
        print(time.ctime(), done_count+i*1000000+1, '-', done_count+(i+1)*1000000)
        query(f"""
            INSERT INTO prices_coordinates_data (price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county, country, latitude, longitude, db_id)
            SELECT price, date_of_transfer, s1.postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county, country, s2.latitude, s2.longitude, s1.db_id
            FROM (SELECT * FROM pp_data LIMIT {done_count+i*1000000},{1000000}) s1
            INNER JOIN postcode_data s2 ON s2.postcode = s1.postcode
        """, connection=connection)
    query('ALTER TABLE `prices_coordinates_data` ADD PRIMARY KEY (`db_id`);', connection=connection)


def make_prices_coordinates_indices(connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    query('USE `property_prices`;', connection=connection)
    query('CREATE INDEX `pc.postcode` USING HASH ON `prices_coordinates_data` (postcode);', connection=connection)
    query('CREATE INDEX `pc.coordinates` USING BTREE ON `prices_coordinates_data` (`latitude`, `longitude`);', connection=connection)

print(query("SHOW FULL PROCESSLIST"))
query("KILL 8278")
print(query("SHOW FULL PROCESSLIST"))
# print(query("""SELECT now()"""))
# print(time.ctime(), 'start')
# create_db('property_prices')
# print(time.ctime(), 'create pp_data')
# create_pp_data()
# print(time.ctime(), 'download')
# download_files()
# print(time.ctime(), 'upload')
# upload_files()
# print(time.ctime(), 'index pp_data')
# make_pp_indices()
# print(time.ctime(), 'create postcode_data')
# create_postcode_data()
# print(time.ctime(), 'download')
# # manually download and unpack the postcode file
# print(time.ctime(), 'upload')
# upload_postcode_file()
# print(time.ctime(), 'index postcode_data')
# make_postcode_indices()
# print(time.ctime(), 'create prices_coordinates')
# create_prices_coordinates_data()
# print(time.ctime(), 'join and populate prices_coordinates')
# populate_prices_coordinates()
# print(time.ctime(), 'index prices_coordinates')
# make_prices_coordinates_indices()
# print(time.ctime())
# print(query("""
#     SELECT COUNT(*) FROM pp_data
# """))
# print(query("""
#     SELECT COUNT(*) FROM postcode_data
# """))
# print(query("""
#     SELECT COUNT(*) FROM prices_coordinates_data
# """))
# print(query("""
#     SELECT * FROM prices_coordinates_data LIMIT 100
# """))


def distance(lat1, lon1, lat2, lon2):
    p = pi / 180
    a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a))


def get_box(location, range_km):
    reference_point = (52, 0)
    latitude_angle_to_km = distance(reference_point[0] - 0.5, reference_point[1], reference_point[0] + 0.5, reference_point[1])
    longitude_angle_to_km = distance(reference_point[0], reference_point[1] - 0.5, reference_point[0], reference_point[1] + 0.5)
    box_lat = range_km / latitude_angle_to_km
    box_lon = range_km / longitude_angle_to_km
    return location[1] - box_lon / 2, location[1] + box_lon / 2, location[0] - box_lat / 2, location[0] + box_lat / 2


def get_house_prices(location, date, range_km, range_years, property_type, connection=None):
    if connection is None:
        connection = connect(database='property_prices')
    lon_0, lon_1, lat_0, lat_1 = get_box(location, range_km)
    datetime_object = datetime.datetime.fromisoformat(date)
    timedelta_object = datetime.timedelta(days=range_years * 365 // 2)
    date_0, date_1 = str(datetime_object - timedelta_object)[:10], str(datetime_object + timedelta_object)[:10]
    return get_house_prices_inner(connection, lat_0, lat_1, lon_0, lon_1, date_0, date_1, property_type)


def get_house_prices_inner(connection, lat_0, lat_1, lon_0, lon_1, date_0, date_1, prop_type):
    q = f"""
      SELECT a_t.`date_of_transfer`, pcd.`postcode`, pcd.`lattitude`, pcd.`longitude`, a_t.`property_type`, a_t.`price` FROM
      (SELECT `postcode`, `date_of_transfer`, `property_type`, `price` FROM `pp_data`
      WHERE `postcode` IN 
      (SELECT `postcode` from `postcode_data`
      WHERE `lattitude` BETWEEN {lat_0} AND {lat_1}
      AND `longitude` BETWEEN {lon_0} AND {lon_1})
      AND `date_of_transfer` BETWEEN '{date_0}' AND '{date_1}'
      AND `property_type` = '{prop_type}') a_t
      INNER JOIN
      `postcode_data` pcd
      ON (pcd.`postcode`= a_t.`postcode`)
      """
    print('here')
    print(query(q))
    df = pd.read_sql(q, connection)
    for col in ['postcode', 'property_type']:
        df[col] = df[col].apply(lambda x: x.decode("utf-8"))
    return df


# a = get_house_prices((52.214, 0.1), '2015-01-01', 1, 1, 'D')
# print(a)
# print(query("SELECT * from `pp_data` limit 10"))
# print(query("SELECT COUNT(*) FROM `pp_data`"))
# print(query("SELECT `postcode` from `pp_data` WHERE `postcode` IN (SELECT `postcode` from `postcode_data` WHERE `lattitude` BETWEEN 52.2 AND 52.22 AND `longitude` BETWEEN 0.09 AND 0.11)"))
# print(query("SELECT COUNT(*) FROM `postcode_data`"))

# print(query("""
#     SELECT COUNT(*) FROM prices_coordinates_data
# """))