#!/usr/bin/env python
import sys
import os
import datetime
import csv
import shutil
import json
from pymongo import MongoClient
from functools import reduce


DEFAULT_DB = "cdisearch"
DEFAULT_COLL = 'fulldb'

CURRENT_PATH  = '../data/current'
ARCHIVE_PATH  = '../data/archive'

TYPE_LIST = 1
TYPE_AGG = 2
TYPE_DUMP = 3



def aggregate_double_fields(coll, fieldname_1, name_1, fieldname_2, name_2, unwind_1: False, unwind_2: False):
    print('aggregate by fields %s and %s' % (fieldname_1, fieldname_2))
    query = []
    if unwind_1: 
        query.append({'$unwind' : "$" + fieldname_1.rsplit('.', 1)[0]})
    if unwind_2:
        query.append({'$unwind' : "$" + fieldname_2.rsplit('.', 1)[0]})

    query.append({ "$group": {"_id": { name_1: "$" + fieldname_1, name_2 : "$" + fieldname_2 },"count": { "$sum": 1 }}})
    cursor = coll.aggregate(query, allowDiskUse=True)
    data = list(cursor)

    return data


def aggregate_field(coll, fieldname):
    print('aggregate by field %s' % (fieldname))
    cursor = coll.aggregate([{ "$group": {"_id": "$" + fieldname,"count": { "$sum": 1 }}}], allowDiskUse=True) 

    data = list(cursor)
    print(data)

    result = reduce(lambda x,y: dict(list(x.items()) + list({ y['_id']: y['count'] }.items())), data,{})
    return result


def aggregate_field_unwind(coll, fieldname):
    print('aggregate by field %s' % (fieldname))
    cursor = coll.aggregate([{ '$unwind' : "$%s" % fieldname.rsplit('.', 1)[0]}, {"$group": {"_id": "$" + fieldname ,"count": { "$sum": 1 }}}], allowDiskUse=True) 

    data = list(cursor)

    result = reduce(lambda x,y: dict(list(x.items()) + list({ y['_id']: y['count'] }.items())), data,{})
    return result


def aggregate_array(coll, fieldname):
    print('aggregate array %s' % (fieldname))
    cursor = coll.aggregate([{'$unwind' : '$' + fieldname}, { "$group": {"_id": "$" + fieldname, 
"count": { "$sum": 1 }
#"%s" % fieldname.rsplit('.', 1)[-1] : {"$first" : "$" + fieldname},
}}], allowDiskUse=True)
    data = list(cursor)
#    print(data)
#    for row in data:
#        print(row['_id'])

#    print(data)
    result = reduce(lambda x,y: dict(list(x.items()) + list({ y['_id']: y['count'] }.items())), data,{})
    return result


def save_current(data):
#    print(datalist)
    for record in [data, ]:
        f = open(os.path.join(CURRENT_PATH, record[0] + '.json'), 'w', encoding='utf8')
        f.write(json.dumps(record[1]))
        f.close()
        f = open(os.path.join(CURRENT_PATH, record[0] + '.csv'), 'w', encoding='utf8')
        if record[2] == TYPE_LIST:
            f.write('value\n')
            for row in record[1]:
                f.write(str(row) + '\n')
        elif record[2] == TYPE_DUMP:
            keys = record[1][0]["_id"].keys()
            headers = list(keys)
            headers.append('count')
            writer = csv.writer(f)
            writer.writerow(headers)
            data_ = sorted(record[1], key=lambda item: item['count'], reverse=True)
            for row in data_:
                r = []
                for k in keys:
                    r.append(row['_id'][k])
                r.append(row['count'])
                writer.writerow(r)               
        elif record[2] == TYPE_AGG:
            data = record[1]
            data_ = sorted(data.items(), key=lambda item: item[1], reverse=True)
            writer = csv.writer(f)
            writer.writerow(['name', 'count'])
            for row in data_:
                writer.writerow(row)   
        f.close()
        print('saved %s' % (record[0]))
        pass

def save_archive():
    today = datetime.datetime.now()
    daypath = '%d-%d-%d' % (today.year, today.month, today.day)
    dirname = os.path.join(ARCHIVE_PATH, daypath)
    os.makedirs(dirname, exist_ok=True)
    filenames = os.listdir(CURRENT_PATH)   
    for name in filenames:
        shutil.copyfile(os.path.join(CURRENT_PATH, name), os.path.join(dirname, name))
    f = open(os.path.join(dirname, 'state.json'), 'w', encoding='utf8')
    f.write(json.dumps({'generated' : daypath}))
    f.close()
    cwd = os.getcwd()
    os.chdir(dirname)
    os.system('gzip -9 *.csv')
    os.system('gzip -9 *.json')
    os.chdir(cwd)
    pass



CONTINENTS_MAP = {
'Western Europe' : 'Europe',
'Northern America' : 'North America',
'Australia and New Zealand' : 'Australia',
'Northern Europe' : 'Europe',
'Southern Europe' : 'Europe',
'Eastern Europe' : 'Europe',
'South America' : 'South America',
'South-eastern Asia' : 'Asia',
'Eastern Europe' : 'Europe',
'Southern Asia' : 'Asia',
'Eastern Asia' : 'Asia',
'Central America' : 'North America',
'Western Asia' : 'Asia',
'Antarctica' : 'Antarctica',
'Central Asia' : 'Asia',
'Northern Africa' : 'Africa',
'Western Africa' : 'Africa',
'Eastern Africa' : 'Africa',
'Caribbean' : 'North America',
'Melanesia' : 'Australia',
'Polynesia' : 'Australia',
'Micronesia' : 'Australia',
'Southern Africa' : 'Africa',
'Middle Africa' : 'Africa'
}


def custom_update_continents():
    f = open(os.path.join(CURRENT_PATH, 'stats_macroregions.json'), 'r', encoding='utf8')
    data = json.load(f)
    f.close()
    results = {}
    for k,v in data.items():
        if k in CONTINENTS_MAP.keys():
            if CONTINENTS_MAP[k] in results.keys():
                results[CONTINENTS_MAP[k]] += v
            else:
                results[CONTINENTS_MAP[k]] = v
        pass
    save_current(['stats_continents', results, TYPE_AGG])


def run():    
    conn = MongoClient()  # For test environment only, production protected with auth
    coll = conn[DEFAULT_DB][DEFAULT_COLL]
  
    data_list = []
    save_current(['stats_res_d_mime', aggregate_field_unwind(coll, 'resources.d_mime'), TYPE_AGG])
    save_current(['stats_res_mimetypes', aggregate_field_unwind(coll, 'resources.mimetype'), TYPE_AGG])
    save_current(['stats_res_formats', aggregate_field_unwind(coll, 'resources.format'), TYPE_AGG])
    save_current(['stats_res_d_ext', aggregate_field_unwind(coll, 'resources.d_ext'), TYPE_AGG])

    save_current(["stats_country_type", aggregate_double_fields(coll, 'source.countries.name', 'country', 'source.catalog_type', 'catalog_type', unwind_1=True, unwind_2=False), TYPE_DUMP])
    save_current(["stats_country_software", aggregate_double_fields(coll, 'source.countries.name', 'country', 'source.software.name', 'software', unwind_1=True, unwind_2=True), TYPE_DUMP])
    save_current(["stats_country_owner", aggregate_double_fields(coll, 'source.countries.name', 'country', 'source.owner_type', 'owner_type', unwind_1=True, unwind_2=False), TYPE_DUMP])

    save_current(['crawledsources', coll.distinct('source.uid'), TYPE_LIST])
    save_current(['stats_software', aggregate_field_unwind(coll, 'source.software.name'), TYPE_AGG])
    save_current(['stats_langs', aggregate_field_unwind(coll, 'source.langs.name'), TYPE_AGG])
#    save_current(['stats_countries', aggregate_field_unwind(coll, 'source.countries.name'), TYPE_AGG])
    save_current(['stats_subregions', aggregate_field_unwind(coll, 'source.subregions.name'), TYPE_AGG])
    save_current(['stats_macroregions', aggregate_field_unwind(coll, 'source.macroregions.name'), TYPE_AGG])
    save_current(['stats_topics', aggregate_array(coll, 'dataset.topics'), TYPE_AGG])
    save_current(['stats_geotopics', aggregate_array(coll, 'dataset.geotopics'), TYPE_AGG])
    save_current(['stats_sources', aggregate_field(coll, 'source.uid'), TYPE_AGG])
    save_current(['stats_schemas', aggregate_field(coll, 'source.schema'), TYPE_AGG])
    save_current(['stats_type', aggregate_field(coll, 'source.catalog_type'), TYPE_AGG])
    save_current(['stats_license', aggregate_field(coll, 'dataset.license_id'), TYPE_AGG])
    save_current(['stats_owner', aggregate_field(coll, 'source.owner_type'), TYPE_AGG])
    save_current(['stats_formats', aggregate_array(coll, 'dataset.formats'), TYPE_AGG])
#    save_current(['stats_tags', aggregate_array(coll, 'dataset.tags'), TYPE_AGG])
#    save_current(['stats_datatypes', aggregate_array(coll, 'dataset.datatypes'), TYPE_AGG])

    # Post processed custom code
    custom_update_continents()
    save_current(['stats_datatypes', aggregate_array(coll, 'dataset.datatypes'), TYPE_AGG])
    save_archive()
          

    
     

if __name__ == "__main__":
    run()


