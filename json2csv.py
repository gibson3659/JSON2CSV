from itertools import chain
from collections import OrderedDict
from orderedset import OrderedSet
import json
import csv
import StringIO
import sys
import argparse

global allowedFields

def loadJSON_multipleLines(segments):
    #Iteratively reads and appends lines to build a json object.
    #It then batches those objects and returns them according to maxRecords.
    #This will speed up some processing but also ensures a good sample of records are used to build the csv headers
    maxRecords=100
    recordSet=[]
    chunk = ""
    for segment in segments:
        chunk += segment
        try:
            recordSet.append( json.loads(chunk, object_pairs_hook=OrderedDict) )
            if len(recordSet)>=maxRecords:
                yield recordSet
                recordSet=[]
            chunk = ""
        except ValueError:
            pass
    #yield recordSet when EOF is reached
    yield recordSet

def create_fields_file(fields_file_path, input_file_path):
    #clear file first
    fields_file_path.seek(0)
    fields_file_path.truncate()
    with fields_file_path as field_file, input_file_path as input_file:
        f=OrderedDict()
        for parsed_json in loadJSON_multipleLines(input_file):
            for d in parsed_json:
                ps_keys=d.keys()
            f.update(zip(ps_keys,[True]*len(ps_keys)))
        json.dump(f,fields_file_path,indent=0)

def json_to_csv(input_file_path, output_file_path, fields_dict):
    global allowedFields
    #json = input_file.read()

    allowedFields = [k for k,v in fields_dict.items() if v==True]
    headers_written=False

    for parsed_json in loadJSON_multipleLines(input_file_path):
        dicts = json_to_dicts(parsed_json)
        #dicts_to_csv(dicts, output_csv)

        if headers_written==False:
            #keys = set(chain.from_iterable([o.keys() for o in dicts]))
            #keys = set()
            keys=OrderedSet()
            for k in [o.keys() for o in dicts]:
                keys.update(k)
            output_csv=csv.DictWriter(output_file_path,fieldnames=keys)
            output_csv.writeheader()
            headers_written=True
        output_csv.writerows(dicts)

def json_to_dicts(json_str):
    if type(json_str)==str:
        objects = json.loads(json_str,object_pairs_hook=OrderedDict)
    elif type(json_str) in (dict, OrderedDict):
        objects=[json_str]
    else:
        objects=json_str

    #def to_single_dict(lst):
    #    result = {}
    #    for d in lst:
    #        for k in d.keys():
    #            result[k] = d[k]
    #    return result;

    return [OrderedDict(to_keyvalue_pairs(obj)) for obj in objects]

def to_keyvalue_pairs(source, ancestors=[], key_delimeter='_'):
    def is_sequence(arg):
        return (not hasattr(arg, "strip") and hasattr(arg, "__getitem__") or hasattr(arg, "__iter__"))

    def is_dict(arg):
        return hasattr(arg, "keys")

    if is_dict(source):
        result = [to_keyvalue_pairs(source[key], ancestors + [key]) for key in source.keys() if (ancestors==[] and key in allowedFields) or ancestors!=[]]
        return list(chain.from_iterable(result))
    elif is_sequence(source):
        result = [to_keyvalue_pairs(item, ancestors + [str(index)]) for (index, item) in enumerate(source)]
        return list(chain.from_iterable(result))
    else:
        return [(key_delimeter.join(ancestors), source)]

'''
def dicts_to_csv(source, output_file):
    #def build_row(dict_obj, keys):
    #    return [dict_obj.get(k, "") for k in keys]
    keys = sorted(set(chain.from_iterable([o.keys() for o in source])))
    #rows = [build_row(d, keys) for d in source]

    #cw = csv.writer(output_file)
    cw=output_file
    #cw.writerow(keys)
    #for row in rows:
        #cw.writerow([c.encode('utf-8') if isinstance(c, str) or isinstance(c, unicode) else c for c in row])
    cw.writerows(source)
'''
#def write_csv(headers, rows, file):
#    cw = csv.writer(file)
#    cw.writerow(headers)
#    for row in rows:
#        cw.writerow([c.encode('utf-8') if isinstance(c, str) or isinstance(c, unicode) else c for c in row])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fields",type=argparse.FileType('a+'),default='fields.json',help="Field list file for output. If not given, a fields.json file will be created allowing all fields.")
    parser.add_argument("-o", "--output",type=argparse.FileType('w'), help="Destination file. To generate a fields.json only, leave blank.")
    parser.add_argument("JSON_FILE_PATH",type=argparse.FileType('r'), help="JSON log file to parse")
    args=parser.parse_args()

    if args.output==None:
        create_fields_file(args.fields, args.JSON_FILE_PATH)
    else:
        try:
            fields_dict=json.load(args.fields, object_pairs_hook=OrderedDict)
            json_to_csv(args.JSON_FILE_PATH, args.output, fields_dict)
        except ValueError:  #Fields file is empty, invalid, or did not exist
            i=raw_input("Invalid fields file specified. Create default fields.json to continue (Y/N)? ")
            if i.upper()=='Y':
                args.fields.truncate()
                create_fields_file(args.fields, args.JSON_FILE_PATH)
                #files must be reopened since they are closed after the with statement in create_fields_file
                fields_dict=json.load(open(args.fields.name), object_pairs_hook=OrderedDict)
                json_to_csv(open(args.JSON_FILE_PATH.name), args.output, fields_dict)
            else:
                quit()


    #    json_to_csv(args[1], args[2])
    #    print 'Finished'
    #else:
        #print 'Usage:'
        #print 'python json2csv.py "{JSON_FILE_PATH}" "{OUTPUT_FILE_PATH}"'
