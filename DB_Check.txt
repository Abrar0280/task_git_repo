import xml.etree.ElementTree as et
from pprint import pprint
from elasticsearch import Elasticsearch
import glob
import pyodbc
import os
from os.path import basename
es = Elasticsearch()
INDEX_NAME='claims_01'
count=0
db = pyodbc.connect('DRIVER={SQL Server};SERVER=WGSWST003;DATABASE=ElasticSearch;UID=pycacviewer;PWD=Welcome2wave')
cursor = db.cursor()
cursor.execute("select distinct RenderingProviderLastName,RenderingProviderFirstName, RenderingProviderNPI from input_table WHERE RenderingProviderLastName = 'black'")
var=cursor.fetchall()
# print(var)

############ XML FILE PARSING CODE BELOW ######################
id_ref = 100
file="C:\\Users\\Mohamedabrar.A.WGS\\Desktop\\HC\\10_xml/**/*.xml"
for f1 in glob.iglob(file,recursive=True):
    # print(f1)
    # name=basename(r"C:\Users\Mohamedabrar.A.WGS\Desktop\HC\10_xml/")
    # print(name)
    xml=open(f1)
    data= xml.read()
    # print(xml)
    # exit()
    # print(data)
    front = {'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 5, 'g': 6, 'h': 7, 'i': 8, 'j': 9}
    reverse = {"0": "a", "1": "b", "2": "c", "3": "d", "4": "e", "5": "f", "6": "g", "7": "h", "8": "i", "9": "j"}
    id_ref = 100
    document = et.fromstring(data.encode('utf-8'))
    all_pages = document.findall("Page")
    # pprint(all_pages)
    list = []
    for pg in all_pages:
        page_number = pg.find("PageNumber")
        # pprint(page_number.text)
        zone = pg.find("Zone")
        # pprint(zone)
        zone_number = zone.find("ZoneNumber")
        # pprint(zone_number)
        lines = zone.findall("Line")
        # pprint(lines)
            #     list = []
        for number in lines:
            num_text = number.find("LineNumber")
            # pprint(num_text.text)
            ocr_text = number.find("OCRCharacters")
            # pprint(ocr_text.text)

                ################### ENCODING HERE ##########################

            page_num = [c for c in page_number.text]
            # print(page_num)
            # print(page_num[1])

            if len(page_num) == 1:
                encode = "XXX" + (reverse[page_num[0]])
                # print(encode)
            elif len(page_num) == 2:
                encode = "XX" + (reverse[page_num[0]]) + (reverse[page_num[1]])
                # print(encode)

                # ZONE ENCODE
            zone_num = [c for c in zone_number.text]
                # print(zone_num)

                # print(len(temp))
            if len(zone_num) == 1:
                zone_encode = (reverse[zone_num[0]])
                    # print(zone_encode)

                # LINE ENCODE
            line_num = [c for c in num_text.text]
                # print(line_num)
            if len(line_num) == 1:
                line_encode = "XX" + (reverse[line_num[0]])
                    # print(line_encode)
            elif len(line_num) == 2:
                line_encode = "X" + (reverse[line_num[0]]) + (reverse[line_num[1]])
                    # print(line_encode)

            join = ocr_text.text + "   " + "||"
            join1 = join + " " + encode
            join2 = join1 + " " + zone_encode
            join3 = join2 + " " + line_encode
            list.append(join3)
    # pprint(list)
    # CREATING INDEX
    # a={"a":list}
    id_ref+=1
    # print("creating '%s' index..." % (INDEX_NAME))
    # res = es.create(index=INDEX_NAME, doc_type="text",body=a,id=id_ref,ignore=[400,404,409])
    # print(" response: '%s'" % (res))

    for str in list:
        string = str.split("|| ")
        ocr, code = string[0], string[1]
        val = code.replace("X", "")
        # print(val)
        value = val.split(" ")
        Page = value[0]
        Zone = value[1]
        Line = value[2]

            ###################### PAGE DECODE ########################
        if len(Page) == 1:
            page_decode = front[Page[0]]
            # print(page_decode)
        elif len(Page) == 2:
            page_decode = front[Page[0]] + front[Page[1]]
            # print(page_decode)
            # exit()

            ####################### ZONE DECODE ############################
        if len(Zone) == 1:
            # print(Zone[0])
            zone_decode = front[Zone[0]]
            # print(zone_decode)

            ########################## LINE DECODE ###############################
        if len(Line) == 1:
            line_decode = front[Line[0]]
            # print(line_decode)
        elif len(Line) == 2:
            line_decode = front[Line[0]] + front[Line[1]]
            # print(line_decode)
        # print(ocr, page_decode, zone_decode, line_decode)
        # exit()
    for each_content in var:
        search_query = {
            "query":
                {
                    "bool":
                        {
                            "must":
                                {
                                    "match_phrase":
                                        {
                                            "a": each_content[0]
                                        }
                                }

                        }
                    },
            "highlight": {
                "fields": {
                    "a": {"number_of_fragments": 0,
                          "fragment_size": 0,
                          "type": "plain"
                          }
                }
            }
            }

        res = es.get(index=INDEX_NAME, doc_type="text",id=id_ref,ignore=[400,404,409])
        # print(res['_source'])
        es.indices.refresh(index=INDEX_NAME)
        # print(search_query)
        res = es.search(index=INDEX_NAME, body=search_query)
        # print(res)
        print("Got %d Hits:" % res['hits']['total'])
        if res['hits']['hits']:
            for hit in res['hits']['hits']:
                        # print(hit)
                query="INSERT INTO claims_001 (ID,F_Name,L_Name,Page,Zone,Line) VALUES (?,?,?,?,?,?)",(each_content[2], each_content[1], each_content[0], page_decode, zone_decode, line_decode)
                # print(query)
                cursor.execute("INSERT INTO claims_001 (ID,F_Name,L_Name,Page,Zone,Line) VALUES (?,?,?,?,?,?)",
                              (each_content[2], each_content[1], each_content[0], page_decode, zone_decode, line_decode))
                # cursor.execute("select distinct RenderingProviderLastName,RenderingProviderFirstName, RenderingProviderNPI from input_table WHERE RenderingProviderLastName = 'black'")
                db.commit()
                count += 1
        if count==0:
            cursor.execute("INSERT INTO claims_001 (ID,F_Name,L_Name,Page,Zone,Line) VALUES (?,?,?,?,?,?)",
                          (f1, 0, 0, 0, 0, 0))
            db.commit()
            count = 0
# res=es.delete(index=INDEX_NAME,doc_type="text",id = id_ref,ignore=404)
# pprint(res)