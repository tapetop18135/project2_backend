from flask import Flask, jsonify, request, json
from flask_cors import CORS
import json

from classReadNetCDF import BaseDataUse
from classRegrids import Regrids
from DBclass import MongoDB_class

import codecs, json
import numpy as np
import pandas as pd

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/api/*": {"origins": ["*"]}})

db = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "temp1x1")

dbDR = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "temp_method")

dbRaw = MongoDB_class("mongodb://localhost:27017/", "climateDB", "db_GHCN")

dbRaw2D = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "rawdata2Darr")


dbMapAVG = MongoDB_class("mongodb://localhost:27017/", "climateDB", "db_avg_Map")

dbMapTrend = MongoDB_class("mongodb://localhost:27017/", "climateDB", "db_trend_Map")

@app.route('/')
def index():
    return jsonify({
            "api":{
                "get geo json" : "/api/getgeocountry",
                "get full map " : "/api/getmap/full/<year>/<type_data>",
                "get reduce map " : "/api/getmap/reduce/<year1>/<year2>/<type_data>/<type_map>",
                "get data" : "/api/getdata/<year>/<type_data>/<lat>/<lon>",
                "get dimension reduction": "/api/getmap/dimenstionR/<type_method>"
            }
        })

@app.route('/api/getgeocountry')
def getGeojson():
    with open('./data/geojson/countries.geo.json') as f:
        data = json.load(f)
    return jsonify(data)

@app.route('/api/findCoor', methods=['POST'])
def finCoor():
    print("############## find Coor ################")
    rq = request.form
    for k in rq.keys():
        data = k
    
    jsond = json.loads(data)   

    # print(jsond['typeUse'])

    if(len(jsond['data']) == 1):
        N = len(jsond['data'][0])
        tempArr = []
        data = jsond['data'][0] 
        for i in range(0, N):
            coor = data[i]
            tempArr.append([find_lat_lon(coor[0]), find_lat_lon(coor[1])])
        # print(len(tempArr))
    else:
        data = jsond['data']
        N = len(data)
        tempArr = []
        for i in range(0, N):
            for j in range(0, len(data[i][0])):
                coor = data[i][0][j]
                tempArr.append([find_lat_lon(coor[0]), find_lat_lon(coor[1])])
        # print(tempArr)
        # print(len(tempArr))
    
    tempArr = np.array(tempArr)
    t = tempArr.T
    dataU = useDataSet(jsond['typeUse'])
    print(min(t[1]), max(t[1]), min(t[0]), max(t[0]))

    print(dataU.sel(time=jsond['year'],lon=slice(max(t[1]), min(t[1])) ))

    return jsonify(
        {
            "status": "True"
        }
    )


def find_lat_lon(num_lat_lon, gridsize=1):
    num_lat_lon = float(num_lat_lon)
    tempN =  round(num_lat_lon, 2)
    if(gridsize == 1):
        num_lat_lon_s = str(num_lat_lon).split('.')
        last = float(num_lat_lon_s[1][0])
        num_lat_lon_s[0] = float(num_lat_lon_s[0]) 
        return float(num_lat_lon_s[0]+.5)   



@app.route('/api/getmap/full/<year>/<type_data>')
def getmap(year, type_data):
    # type_data = request.args.get('type_data')
    if(year):
        data_use = useDataSet(type_data)
        data = data_use.sel(time=year)[type_data]
        lat_list = json.dumps(data.coords['lat'].values.tolist())
        lon_list = json.dumps(data.coords['lon'].values.tolist())
        data_list = json.dumps(np.nan_to_num(data.values).tolist())

    return jsonify(
        {
            "data": {"data_list":data_list, "lat_list":lat_list, "lon_list":lon_list}
            # "description":{"time": year, "desc": data.var_desc, "long_des": data.long_name, "units": data.units} 
        }
    )#

@app.route('/api/getmap/reduce/<year1>/<year2>/<type_data>')
def getmap_reduce(year1, year2, type_data): 
    if(year1 or year2):
        lat_list = []
        lon_list = []
        data_list = {}
        date_list = []
        i = 0
        for data in db.queyBydateBT(year1, year2, type_data):
            temp = {}
            if (i == 0):
                lat_list = data["lat_list"]
                lon_list = data["lon_list"]
                i = 1        

            date_list.append(str(data["date"])[0:10])
            # temp[str(data["date"])[0:10]] = data["data_list"] 
            df = pd.DataFrame(data["data_list"])
            df.fillna(-99.9999, inplace = True)
            a = df.values.tolist()
            # print(a)
            data_list[str(data["date"])[0:10]] = a# np.nan_to_num(np.array(data["data_list"])).tolist()
            

        
        data =  {
                "date_list":date_list,
                "data_list": data_list, 
                "lat_list":lat_list, 
                "lon_list":lon_list, 
                "typeUse":type_data, 
                "dateStart":year1, 
                "dateStop":year2, 
                "length": len(data_list)
                }

    return jsonify(
        {
            "data": data
        }
    )#

import time

from datetime import date, datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

@app.route('/api/getmap/rawdata/<dataset>/<year_start>/<year_stop>/<index_name>/')
def getmap_raw(dataset, year_start, year_stop, index_name):
    start_time = time.time()

    print(f'{year_start} - {year_stop}')
    # array_DATA = []
    array_date = []
    dataR = {}
    # i = 0
    for data in dbRaw.queryDBby_indic(dataset, year_start, year_stop, index_name):
        # print(data["detail"])
        # break
        # lat_list = data["detail"]["lat_list"]
        # lon_list = data["detail"]["lon_list"]
        index_name = data["detail"]["index_name"]
        short_name = data["detail"]["shor_name"]
        type_measure = data["detail"]["type_measure"]
        unit = data["detail"]["unit"]
        date = data["detail"]["date"]
        method = data["detail"]["method"]
        # shape = data["detail"]["shape"]
        # author = data["detail"]["author"]
        arrayData = data["detail"]["arrayMonth"]
        # print(np.array(data["Ann"]).tolist())
        # datau = np.nan_to_num(np.array(data["Ann"])).tolist()
        # datau = []
        # arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        # # i+=1
        # # print(i)
        # # print(data["Jan"])
        # # print("----------------------------------------------")
        # # break
        # for ad in arrayData:
        #     # print(ad)
        #     df = pd.DataFrame(data[ad])
        #     df.fillna(-99.9999, inplace = True)
        #     a = df.values.tolist()
            
        #     datau.append(a)

        array_date.append(str(date)[0:10])
        dataR[str(date)[0:10]] =  {
                "method":method,
                # "lat_list":lat_list, 
                # "lon_list":lon_list, 
                "index_name":index_name, 
                "short_name":short_name, 
                "unit":unit, 
                "date":date, 
                # "shape":shape, 
                # "author":author, 
                "data":data["data"], 
                "arrayData": arrayData,
                "type_measure": type_measure,
                }

    elapsed_time = time.time() - start_time
    # print(elapsed_time)

    # response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    # response.headers['Pragma'] = 'no-cache'
    dataReal = {
            "data": dataR,
            "size": len(array_date),
            "date_range": array_date
        }

    response = app.response_class(
        response=json.dumps(dataReal, default=json_serial),
        status=200,
        mimetype='application/json'
    )
    
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers['Cache-Control'] = 'public, max-age=0'

    return response

@app.route('/api/getmap/mapAVG/<dataset>/<year_start>/<year_stop>/<index_name>/')
def getmap_mapANG(dataset, year_start, year_stop, index_name):
    year_r = year_start+"_"+year_stop
    print(year_r)
    dataR = []

    arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    for data in dbMapAVG.queryMapDBby_indic(dataset, year_r, index_name):
        index_name = data["detail"]["index_name"]
        short_name = data["detail"]["shor_name"]
        type_measure = data["detail"]["type_measure"]
        unit = data["detail"]["unit"]
        date = data["detail"]["date"]
        method = data["detail"]["method"]
        # shape = data["detail"]["shape"]
        # author = data["detail"]["author"]
        arrayData = arrayData

    dataR.append({
                "method":method,
                # "lat_list":lat_list, 
                # "lon_list":lon_list, 
                "index_name":index_name, 
                "short_name":short_name, 
                "unit":unit, 
                "date":date, 
                # "shape":shape, 
                # "author":author, 
                "data":data["data"], 
                "arrayData": arrayData,
                "type_measure": type_measure,
                })

    dataReal = {
        "data": dataR,
        # "size": len(array_date),
        "date_range": [0]
    }

    response = app.response_class(
        response=json.dumps(dataReal, default=json_serial),
        status=200,
        mimetype='application/json'
    )
    
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers['Cache-Control'] = 'public, max-age=0'

    return response


@app.route('/api/getmap/mapTrend/<dataset>/<year_start>/<year_stop>/<index_name>/')
def getmap_mapTrend(dataset, year_start, year_stop, index_name):
    year_r = year_start+"_"+year_stop
    print(year_r)
    arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    dataR = []
    for data in dbMapTrend.queryMapDBby_indic(dataset, year_r, index_name):
        index_name = data["detail"]["index_name"]
        short_name = data["detail"]["shor_name"]
        type_measure = data["detail"]["type_measure"]
        unit = data["detail"]["unit"]
        date = data["detail"]["date"]
        method = data["detail"]["method"]
        shape = data["detail"]["shape"]
        author = data["detail"]["author"]
        arrayData = arrayData

    dataR.append({
                "method":method,
                # "lat_list":lat_list, 
                # "lon_list":lon_list, 
                "index_name":index_name, 
                "short_name":short_name, 
                "unit":unit, 
                "date":date, 
                "shape":shape, 
                "author":author, 
                "data":data["data"], 
                "arrayData": arrayData,
                "type_measure": type_measure,
                })

    dataReal = {
        "data": dataR,
        # "size": len(array_date),
        "date_range": [0]
    }

    response = app.response_class(
        response=json.dumps(dataReal, default=json_serial),
        status=200,
        mimetype='application/json'
    )
    
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers['Cache-Control'] = 'public, max-age=0'

    return response
# @app.route('/api/getmap/rawdata2D/<dataset>/<year_start>/<year_stop>/<index_name>/')
# def getmap_raw2D(dataset, year_start, year_stop, index_name):
#     # http://127.0.0.1:3000/api/getmap/rawdata2D/GHCN/1951-01-01/1971-01-01/TXx/
#     start_time = time.time()

#     print(f'{year_start} - {year_stop}')
#     print(f'------------------------------------------')
#     # array_DATA = []
#     array_date = []
#     dataR = {}
#     i = 0
#     print(dbRaw2D.queryDBby_indic(dataset, year_start, year_stop, index_name))
#     for data in dbRaw2D.queryDBby_indic(dataset, year_start, year_stop, index_name):
#         # print(i)
#         # i+=1
#         # print(f'----------------- END ---------------------')
#         # continue
#         # print(data["detail"])
#         # break
#         # lat_list = data["detail"]["lat_list"]
#         # lon_list = data["detail"]["lon_list"]
#         index_name = data["detail"]["index_name"]
#         short_name = data["detail"]["shor_name"]
#         type_measure = data["detail"]["type_measure"]
#         unit = data["detail"]["unit"]
#         date = data["detail"]["date"]
#         method = data["detail"]["method"]
#         shape = data["detail"]["shape"]
#         author = data["detail"]["author"]
#         # arrayData = data["detail"]["arrayMonth"]
#         # print(np.array(data["Ann"]).tolist())
#         # datau = np.nan_to_num(np.array(data["Ann"])).tolist()
#         datau = []
#         arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
#         # # i+=1
#         # # print(i)
#         # # print(data["Jan"])
#         # # print("----------------------------------------------")
#         # # break
#         for ad in arrayData:
#             # print(ad)
#             df = pd.DataFrame(data[ad])
#             df.fillna(-99.9999, inplace = True)
#             a = df.values.tolist()
            
#             datau.append(a)

#         array_date.append(str(date)[0:10])
#         dataR[str(date)[0:10]] =  {
#                 "method":method,
#                 # "lat_list":lat_list, 
#                 # "lon_list":lon_list, 
#                 "index_name":index_name, 
#                 "short_name":short_name, 
#                 "unit":unit, 
#                 "date":date, 
#                 "shape":shape, 
#                 "author":author, 
#                 "data":datau, 
#                 # "arrayData": arrayData,
#                 "type_measure": type_measure,
#                 }

#     elapsed_time = time.time() - start_time
#     print(elapsed_time)

#     # response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
#     # response.headers['Pragma'] = 'no-cache'


#     dataReal = {
#             "AVGWorld": "AVGGGGGG",
#             "TrendWorld": "TRENDDDDD",
#             "data": dataR,
#             "size": len(array_date),
#             "date_range": array_date,
#             # "timeCounter": elapsed_time
#         }

#     response = app.response_class(
#         response=json.dumps(dataReal, default=json_serial),
#         status=200,
#         mimetype='application/json'
#     )
    
#     response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
#     response.headers["Pragma"] = "no-cache"
#     response.headers["Expires"] = "0"
#     response.headers['Cache-Control'] = 'public, max-age=0'

#     return response


# @app.route('/api/getmap/rawdata2D/<dataset>/<year_start>/<year_stop>/<index_name>/')
# def getmap_raw2D(dataset, year_start, year_stop, index_name):
#     # http://127.0.0.1:3000/api/getmap/rawdata2D/GHCN/1951-01-01/1971-01-01/TXx/
#     start_time = time.time()

#     print(f'{year_start} - {year_stop}')
#     print(f'------------------------------------------')
#     # array_DATA = []
#     array_date = []
#     dataR = {}
#     i = 0
#     arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
#     print(dbRaw2D.queryDBby_indic(dataset, year_start, year_stop, index_name))
#     tempSeason = {}
#     tempSeasonANG_TREND = {}
#     for te in arrayData:
#         tempSeason[te] = []
#         tempSeasonANG_TREND[te] = []
#     # for te in arrayData:
#     #     tempSeason[te] = []
#     # for k in tempSeason:
#     #     tempSeason[k].append([[0]*1]*2)
#     # print(tempSeason)
#     for data in dbRaw2D.queryDBby_indic(dataset, year_start, year_stop, index_name):
#         index_name = data["detail"]["index_name"]
#         short_name = data["detail"]["shor_name"]
#         type_measure = data["detail"]["type_measure"]
#         unit = data["detail"]["unit"]
#         date = data["detail"]["date"]
#         lat_list = data["detail"]["lat_list"]
#         lon_list = data["detail"]["lon_list"]
#         method = data["detail"]["method"]
#         shape = data["detail"]["shape"]
#         author = data["detail"]["author"]
#         array_date.append(str(date)[0:10])
#         datau = []
        
#         # arr = [0,1,2,3,4,5,6,7,8,9,10,11,12]
#         for k in tempSeason:
#             # tempSeason[k]
#             # temp = np.array(temp)
#             tempDat = []
#             tempDatAVG_TREND = [] 
#             for lat_i in range(0, len(lat_list)):
#                 for lon_i in range(0, len(lon_list)):
#                     tempData = {}
#                     # tempData["coor_val"] = {"lat": lat, "lon": lon, "value": data.sel(lat=lat,lon=lon)[temp].values.tolist()}
#                     # tempMonth[temp].append(tempData["coor_val"])
#                     # # print(len(tempMonth["Ann"]))
#                     # break
#                         # print(data.sel(lat=lat,lon=lon)[temp].values)
#                         # print(np.isnan(data.sel(lat=lat,lon=lon)[temp].values))
#                     if(not(np.isnan(np.array(data[k][lat_i][lon_i])))):
#                         tempData["coor_val"] = {"lat": lat_list[lat_i], "lon": lon_list[lon_i], "value": data[k][lat_i][lon_i]}
#                         tempDat.append(tempData["coor_val"])
#                         tempDatAVG_TREND.append(data[k][lat_i][lon_i])
#                     else:
#                         tempDatAVG_TREND.append(None)
                    
            
#             tempSeasonANG_TREND[k].append(tempDatAVG_TREND)
#             tempSeason[k].append(tempDat)
        
#             # print(tempSeason[k][0])
#             # break
#         # break
#     i = 0
#     for tel in tempSeason["Ann"]:
#         print(i,tel[0])
#         i+=1
#     # print(tempSeason["Ann"][0][0])
#     # print(tempSeason["Ann"][1][0])
#     # print(tempSeason["Ann"][1][1])
#     print("--------- Ann ----------")
#     print(tempSeason["Jan"][0][0])
#     print(tempSeason["Feb"][0][0])
#     break
#         # i+=1
#         # print(i)
#         # print(data["Jan"])
#         # print("----------------------------------------------")
#         # break
#         for ad in arrayData:
#             # print(ad)
#             df = pd.DataFrame(data[ad])
#             df.fillna(-99.9999, inplace = True)
#             a = df.values.tolist()
            
#             datau.append(a)

        
#         dataR[str(date)[0:10]] =  {
#                 "method":method,
#                 # "lat_list":lat_list, 
#                 # "lon_list":lon_list, 
#                 "index_name":index_name, 
#                 "short_name":short_name, 
#                 "unit":unit, 
#                 "date":date, 
#                 "shape":shape, 
#                 "author":author, 
#                 "data":datau, 
#                 # "arrayData": arrayData,
#                 "type_measure": type_measure,
#                 }

#     elapsed_time = time.time() - start_time
#     print(elapsed_time)

#     dataReal = {
#             "AVGWorld": "AVGGGGGG",
#             "TrendWorld": "TRENDDDDD",
#             "data": dataR,
#             "size": len(array_date),
#             "date_range": array_date,
#             # "timeCounter": elapsed_time
#         }

#     response = app.response_class(
#         response=json.dumps(dataReal, default=json_serial),
#         status=200,
#         mimetype='application/json'
#     )
    
#     response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
#     response.headers["Pragma"] = "no-cache"
#     response.headers["Expires"] = "0"
#     response.headers['Cache-Control'] = 'public, max-age=0'

#     return response


@app.route('/api/getmap/dimenstionR/<type_method>/<type_map>')
def getmap_DR(type_method, type_map): 
    print(type_method)
    print(type_map)
    if(type_method):
        # method_name = ""
        # type_name = ""
        # lat_list = []
        # lon_list = []
        # date_list = []
        # data_pc = []
        # data_eofs = []
        data = {}

        for data in dbDR.queryDMbyMethod(type_map, type_method):
            lat_list = data["lat_list"]
            lon_list = data["lon_list"]
            method_name = data["method_name"]
            type_map = data["type_map"]    
            data_pc = data["n_component_and_n_sample"]
            data_eofs = data["n_component_and_n_feature"]
            date_range = data["date_range"]

            data =  {
                "method_name":method_name,
                "type_map": type_map, 
                "lat_list":lat_list, 
                "lon_list":lon_list, 
                "data_pc":data_pc, 
                "data_eofs":data_eofs, 
                "date_range":date_range, 
                }
            try:
                data["variance_ratio"] = data["variance_ratio"]
            except:
                pass
    
    return jsonify(
        {
            "data": data,
        }
    )

@app.route('/api/getdata/<year>/<type_data>/<lat>/<lon>')
def getdata(year, type_data, lat, lon):
    temp_dat = "No data or Error"
    
    data_use = useDataSet(type_data)
    
    if(year):
        temp_dat = data_use.sel(time=year)[type_data]
        if(lat and lon):
            temp_dat = temp_dat.sel(lat=float(lat),lon=float(lon)).values
            temp_dat = str(temp_dat)
        else:
            temp_dat = json.dumps(temp_dat.values.tolist())
    
    return jsonify(
        {
            "value":temp_dat
        }
    )#
    


if __name__ == "__main__" :
    app.run(host="localhost", port=3200, debug=True)
    # app.run(host="192.168.1.100", port=3000, debug=True)