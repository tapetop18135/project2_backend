from flask import Flask, jsonify, request
from flask_cors import CORS
import json

from classReadNetCDF import BaseDataUse
from classRegrids import Regrids
from DBclass import MongoDB_class

import codecs, json
import numpy as np

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/api/*": {"origins": ["*"]}})
x = 100

obj = BaseDataUse()
data_precip = obj.getData_from_Xarray('rawdata/precipRawNew.nc','precip')
data_tempMax = obj.getData_from_Xarray('rawdata/tmax.*.nc','tmax')
data_tempMin = obj.getData_from_Xarray('rawdata/tmin.*.nc','tmin')
data_SSTa = obj.getData_from_Xarray('SST/ersst.*.nc','ssta')
data_SST = obj.getData_from_Xarray('SST/ersst.*.nc','sst')
# print(data_precip)
reg = Regrids()

db = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "temp1x1")

def useDataSet(type_data):
    if type_data == "precip" :
        data_use = data_precip
    elif type_data == "tmax" :
        data_use = data_tempMax
    elif type_data == "tmin" :
        data_use = data_tempMin
    elif type_data == "ssta" :
        data_use = data_SSTa
    elif type_data == "sst" :
        data_use = data_SST

    return data_use

@app.route('/')
def index():
    return jsonify({
            "api":{
                "get geo json" : "/api/getgeocountry",
                "get full map " : "/api/getmap/full/<year>/<type_data>",
                "get reduce map " : "/api/getmap/reduce/<year>/<type_data>",
                "get data" : "/api/getdata/<year>/<type_data>/<lat>/<lon>"
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
            "data": {"data_list":data_list, "lat_list":lat_list, "lon_list":lon_list},
            # "description":{"time": year, "desc": data.var_desc, "long_des": data.long_name, "units": data.units} 
        }
    )#

@app.route('/api/getmap/reduce/<year>/<type_data>')
def getmap_reduce(year, type_data): 
    if(year):
        data_use = useDataSet(type_data)
        
        data = data_use.sel(time=year)[type_data]
        print(data)
        if(type_data == "tmax" or type_data == "tmin" or type_data == "precip" ):

            data_list = json.dumps(reg.regrids(data, 2))
            # print(data_list)
            # lat_list, lon_list = reg.getLatLon_1x1(data)

            # data_list = json.dumps(reg.regrids(data))
            lat_list, lon_list = reg.getLatLon_1x1(data)

            lat_list = json.dumps(lat_list)
            lon_list = json.dumps(lon_list)
        else: 
            lat_list = json.dumps(data.coords['lat'].values.tolist())
            lon_list = json.dumps(data.coords['lon'].values.tolist())
            data_list = json.dumps(np.nan_to_num(data.values*-1).tolist())

    return jsonify(
        {
            "data": {"data_list":data_list, "lat_list":lat_list, "lon_list":lon_list},
            # "description":{"time": year, "desc": data.var_desc, "long_des": data.long_name, "units": data.units} 
        }
    )#

@app.route('/api/getdata/<year>/<type_data>/<lat>/<lon>')
def getdata(year, type_data, lat, lon):
    # type_data = request.args.get('type_data')
    # year = request.args.get('year')
    # lat = request.args.get('lat')
    # lon = request.args.get('lon')
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
    app.run(host="127.0.0.1", port=3000, debug=True)