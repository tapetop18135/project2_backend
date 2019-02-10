import pymongo
from classReadNetCDF import BaseDataUse
from datetime import datetime
from classRegrids import Regrids
# myDict = {
#     "method_name": "kernelPCA",
#     "type_map": "tmax", 
#     "date": ["1994-10-12", "2012-10-12"], 
#     "valueX_time": [1, 2, 3, 4, 5, 3, 7, 8, 9, 1324, 7, 98, 13, 4, 89, 120],
#     "valueY_temp": [1, 3, 79, 10, 2, 4, 78, 32, 1, 7, 7, 9, 63, 9, 7, 78]
# }


import xarray as xr


class MongoDB_class:
    def __init__(self, name, db_name, collection_name):
        self.myclient = pymongo.MongoClient(name)
        self.mydb = self.myclient[db_name]
        self.mycol = self.mydb[collection_name]

    def checkDB_and_Col(self):
        return [self.myclient.list_database_names(), self.mydb.list_collection_names()]

    def query_allDB(self, data={}):
        if(data == {}):
            mydoc = self.mycol.find()
        else:
            mydoc = self.mycol.find(data)
        return mydoc

    def check_DB(self, method):
        mdict = {"method_name" : method}
        temp = self.mycol.find_one(mdict)
        if(temp != None):
            return True
        return False


    def insertDB(self, mdict):
        x = self.mycol.insert_one(mdict)
        print(x.inserted_id)
    
    def queyBydate(self):
        x = self.mycol.find().sort([("date",-1)])
        return x

    def queyBydateBT(self,dateS,dateE, typeUse):
        # x = self.mycol.find()
        dateS = datetime.strptime(dateS, '%Y-%m-%d')
        dateE = datetime.strptime(dateE, '%Y-%m-%d')
        x = self.mycol.find({"$and": [{"date":{"$gte":dateS, "$lte": dateE},"typeUse":typeUse}] })
        return x    

    def queryDMbyMethod(self, typeUse, method):
        x = self.mycol.find({"type_map":typeUse,  "method_name":method})
        return x

    def deleteDB(self, method):
        mdict = {"method_name" : method}
        self.mycol.delete_many(mdict)
        return True

    def deleteCol(self):
        self.mycol.drop()
        return True

    def updateDB(self, olddict, newvalue):
        # self.mycol.update_one(myquery, newvalues)
        pass

    def queryDBby_indic(self, dataset, dateS, dateE, index_name):
        dateS = datetime.strptime(dateS, '%Y-%m-%d')
        dateE = datetime.strptime(dateE, '%Y-%m-%d')
        # print("dataset", dataset)
        # print("dateS", dateS)
        # print("dateE", dateE)
        # print("index_name", index_name)
        x = self.mycol.find({
            "$and": [
                {"dataset_name_short": dataset}, 
                {"detail.index_name": index_name},
                {
                    "detail.date": {"$gte":dateS, "$lte": dateE}
                    # "detail.date": {$gte : new Date('1951-01-01') , $lte: new Date('1952-01-01')}
                }, 
                ] 
            }
        )

        return x

    def queryMapDBby_indic(self, dataset, year, index_name):
        x = self.mycol.find({
            "$and": [
                {"dataset_name_short": dataset}, 
                {"detail.index_name": index_name},
                {
                    "detail.date": "1971-01-01_2010-01-01"
                    # "detail.date": {$gte : new Date('1951-01-01') , $lte: new Date('1952-01-01')}
                }, 
                ] 
            }
        )
        return x

# test = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "temp1x1")
# dbDR = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "temp_method")
# # # # arr_db, arr_col = test.checkDB_and_Col()
# # # # print(arr_db)
# # # # print(arr_col)
# # # # print(test.queyBydate())

# for d in dbDR.queryDMbyMethod("sst", "pca"):
#     print(d)

# obj = BaseDataUse()
# type_data = "sst"
# # data_tempMax = obj.getData_from_Xarray('rawdata/tmax.*.nc','tmax')
# # data_precip = obj.getData_from_Xarray('rawdata/precipRawNew.nc','precip')
# # data_tempMax = obj.getData_from_Xarray('rawdata/tmax.*.nc','tmax')
# # data_tempMin = obj.getData_from_Xarray('rawdata/tmin.*.nc','tmin')
# # data_SSTa = obj.getData_from_Xarray('SST/ersst.*.nc','ssta')
# data_SST = obj.getData_from_Xarray('SST/ersst.*.nc','sst')
# print(data_SST)
# date_list = data_SST.coords["time"].values

# ########## Find Between ##########

# textS = "2010-01-01"
# textSt = "2010-01-05"
# # #start = datetime.strptime(str(date_list[100])[0:10], '%Y-%m-%d')
# # #stop = datetime.strptime(str(date_list[101])[0:10], '%Y-%m-%d')
# start = datetime.strptime(textS, '%Y-%m-%d')
# stop = datetime.strptime(textSt, '%Y-%m-%d')
# print("DB")
# # # print(test.queyBydateBT(start, stop))
# for t in test.queyBydateBT(start, stop, "tmax"):
#     print(t)



# # reg = Regrids()
# i = 0

# for d in date_list:
#     d = str(d)
#     data = data_SST.sel(time=d)[type_data]
#     data_list = reg.regrids(data, 2)
#     lat_list, lon_list = reg.getLatLon_1x1(data)
#     myDict = {
#         "name" : "sst",
#         "typeUse": "sst",
#         "date": datetime.strptime(d[0:10], '%Y-%m-%d'),
#         "lat_list": lat_list,
#         "lon_list": lon_list,
#         "data_list": data_list
#     }
#     # test.insertDB(myDict)
#     i+=1
#     if(i == 2):
#         break

# ##############################################
# i = 0

# for d in date_list:
#     d = str(d)
#     data = data_SST.sel(time=d)[type_data]
#     data_list = data.values.tolist()
#     lat_list = data.lat.values.tolist()
#     lon_list = data.lon.values.tolist()
#     # data_list = reg.regrids(data, 2)
#     # lat_list, lon_list = reg.getLatLon_1x1(data)
#     print(type(lat_list))
#     print(type(lon_list))
#     print(type(data_list))
#     # print(lat_list.shape)
#     # print(lon_list.shape)
#     # print(data_list.shape)
#     myDict = {
#         "name" : "sst",
#         "typeUse": "sst",
#         "date": datetime.strptime(d[0:10], '%Y-%m-%d'),
#         "lat_list": lat_list,
#         "lon_list": lon_list,
#         "data_list": data_list
#     }
#     test.insertDB(myDict)
#     i+=1
#     if(i == 2):
#         break

# test.deleteCol()

# print(data_tempMax.sel(time=date_list[0])[type_data])
# print(data_tempMax.sel(time=date_list[1])[type_data])


# myDict = {
#     "name" : "tmax",
#     "typeUse": "tmax",
#     "date": Date(),
#     "lat_list": [0]*180 ,
#     "lon_list": [1]*360,
#     "data_list": [[3]*180]*360
# }

# test.insertDB(myDict)
# print(test.check_DB("kernelPCA"))
# print(test.deleteDB('kernelPCA'))



############## GHCN
# test = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "rawdata")
# # a = test.queryDBby_indic("GHCN", "1951-01-01", "1955-01-01", "TN10p")
# test = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "rawdata2Darr")
# test.deleteCol()




# localname = "GHCND_TXx_1951-2018_RegularGrid_global_2.5x2.5deg_LSmask.nc"
# # # D:\Project\webService\netCDF\GHCN Indics\GHCND_CDD_1951-2018_RegularGrid_global_2.5x2.5deg_LSmask.nc
# obj = xr.open_mfdataset(f'D:/Project/webService/netCDF/GHCN Indics/{localname}')
# print(obj)
# attributes = obj.attrs
# coordinates = obj.coords
# # data = obj.data

# dateList = coordinates["time"].values

# dataset_name_short = "GHCN"
# index_name = "TXx"
# shor_name = "Max Tmax (Warmest daily maximum temperature)"
# unit = "°C"
# type_measure = "temperature"
# method = "Intensity"
# i = 0
# for d in dateList:
#     # print(datetime.strptime(str(d)[:8],'%Y%m%d'))
#     i+=1
#     data = obj.sel(time=d)
#     dataU = data["Ann"].values
#     # break
#     myDict = {
#         "dataset_name_short" : dataset_name_short,
#             "detail" : {
#                 "dataset_name" : localname,
#                 "author" : attributes["author"],
#                 "index_name": index_name,
#                 "shor_name": shor_name,
#                 "type_measure": type_measure,
#                 "unit": unit,
#                 "date": datetime.strptime(str(d)[:8],'%Y%m%d'),
#                 "method": method,
#                 "shape": data.dims,
#                 "lat_list": data.coords["lat"].values.tolist(),
#                 "lon_list": data.coords["lon"].values.tolist()
#             },
#             "Ann": data["Ann"].values.tolist(),
#             "Jan": data["Jan"].values.tolist(),
#             "Feb": data["Feb"].values.tolist(),
#             "Mar": data["Mar"].values.tolist(),
#             "Apr": data["Apr"].values.tolist(),
#             "May": data["May"].values.tolist(),
#             "Jun": data["Jun"].values.tolist(),
#             "Jul": data["Jul"].values.tolist(),
#             "Aug": data["Aug"].values.tolist(),
#             "Sep": data["Sep"].values.tolist(),
#             "Oct": data["Oct"].values.tolist(),
#             "Nov": data["Nov"].values.tolist(),
#             "Dec": data["Dec"].values.tolist(),
#         } 
#     # print(myDict["GHCN"][datetime.strptime(str(d)[:8],'%Y%m%d')]["data"])

#     # print(myDict["Jan"])
#     test.insertDB(myDict)
    
    #################################### NEEEEEEEEEEEEEEEEEEEEE WWWWWWWWWWWWWWWWWWWWWWWW #####################
import numpy as np
test = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "db_GHCN")
localname = "GHCND_TXx_1951-2018_RegularGrid_global_2.5x2.5deg_LSmask.nc"
# # D:\Project\webService\netCDF\GHCN Indics\GHCND_CDD_1951-2018_RegularGrid_global_2.5x2.5deg_LSmask.nc
obj = xr.open_mfdataset(f'D:/Project/webService/netCDF/GHCN Indics/{localname}')
print(obj)
attributes = obj.attrs
coordinates = obj.coords
# data = obj.data

dateList = coordinates["time"].values

dataset_name_short = "GHCN"
index_name = "TXx"
shor_name = "Max Tmax"
unit = "°C"
type_measure = "temperature"
method = "Intensity"
i = 0
print(f"Dataset is : {localname}")
for d in dateList:
    # print(datetime.strptime(str(d)[:8],'%Y%m%d'))
    tempData = {}
    arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    tempMonth = {}
    
    
    data = obj.sel(time=d)
    # dataU = data["Ann"].sel()
    for temp in arrayData:
        tempMonth[temp] = []
        for lat in data.coords["lat"].values.tolist():
            for lon in data.coords["lon"].values.tolist():
                
                # tempData["coor_val"] = {"lat": lat, "lon": lon, "value": data.sel(lat=lat,lon=lon)[temp].values.tolist()}
                # tempMonth[temp].append(tempData["coor_val"])
                # # print(len(tempMonth["Ann"]))
                # break
                    # print(data.sel(lat=lat,lon=lon)[temp].values)
                    # print(np.isnan(data.sel(lat=lat,lon=lon)[temp].values))
                if(not(np.isnan(data.sel(lat=lat,lon=lon)[temp].values))):
                    tempData["coor_val"] = {"lat": lat, "lon": lon, "value": data.sel(lat=lat,lon=lon)[temp].values.tolist()}
                    tempMonth[temp].append(tempData["coor_val"])
                    # break
            # break
        # print(temp,tempMonth[temp])
        # if(temp == "Jan"):
        #     break
    myDict = {
        "dataset_name_short" : dataset_name_short,
            "detail" : {
                "arrayMonth" : arrayData,
                "dataset_name" : localname,
                "author" : attributes["author"],
                "index_name": index_name,
                "shor_name": shor_name,
                "type_measure": type_measure,
                "unit": unit,
                "date": datetime.strptime(str(d)[:8],'%Y%m%d'),
                "method": method,
                "shape": data.dims,

                # "lat_list": data.coords["lat"].values.tolist(),
                # "lon_list": data.coords["lon"].values.tolist()

            },
        "data" : tempMonth
            # "Ann": data["Ann"].values.tolist(),
            # "Jan": data["Jan"].values.tolist(),
            # "Feb": data["Feb"].values.tolist(),
            # "Mar": data["Mar"].values.tolist(),
            # "Apr": data["Apr"].values.tolist(),
            # "May": data["May"].values.tolist(),
            # "Jun": data["Jun"].values.tolist(),
            # "Jul": data["Jul"].values.tolist(),
            # "Aug": data["Aug"].values.tolist(),
            # "Sep": data["Sep"].values.tolist(),
            # "Oct": data["Oct"].values.tolist(),
            # "Nov": data["Nov"].values.tolist(),
            # "Dec": data["Dec"].values.tolist(),
        } 
    # print(myDict["GHCN"][datetime.strptime(str(d)[:8],'%Y%m%d')]["data"])

    # print("detail",myDict["detail"])
    # print("data",myDict["data"])
    
    # break
    print(f"{i} - {test.insertDB(myDict)}")
    
    i+=1
######################### AVG TREND ########################
# print("############### AVG ######################")
# test = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "AVG_mapData")

# from netCDF4 import Dataset
# import numpy as np
# loname = "GHCND_TN10p_1951-2018_RegularGrid_global_AVG.nc"
# localname = f"D:/Project/webService/netCDF/AVG_TREND_map/ghcndex_indexAVG/{loname}"


# dataset_name_short = "GHCN"
# index_name = "TN10p"
# shor_name = "Cool nights (Share of days when Tmin < 10th percentile)"
# unit = "percen of days"#"°C"
# type_measure = "temperature"
# method = "Frequency"
# author = "Nuttakit_Puntakarn"
# i = 0

# ncin = Dataset(localname, 'r')

# nday, nlat, nlon = ncin.variables["temp"][:].shape
# temp = ncin.variables["temp"]

# arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
# print(ncin.variables)
# lat_list = ncin.variables["latitudes"][:]
# lon_list = ncin.variables["longitudes"][:]
# tempData = {}
# for day in range(0,nday):
#     tempData[arrayData[day]] = []
#     for lat_i in range(0, nlat):
#         for lon_i in range(0, nlon):
#             # print(lon_i)
#             tempD = {}
#             # print(lat_list[lat_i], lon_list[lon_i], temp[day][lat_i][lon_i])
#             if(np.isnan(temp[day][lat_i][lon_i])):
#                 pass
#             else:
#                 tempData[arrayData[day]].append({"lat": float(lat_list[lat_i]), "lon": float(lon_list[lon_i]), "value": float(temp[day][lat_i][lon_i])})
        
#     print(len(tempData[arrayData[day]]))
#     print(arrayData[day],"----------------------------------------------------")
# # print(tempData)

# myDict = {
#         "dataset_name_short" : dataset_name_short,
#             "detail" : {
#                 "arrayMonth" : arrayData,
#                 "dataset_name" : loname,
#                 "index_name": index_name,
#                 "shor_name": shor_name,
#                 "type_measure": type_measure,
#                 "unit": unit,
#                 "author" : author,
#                 "date": "1971-01-01_2010-01-01",
#                 "method": method,
#                 "shape": (nlat, nlon),

#             },
#         "data" : tempData
#         } 

# test.insertDB(myDict)

# print("############### TREND ######################")

# test = MongoDB_class("mongodb://localhost:27017/", "dimenReduct", "Trend_mapData")

# from netCDF4 import Dataset
# import numpy as np
# loname = "GHCND_TN10p_1951-2018_RegularGrid_global_Trend_1971-2010.nc"
# localname = f"D:/Project/webService/netCDF/AVG_TREND_map/ghcndex_indexTREND/{loname}"
# # # dataset_name_short = "GHCN"
# # # index_name = "TX10p"
# # # shor_name = "Min Tmin (Coldest daily minimum temperature)"
# # # unit = "percen of days"# "°C"#
# # type_measure = "temperature"
# # method = "Frequency"# "Intensity"#
# # author = "Nuttakit_Puntakarn"
# i = 0

# ncin = Dataset(localname, 'r')

# nday, nlat, nlon = ncin.variables["temp"][:].shape
# temp = ncin.variables["temp"]

# arrayData = ["Ann","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
# print(ncin.variables)
# lat_list = ncin.variables["latitudes"][:]
# lon_list = ncin.variables["longitudes"][:]
# tempData = {}
# for day in range(0,nday):
#     tempData[arrayData[day]] = []
#     for lat_i in range(0, nlat):
#         for lon_i in range(0, nlon):
#             # print(lon_i)
#             tempD = {}
#             # print(lat_list[lat_i], lon_list[lon_i], temp[day][lat_i][lon_i])
#             if(np.isnan(temp[day][lat_i][lon_i])):
#                 pass
#             else:
#                 tempData[arrayData[day]].append({"lat": float(lat_list[lat_i]), "lon": float(lon_list[lon_i]), "value": float(temp[day][lat_i][lon_i])})
        
#     print(len(tempData[arrayData[day]]))
#     print(arrayData[day],"----------------------------------------------------")
# # print(tempData)

# myDict = {
#         "dataset_name_short" : dataset_name_short,
#             "detail" : {
#                 "arrayMonth" : arrayData,
#                 "dataset_name" : loname,
#                 "index_name": index_name,
#                 "shor_name": shor_name,
#                 "type_measure": type_measure,
#                 "unit": unit,
#                 "author" : author,
#                 "date": "1971-01-01_2010-01-01",
#                 "method": method,
#                 "shape": (nlat, nlon),

#             },
#         "data" : tempData
#         } 

# test.insertDB(myDict)
