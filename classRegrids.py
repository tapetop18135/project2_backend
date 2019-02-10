import numpy as np
import xarray as xr
import time

class Regrids():

    def __init__(self):
        np.seterr(divide='ignore', invalid='ignore')
    
    def getRawdata(self, locafile):
        return xr.open_dataset(locafile)

    def regrids(self, dts,n=2):
        lat = len(dts)
        lon = len(dts[0])
        ary2d = np.array(dts)
        temp = ary2d.reshape([lat//n,n,lon//n,n])
        result = np.mean(temp, axis=(1,3))
        print(f"{ary2d.shape} => {result.shape}")
        return np.nan_to_num(result).tolist()

    def reGrid_1x1np(self, dts):
        print("regrid 1x1 numppy")
        lat = len(dts) # 360
        lon = len(dts[0]) # 720
        ary2d = np.array(dts)
        temp = ary2d.reshape([lat//2,2,lon//2,2])
        m3 = np.nanmean(temp, axis=(1,3))
        print(m3.shape)
        return np.nan_to_num(m3).tolist()

    def reGride_1x1(self, dts):
        print("regrid 1x1")
        print(f"lat : {int(len(dts)/2)}")
        print(f"lon : {int(len(dts[0])/2)}")
        dts = np.nan_to_num(dts)
        lenEnd_lat = int(len(dts))
        lenEnd_lon = int(len(dts[0]))
        array_lat = []
        for i in range(0,lenEnd_lat,2):
            ary_lon = []
            for j in range(0,lenEnd_lon,2):
                ary_lon.append((dts[i][j]+dts[i][j+1]+dts[i+1][j]+dts[i+1][j+1])/4)

                # count = np.count_nonzero([dts[i][j],dts[i][j+1],dts[i+1][j],dts[i+1][j+1]])
                # ary_lon.append((dts[i][j]+dts[i][j+1]+dts[i+1][j]+dts[i+1][j+1])/count)
            
            array_lat.append(ary_lon)

        return np.nan_to_num(np.array(array_lat)).tolist()

    def getLatLon_1x1(self, dts):
        lat_new = []
        for i in range(0,dts['lat'].size,2):
            lat_new.append((dts['lat'].values[i]+dts['lat'].values[i+1])/2)
        lon_new = []
        for i in range(0,dts['lon'].size,2):
            lon_new.append((dts['lon'].values[i]+dts['lon'].values[i+1])/2)
        return lat_new, lon_new



# locafile = 'D:/Project/webService/netCDF/rawdata/tmax.1979.nc'
# reg = Regrids() # Creat obj
# dts = reg.getRawdata(locafile)

# start = time. time() # detect time
# tmax = reg.reGrid_1x1np(dts['tmax'][0]) # INPUT 2D lat and lon
# lat, lon = reg.getLatLon_1x1(dts)
# print(f"time = {time.time() - start}") # detect time

# ##################### result #####################
# print(np.array(tmax).shape)
# print(len(lat))
# print(len(lon))
##################################################


#################### TEST REGRID #######################
# dts = np.array([
#     [np.nan,1,1,1, 1,1,1,1],
#     [2,2,2,2, 2,2,2,2],
#     [3,3,3,3, 3,3,3,3],
#     [4,4,4,4, 4,4,4,np.nan]
# ])
# test = reg.reGride_1x1(dts)
# print(test)
# test1 = reg.reGrid_1x1np(dts)
# print(test)



