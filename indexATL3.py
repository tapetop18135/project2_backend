import xarray as xr
from netCDF4 import Dataset
from scipy import signal
import numpy as np
import matplotlib.pyplot as plt

infileinfile  =  'D:\Project\PanoplyWin-4.9.3\sst.mnmean.v3.nc'

dataset = xr.open_dataset(infileinfile)
print(dataset['sst'][252:1872]) 

ncin = Dataset(infileinfile, 'r')
sst  = ncin.variables['sst'][252:1872] # [672:1920]
lat  = ncin.variables['lat'][:]
lon  = ncin.variables['lon'][:]
print("----------------------------------------------------------")

nt,nlat,nlon = sst.shape    
print(sst.shape )   
sst_old = sst

sst  =  sst.reshape((nt,nlat*nlon), order='F')
sst_detrend = signal.detrend(sst, axis=0, type='linear', bp=0)
sst_detrend = sst_detrend.reshape((12,int(nt/12), nlat,nlon), order='F').transpose((1,0,2,3)) 

sst_season = np.mean(sst_detrend, axis=0) #
sst_diff = sst_detrend - sst_season
sst_diff = sst_diff.transpose((1,0,2,3)).reshape((nt, nlat,nlon), order='F')

# print(sst_diff[0].mean())
print(sst_diff[0][0])

# atl3 = []
# for i in sst_old:
#     atl3.append(sst_diff)
# print(atl3)
# print(len(atl3))

# # atl3 = np.mean(sst_old,axis=1)
# # print(atl3)

# x = np.linspace(1875,2010,len(atl3))
# plt.plot(x,signal.detrend(atl3))
# plt.show()
# a = [
#     [
#     [4,4,4,4,4],
#     [2,2,2,2,2],
#     [3,3,3,3,3]
#     ],
#     [
#     [4,4,4,4,4],
#     [2,2,2,2,2],
#     [3,3,3,3,3]
#     ],
# ]
# a = np.array(a)
# print(np.mean(a,axis=-2))