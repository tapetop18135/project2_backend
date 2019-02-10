import xarray as xr

class BaseDataUse:
    def getData_from_Xarray(self, loca_name, typeName, dateTime=-1): 
        df = xr.open_mfdataset(f'D:/Project/webService/netCDF/{loca_name}')
        if(typeName == "ssta" or typeName == "sst"):
            df = df.sel(lev=0)
        if(dateTime != -1):
            df = df.sel(time=dateTime)
            df = df[typeName]
        else:
            pass
        return df


# t = BaseDataUse()

# te = t.getData_from_Xarray('precip.*.nc','precip','2000-01-01')
# print(te.var_desc)
# print(te.long_name)
# print(te.units)