import numpy as np
import pandas as pd
import xarray as xr

# Crear una matriz de valores aleatorios de 4x3
data = np.random.rand(4, 3)

locs = ['IA', 'IL', 'IN']

# Crear un Pandas DateTimeIndex de 4 dias desde 2000-01-01
times = pd.date_range("2000-01-01", periods=4)

# Crear el DataArray
foo = xr.DataArray(data, coords=[times, locs], dims=["time", "space"])

# Crear un DataArray solo con el campo obligatorio
foo2 = xr.DataArray(data)

# Tambien se puede juntar las coords con las dims en el mismo argumento
foo3 = xr.DataArray(data, coords=[('time', times), ('space', locs)])

# O crear las coords como un diccionario
foo4 = xr.DataArray(
    data,
    coords={
        'time': times,
        'space': locs,
        'const': 42,
        'ranking': ('space', [1, 2, 3])
        },
        dims=['time', 'space'])

# Desde pandas
df = pd.DataFrame({"x": [0, 1], "y": [2, 3]}, index=["a", "b"])
df.index.name = "abc"
df.columns.name = "xyz"

foo5 = xr.DataArray(df)

# Propiedades
# print(foo.dims)
# print(foo.coords)
# print(foo.values)
# print(foo.attrs)
# print(foo.name)

foo.name = "foo"
foo.attrs["units"] = "meters"
# print(foo)

# Rename te devuelve una copia del dataSet con otro nombre
foo6 = foo.rename("bar")
# print(foo)

# Coordinates
# Se trata como un diccionario
# print(foo.coords['time'])
# print(foo['time'])

foo["ranking"] = ("space", [1, 2, 3])
del foo['ranking']
# print(foo.coords)

# Crear un Dataset
temp = 15 + 8 * np.random.randn(2, 2, 3)
precip = 10 * np.random.rand(2, 2, 3)
lon = [[-99.83, -99.32], [-99.79, -99.23]]
lat = [[42.25, 42.21], [42.63, 42.59]]
ds = xr.Dataset(
    {
        "temperature": (['x', 'y', 'time'], temp),
        "precipitation": (['x', 'y', 'time'], precip),
    },
    coords={
        "lon": (['x', 'y'], lon),
        "lat": (['x', 'y'], lat),
        "time": pd.date_range("2000-01-01", periods=3),
        'reference_time': pd.Timestamp("2000-01-01"),
    })
print(ds)
