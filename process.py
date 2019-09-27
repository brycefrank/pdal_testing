import pandas as pd
import pyfor
import os

def my_py_filter(ins, outs):
    df = pd.DataFrame(ins)
    df = df.rename(columns={"X": "x", "Y": "y", "Z":"z", "ReturnNumber": "return_num"})
    cloud_data = pyfor.cloud.CloudData(df, header=None)
    cloud = pyfor.cloud.Cloud(cloud_data)
    grid = cloud.grid(15)

    #pct = (10, 30, 50, 70, 90)
    #for p in pct:
    #    pyfor.metrics.grid_percentile(grid, p).write("C:\\Users\\frankbr\\Programming\\pdal_testing\\data\\std_metrics\\p_{}_{}.tif".format(p, pdalargs["bbox"]))
    std_metrics = grid.standard_metrics(1)

    for key, value in std_metrics.items():
        value.write("C:\\Users\\frankbr\\Programming\\pdal_testing\\data\\std_metrics\\{}_{}.tif".format(key, pdalargs["bbox"]))
