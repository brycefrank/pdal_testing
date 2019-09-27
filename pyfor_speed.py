import laspy
import os
import numpy as np
import pyfor
import time

#laz_dir = 'data/laz'

#start = time.time()
#os.system("laszip -i data/laz/*.laz")
#end = time.time()
#print(end - start)

#las_files = [os.path.join('data/las', path) for path in os.listdir('data/las')]
#print(las_files)

col = pyfor.collection.from_dir('data/las', n_jobs = 6)

#start = time.time()
#col.create_index()
#end = time.time()
#print(end - start)

def my_func(merged, tile):
    try:
        merged.normalize(1)
        grid = merged.grid(17)
        metrics = pyfor.metrics.standard_metrics(grid)
        print(metrics)

    except Exception:
        pass
        

start = time.time()
col.retile_raster(17, 300, 17)
col.par_apply(my_func)
end = time.time()

print(end - start)