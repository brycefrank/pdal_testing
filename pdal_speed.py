import laspy
import os
import numpy as np
from shapely.geometry import Polygon
import json
import pdal
from joblib import Parallel, delayed
import time

las_files = [os.path.join('data/las', path) for path in os.listdir('data/las') if path.endswith('.las')]
print(las_files)




def get_project_bbox(las_files):
    xs = []
    ys = []

    for las_file in las_files:
        las = laspy.file.File(las_file)
        x, y, z = las.header.min

        xs.append(x)
        ys.append(y)

        x, y, z = las.header.min
        xs.append(x)
        ys.append(y)

    bbox = np.min(xs), np.max(xs), np.min(ys), np.max(ys)
    return(bbox)

def square_buffer(polygon, buffer):
    """
    A simple square buffer that expands a square by a given buffer distance.
    :param polygon: A shapely polygon.
    :param buffer: The buffer distance.
    :return: A buffered shapely polgon
    """

    minx, miny, maxx, maxy = polygon.bounds
    n_minx, n_miny, n_maxx, n_maxy = minx - buffer, miny - buffer, maxx + buffer, maxy + buffer

    buffered_poly = Polygon([[n_minx, n_miny],
                             [n_minx, n_maxy],
                             [n_maxx, n_maxy],
                             [n_maxx, n_miny]])
    return buffered_poly


# Ripped from pyfor
def retile_raster(bbox, target_cell_size, target_tile_size, buffer = 0):
    bottom, left = bbox[2], bbox[0]
    top, right = bbox[3], bbox[1]

    new_tile_size = np.ceil(target_tile_size / target_cell_size) * target_cell_size

    project_width = right - left
    project_height = top - bottom

    num_x = int(np.ceil(project_width / new_tile_size))
    num_y = int(np.ceil(project_height / new_tile_size))


    new_tiles = []
    for i in range(num_x):
        for j in range(num_y):
            # Create geometry
            tile_left, tile_bottom = left + i * new_tile_size, bottom + j * new_tile_size

            new_tile = Polygon([
                [tile_left, tile_bottom], #bl
                [tile_left, tile_bottom + new_tile_size], #tl
                [tile_left + new_tile_size, tile_bottom + new_tile_size], #tr
                [tile_left + new_tile_size, tile_bottom]]) #br

            if buffer > 0:
                new_tile = square_buffer(new_tile, buffer)

            new_tiles.append(new_tile)

    return new_tiles



                                                          

def generate_pipelines(tiles, ept_path):
    pipelines = []
    for tile in tiles:
        coords = tile.exterior.coords
        bbox = min_x, max_x, min_y, max_y = coords[0][0], coords[2][0], coords[0][1], coords[1][1]
        bound_str = "([{}, {}], [{}, {}])".format(min_x, max_x, min_y, max_y)

        flat_coords = [int(np.floor(coord)) for coord in bbox]
        out_str = '{}_{}_{}_{}'.format(*flat_coords)

        pipeline = [
            {
            "type" : "readers.ept",
            "filename": ept_path,
            "bounds": bound_str
            },
            {
                "type": "filters.hag"
            },
            {
                "type": "filters.ferry",
                "dimensions":"HeightAboveGround=Z"
            },
            {
                "type": "filters.python",
                "script": "process.py",
                "module": "process",
                "function": "my_py_filter",
                "pdalargs": {
                        "bbox": out_str,
                        "crs": "+proj=utm +zone=10 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
                    }
            }
        ]

        pipeline_json = json.dumps(pipeline)
        pipelines.append(pipeline_json)

    return(pipelines)


def execute_pipeline(pipeline):
    print("Executing pipeline at {}".format(pipeline))
    pipeline = pdal.Pipeline(pipeline)
    pipeline.validate()
    try:
        pipeline.execute()
    except RuntimeError:
        pass


tiles = retile_raster(get_project_bbox(las_files), 17, 300, buffer=17)
pipelines = generate_pipelines(tiles, "data/entwine/ept.json")
Parallel(n_jobs=6)(delayed(execute_pipeline)(pipeline) for pipeline in pipelines)


