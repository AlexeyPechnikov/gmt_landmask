#rm -fr gmt_landmask && python3.11 gmt_dem_tiles.py 
import os
import joblib
from tqdm.auto import tqdm
import subprocess

# Assuming path_id and tile_id patterns are provided, update them as necessary
backend = 'loky'
#backend = 'sequential'
resolution = '3s'
basedir = resolution
path_id = '{SN2}'
tile_id = '{SN2}{WE3}'

def make_tile(lon, lat, debug=False):
    SN2 = f'{"S" if lat < 0 else "N"}{abs(lat):02}'
    WE3 = f'{"W" if lon < 0 else "E"}{abs(lon):03}'
    # Update these lines if the path_id and tile_id are supposed to be global or instance variables
    local_path_id = path_id.format(SN2=SN2, WE3=WE3)
    local_tile_id = tile_id.format(SN2=SN2, WE3=WE3)
    tile_dir = os.path.join(basedir, local_path_id)
    tile_filename = os.path.join(tile_dir, local_tile_id)
    if debug:
        print('DEBUG tile_id', local_tile_id)
        print('DEBUG tile_filename', tile_filename)
    
    # create the directory if needed
    if debug:
        print('DEBUG create tile directory:', tile_dir)
    os.makedirs(tile_dir, exist_ok=True)

    # Execute console command
    command = f'gmt grdlandmask -R{lon}/{lon+1}/{lat}/{lat+1} -I{resolution} -Df -NNaN/1 -rp -G{tile_filename}.nc'
    if command:
        print ('DEBUG command:', command)
    if os.path.exists(f'{tile_filename}.nc'):
        os.remove(f'{tile_filename}.nc')
    subprocess.run(command, shell=True, check=True)
    
    if not os.path.exists(f'{tile_filename}.nc'):
        raise Exception(f'ERROR: tile {tile_filename}.nc is not created')
    
    # Gzip the file using console command
    if os.path.exists(f'{tile_filename}.nc.gz'):
        os.remove(f'{tile_filename}.nc.gz')
    subprocess.run(f'gzip {tile_filename}.nc', shell=True, check=True)

    if not os.path.exists(f'{tile_filename}.nc.gz'):
        raise Exception(f'ERROR: tile {tile_filename}.gz is not created')
#    os.rename(f'{tile_filename}.nc.gz', f'{tile_filename}.gz')

    # Cleanup: original nc file is already removed by gzip
    return True

# world grid
lon_range = range(-180, 180)
lat_range = range(-90, 90)
total_tiles = len(lon_range) * len(lat_range)
assert total_tiles==64800, 'ERORR: unexpected tiles amount'

# create the directory if needed
os.makedirs(basedir, exist_ok=True)

with tqdm(total=total_tiles, desc='GMT DEM Tiles Generation') as pbar:
    def update(*args):
        pbar.update()

    joblib.Parallel(n_jobs=4, backend='loky')(joblib.delayed(make_tile)(x, y, True)\
                        for x in lon_range for y in lat_range)





# update the script to use tqdm instead of self.tqdm_joblib, complete external command calls
# 
# import os
# import joblib
# from tqdm.auto import tqdm
# 
# path_id = '{SN2}'
# tile_id = '{SN2}{WE3}'
# 
# def make_tile(lon, lat, debug=False):
#     import os
# 
#     SN2 = f'{"S" if lat<0 else "N"}{abs(lat):02}'
#     WE3 = f'{"W" if lon<0 else "E"}{abs(lon):03}'
#     path_id = self.path_id.format(SN2=SN2, WE3=WE3)
#     tile_id = self.tile_id.format(SN2=SN2, WE3=WE3)
#     tile_filename = os.path.join(path_id, tile_id)
#     if debug:
#         print ('DEBUG tile_id', tile_id)
#         print ('DEBUG tile_filename', tile_filename)
#     # execute console command
#     gmt grdlandmask -R0/1/0/1 -I1s -Df -NNaN/1 -rp -G{tile_filename}.nc
#     
#     if not os.path.exists(f'{tile_filename}.nc'):
#         return False
#     
#     # gzip f'{tile_filename}.nc' using console command to f'{tile_filename}.gz'
#     
#     # cleanup
#     os.remove(f'{tile_filename}.nc')
#     
#     return True
# 
# with self.tqdm_joblib(tqdm(desc=f'GMT DEM Tiles Generation', total=(180-(180)+1)*(90-(-90)+1))) as progress_bar:
#     tile_xarrays = joblib.Parallel(n_jobs=n_jobs, backend='sequential')(joblib.delayed(make_tile)(x, y, True)\
#                         for x in range(-180, 180 + 1) for y in range(-90, 90 + 1))
#                         