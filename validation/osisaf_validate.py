from __future__ import print_function

import os
from osgeo import gdal, osr
import numpy as np
from subprocess import call
import sys
import logging


class OsisafValidate():
    def __init__(self, myo_fn, osi_fn):

        self.g_myo = gdal.Open('NETCDF:"{0}":ice_edge'.format(myo_fn))
        self.g_osi = gdal.Open('NETCDF:"{0}":ice_edge'.format(osi_fn))
        self.g_conf = gdal.Open('NETCDF:"{0}":confidence_level'.format(osi_fn))

        self.myo_proj = "+proj=stere +a=6371000.0 +b=6371000.0 \
                         +lat_0=-90.0 +lat_ts=-90.0 +lon_0=0.0"
        self.osi_proj = "+proj=stere +lat_0=-90 +lat_ts=-70 +lon_0=0 \
                         +k=1 +x_0=0 +y_0=0 +a=6378273 +b=6356889.44891 +units=m +no_defs"
        
        self.sr_myo = osr.SpatialReference()
        self.sr_myo.ImportFromProj4(self.myo_proj)
        self.g_osi.SetGeoTransform([-3950000.0, 10000.0, 0.0, 4350000.0, 0.0, -10000.])
        self.g_conf.SetGeoTransform([-3950000.0, 10000.0, 0.0, 4350000.0, 0.0, -10000.])
        self.sr_osi = osr.SpatialReference()
        self.sr_osi.ImportFromProj4(self.osi_proj)
        self.g_osi.SetProjection(self.sr_osi.ExportToWkt())
        self.g_conf.SetProjection(self.sr_osi.ExportToWkt())
        self.myo_geo = self.g_myo.GetGeoTransform()

        logging.debug('OsisafValidate object created for {0} and {1}.'.format(myo_fn, osi_fn))


    def create_transform_paras(self):
        
        tx = osr.CoordinateTransformation(self.sr_myo, self.sr_osi)

        x_size = self.g_myo.RasterXSize 
        y_size = self.g_myo.RasterYSize

        (self.ulx, self.uly, ulz ) = tx.TransformPoint(self.myo_geo[0], self.myo_geo[3])
        (self.lrx, self.lry, lrz ) = tx.TransformPoint(self.myo_geo[0] + self.myo_geo[1]*x_size, \
                                      self.myo_geo[3] + self.myo_geo[5]*y_size )


    def crop_osi(self, ds, iceedge=True):
        
        bounds = [self.ulx, self.uly, self.lrx, self.lry]
        osi_crop_ds = gdal.Translate('', ds, projWin = bounds, format = 'VRT')

        osi_crop_arr = osi_crop_ds.ReadAsArray()

        if iceedge:
            osi_crop_arr[osi_crop_arr == 3] = 2

        return osi_crop_arr


    def project_myo(self):

        pixel_spacing = 1000
        mem_drv = gdal.GetDriverByName( 'MEM' )
        myo_projected_ds = mem_drv.Create('', int(self.lrx - self.ulx)/pixel_spacing, \
                int(self.uly - self.lry)/pixel_spacing, 1, gdal.GDT_Byte)
        new_geo = (self.ulx, 1000, self.myo_geo[2], self.uly, self.myo_geo[4], -1000 )

        myo_projected_ds.SetGeoTransform(new_geo)
        myo_projected_ds.SetProjection(self.sr_osi.ExportToWkt())

        gdal.ReprojectImage(self.g_myo, myo_projected_ds, \
                self.sr_myo.ExportToWkt(), self.sr_osi.ExportToWkt(), \
                gdal.GRA_NearestNeighbour)

        myo_arr = myo_projected_ds.ReadAsArray()
        myo_arr = np.array(myo_arr)

        return myo_arr


    def resample_myo(self, myo_arr, xsize, ysize):

        myo_resample_arr = np.zeros((xsize, ysize), np.int8)

        x_count = 0
        y_count = 0
        for x in range(0, xsize*10, 10):
            y_count = 0
            for y in range(0, ysize*10, 10):
                new_cell = myo_arr[x:x+10, y:y+10]
                if new_cell.size == 0:
                    continue
                else:
                    unique, counts = np.unique(new_cell, return_counts=True)
                    total_pixels = np.sum(counts)
                    classes = dict(zip(unique, counts))
                    valid_pixels = 0
                    water = 0
                    land = 0
                    ice = 0
                    if 0 in classes:
                        valid_pixels = valid_pixels + classes[0]
                    if 10 in classes:
                        valid_pixels = valid_pixels + classes[10]
                    if 1 in classes:
                        water = classes[1]
                    if 2 in classes:
                        ice = classes[2]
                    if 9 in classes:
                        land = classes[9]
                    if 100*(float(valid_pixels)/float(total_pixels)) > 10:
                        myo_resample_arr[x_count, y_count] = 0
                    else:
                        #(count, priority, type-code), so sorting priotises water > ice > land
                        m = sorted([(water, 3, 1), (land, 1, 9), (ice, 2, 2)])
                        val = m[2][2]
                        myo_resample_arr[x_count, y_count] = val

                y_count += 1
            x_count += 1

        return myo_resample_arr


    def generate_mask(self, myo_arr, osi_arr):
        xsize, ysize = osi_arr.shape
        out_arr = np.ones((xsize, ysize), dtype=np.bool_)
        for x in range(0, xsize):
            for y in range(0, ysize):
                current_osi = osi_arr[x, y]
                current_myo = myo_arr[x, y]
                if current_osi == -1 or current_myo in [0, 9, 10]:
                    out_arr[x, y] = 0
        return out_arr


    def generate_myo_data_mask(self, myo_arr):
        xsize, ysize = myo_arr.shape
        tmp_arr = np.ones((xsize, ysize), dtype=np.bool_)
        for x in range(0, xsize):
            for y in range(0, ysize):
                current_myo = myo_arr[x, y]
                if current_myo in [0, 10]:
                    tmp_arr[x, y] = 0


        out_arr = np.empty_like(tmp_arr)

        out_arr[tmp_arr==0] = 1
        out_arr[tmp_arr==1] = 0


        return out_arr



    def calculate_agreement(self, myo_arr, osi_arr, mask, conf_level='all', conf_arr=None):
        '''
        Returns agreement statistics as a dictionary
        Named as specified in 2.2.2 of report

        '''

        ice_ice = 0
        water_water = 0 
        water_ice = 0  
        ice_water = 0 
        valid_pixels = np.count_nonzero(mask) #include variable
        cols, rows = osi_arr.shape
        for c in range(0, cols):
            for r in range(0, rows):
                if conf_level in [0, 1, 2, 3, 4, 5]:
                    if mask[c, r] and conf_arr[c, r] == conf_level:
                        if osi_arr[c, r] == 1 and myo_arr[c, r] == 1:
                            water_water += 1
                        if osi_arr[c, r] == 2 and myo_arr[c, r] == 2:
                            ice_ice += 1 
                        if osi_arr[c, r] == 2 and myo_arr[c, r] == 1:
                            water_ice += 1
                        if osi_arr[c, r] == 1 and myo_arr[c, r] == 2:
                            ice_water += 1

                else:
                    if mask[c, r]:
                        if osi_arr[c, r] == 1 and myo_arr[c, r] == 1:
                            water_water += 1
                        if osi_arr[c, r] == 2 and myo_arr[c, r] == 2:
                            ice_ice += 1 
                        if osi_arr[c, r] == 2 and myo_arr[c, r] == 1:
                            water_ice += 1
                        if osi_arr[c, r] == 1 and myo_arr[c, r] == 2:
                            ice_water += 1


        relevant = ice_ice + water_water + water_ice + ice_water
        if relevant == 0:
            agree = 'nan'
            osi_over = 'nan'
            osi_under = 'nan'
        else:
            agree = (float(ice_ice) + float(water_water))/float(relevant)
            osi_over = float(water_ice)/float(relevant)
            osi_under = float(ice_water)/float(relevant)


        return {'water_water': water_water,
                'ice_ice': ice_ice,
                'water_ice': water_ice,
                'ice_water': ice_water,
                'relevant': relevant,
                'agree': agree,
                'osi_under': osi_under,
                'osi_over': osi_over
                } 


    def find_edge(self, arr):
        '''
        Outputs a raster ice edge. ice edge = 1, else zero.
        Requires a mask indicating the valid pixels to include
        The mask indicated the areas where both osi edge and
        myo edge have no data. 

        '''

        xsize, ysize = arr.shape
        out_arr = np.zeros((xsize, ysize), dtype=np.bool_)

        for x in range(0, xsize):
            for y in range(0, ysize):
	        current = arr[x, y]
                vals = arr[x-1:x+2, y-1:y+2]
                vals = np.array(vals.flatten())
                if current == 2 and np.any(vals == 1):
                    out_arr[x, y] = 1
        return out_arr


    def measure(self, iceedge):
        '''
        inputs an ice edge array of 1 = ice edge, else = 0
        outputs an array of euclidean distances in pixels from that ice edge
    
        '''
        cols, rows = iceedge.shape
        uniques, counts = np.unique(iceedge, return_counts=True)
        num_ice = dict(zip(uniques, counts))
        dist_all = np.full((cols, rows, num_ice[1]), sys.maxint, dtype=np.float32)

        i = 0
        for c in range(0, cols):
            for r in range(0, rows):
                val = iceedge[c, r]
                if val == 1:
                    dist = np.full((cols, rows), -1, dtype=np.float32)

                    for x in range(0, cols):
                        for y in range(0, rows):
                            dist1 = abs(x - c)
                            dist2 = abs(y - r)
                            dist[x, y] = np.sqrt((dist1 * dist1) + (dist2 * dist2))

                    dist[c, r] = 0
                    dist_all[:, :, i] = dist
                    i += 1

        final_distances = np.amin(dist_all, axis=2)
        return final_distances


    def extract_mean_distance(self, iceedge, iceedge_delta, nodata_delta, mask):
        '''
        if testing distance MyO away from OSI,
        iceedge is OSI edge, and iceedge_delta is MyO distance array 
    
        '''
        cols, rows = iceedge_delta.shape
        iceedge_distances = np.empty_like(iceedge_delta, dtype=np.float32)
    
        for c in range(0, cols):
            for r in range(0, rows):
                mask_val = mask[c, r]
                iceedge_val = iceedge[c, r]
                delta_val = iceedge_delta[c, r]
                nodata_val = nodata_delta[c, r]
                if mask_val == 1 and iceedge_val == 1 and delta_val <= nodata_val:
                    iceedge_distances[c, r] = delta_val
                else:
                    iceedge_distances[c, r] = -1
       
        iceedge_distances = iceedge_distances[iceedge_distances != -1]
        iceedge_distances = iceedge_distances.flatten()

        mean_distance = np.mean(iceedge_distances)

        return mean_distance


    def write_ds(self, arr, filename):
        '''
        Just a handy method to write an array to Geotiff
        Only write array if projected and resampled to the common projection
        So will write results of the following methods:
         - resample_myo()
         - crop_osi()
         - generate_mask()
         - find_edge()
         - measure()

        '''

        xsize, ysize = arr.shape
        self.geo = (self.ulx, 10000, 0.0, self.uly, 0.0, -10000)
        driver = gdal.GetDriverByName ( "GTiff" )
        dst_ds = driver.Create(filename, ysize, xsize, 1, gdal.GDT_Float32)
        band_1 = dst_ds.GetRasterBand(1)
        band_1.WriteArray(arr)
        dst_ds.SetProjection(self.sr_osi.ExportToWkt())
        dst_ds.SetGeoTransform(self.geo)
        dst_ds = None 
       

    def run(self):
        '''
        Method to chain together the methods to extract stats

        '''

        self.create_transform_paras()
        logging.debug('Transform parameters created.')
        osi_crop_arr = self.crop_osi(self.g_osi)
        logging.debug('Cropped OSISAF ice edge to extent of CMEMS high resolution (validation) edge.')
        osi_conf_arr = self.crop_osi(self.g_conf, iceedge=False)
        logging.debug('Cropped OSISAF ice edge confidence array to extent of CMEMS high resolution (validation) edge.')
        myo_arr = self.project_myo()
        logging.debug('CMEMS high resolution (validation) edge projected to OSISAF ice edge projection.')
        xsize, ysize = osi_crop_arr.shape
        myo_resample_arr = self.resample_myo(myo_arr, xsize, ysize)
        logging.debug('CMEMS high resolution (validation) edge resampled and aggregated to OSISAF grid.')

        myo_nodata_mask = self.generate_myo_data_mask(myo_resample_arr)
        logging.debug('Mask created of no data areas in CMEMS edge')
        mask = self.generate_mask(myo_resample_arr, osi_crop_arr)
        logging.debug('Mask created of no data areas in CMEMS edge and OSISAF edge.')
        
        iceedge_osi = self.find_edge(osi_crop_arr)
        logging.debug('Edge identified in OSISAF edge')
        iceedge_myo = self.find_edge(myo_resample_arr)
        logging.debug('Edge identified in CMEMS edge')

        myo_delta = self.measure(iceedge_myo)
        logging.debug('Distance matrix calculated for CMEMS edge')
        nodata_delta = self.measure(myo_nodata_mask)
        logging.debug('Distance matrix created for no data mask')
        distance_osi_myo = self.extract_mean_distance(iceedge_osi, myo_delta, nodata_delta, mask)
        logging.debug('Average pixel distance between edges calculated.')

        stats_all = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask)
        logging.debug('Agreement calculated successfully')
        stats_0 = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask, conf_level=0, conf_arr=osi_conf_arr)
        stats_1 = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask, conf_level=1, conf_arr=osi_conf_arr)
        stats_2 = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask, conf_level=2, conf_arr=osi_conf_arr)
        stats_3 = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask, conf_level=3, conf_arr=osi_conf_arr)
        stats_4 = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask, conf_level=4, conf_arr=osi_conf_arr)
        stats_5 = self.calculate_agreement(myo_resample_arr, osi_crop_arr, mask, conf_level=5, conf_arr=osi_conf_arr)
        logging.debug('Agreement calculated for all confidence levels.')
        stats_all['average_pixel_dist'] = distance_osi_myo
        stats_all['agreeConf0'] = stats_0['agree']
        stats_all['agreeConf1'] = stats_1['agree']
        stats_all['agreeConf2'] = stats_2['agree']
        stats_all['agreeConf3'] = stats_3['agree']
        stats_all['agreeConf4'] = stats_4['agree']
        stats_all['agreeConf5'] = stats_5['agree']

        return stats_all



if __name__ == "__main__":

    validate = OsisafValidate('ice_edge_hr_sh_20190129_043714_1.nc', 'ice_edge_sh_polstere-100_multi_201902251200.nc')

    x = np.ones((20, 20), dtype=np.uint8)
    xsize = 2
    ysize = 2
    x[1:10, 2:10] = 2
    x[2:15, 10:19] = 2
    print(x)
    y = validate.resample_myo(x, 2, 2)
    print(y)
