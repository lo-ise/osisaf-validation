from __future__ import print_function

import ftplib
import argparse
import os
import psycopg2
import logging


from osisaf_validate import OsisafValidate

logging.basicConfig(filename='/data/polarview/log/osisaf-edges.log',
        format='%(asctime)s - %(name)s - %(funcName)s - %(message)s', datefmt='%Y%m%d %I:%M:%S %p',
        level=logging.DEBUG)


parser = argparse.ArgumentParser()

parser.add_argument('-i', type=str, metavar='myo_iceedge', dest='myo_iceedge', 
               required=True, default='', 
               help='Useage: -i ice_edge_hr_sh_20190130_232427_1.nc')


args = parser.parse_args()

myo_edge_path = '/data/polarview/MyOcean/final/'

myo_iceedge = os.path.join(myo_edge_path, args.myo_iceedge)
date_string = os.path.basename(myo_iceedge)[15:23]
osi_files = 'ice_edge_sh_polstere-100_multi_{0}1200.nc'
osi_iceedge = osi_files.format(date_string)

logging.info('Validation of {0} using {1} begun.'.format(osi_iceedge, myo_iceedge))

ftppath = 'archive/ice/edge/{0}/{1}/'.format(date_string[0:4], date_string[4:6])

ftp = ftplib.FTP('osisaf.met.no')
ftp.login()
ftp.cwd(ftppath)

ftp_listing = ftp.nlst()

if osi_iceedge in ftp_listing:

    try:
        ftp.retrbinary("RETR " + osi_iceedge, open(osi_iceedge, 'wb').write)
        ftp.quit()

    except ftplib.all_errors:
        print('Failed to download file.')

    v = OsisafValidate(myo_iceedge, osi_iceedge)
    stats = v.run()
    print(stats)    

    conn = psycopg2.connect("dbname=polarview user=polarview password=TCcTCC!67. host=postgres.nerc-bas.ac.uk port=5432")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cmd = """INSERT INTO public.osisaf_validation_results(
              osisaf_edge_date, osisaf_edge_filename, validation_edge_filename, 
              agree, relevant, water_water, ice_ice, water_ice, ice_water,
              average_pixel_distance, agreeconf0, agreeconf1, agreeconf2, 
              agreeconf3, agreeconf4, agreeconf5, osi_under, osi_over
              ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    data = (date_string, osi_iceedge, os.path.basename(myo_iceedge), str(stats['agree']),
             str(stats['relevant']), str(stats['water_water']), str(stats['ice_ice']), str(stats['water_ice']),
             str(stats['ice_water']), str(stats['average_pixel_dist']), str(stats['agreeConf0']),
             str(stats['agreeConf1']), str(stats['agreeConf2']), str(stats['agreeConf3']), str(stats['agreeConf4']),
             str(stats['agreeConf5']), str(stats['osi_under']), str(stats['osi_over']), )
    cur.execute(cmd, data)
    cur.close()
    conn.close()

          
    os.remove(osi_iceedge)
           


else:
    print('file not available')
