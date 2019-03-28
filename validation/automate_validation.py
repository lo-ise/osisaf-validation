from __future__ import print_function

import ftplib
import argparse
import os
import psycopg2
import logging
import glob
from datetime import datetime, timedelta

from osisaf_validate import OsisafValidate

logging.basicConfig(filename='/data/polarview/log/osisaf-edges.log',
        format='%(asctime)s - %(name)s - %(funcName)s - %(message)s', datefmt='%Y%m%d %I:%M:%S %p',
        level=logging.DEBUG)


myo_edge_path = '/data/polarview/MyOcean/final/'
osi_files = 'ice_edge_sh_polstere-100_multi_{0}1200.nc'
osi_tmp_path = '/users/polarview/scripts/osisaf/tmp/'

def check_if_already_done(cur, filename):
    cur.execute("SELECT validation_edge_filename FROM public.osisaf_validation_results WHERE validation_edge_filename = %s", (filename,))
    return cur.fetchone() is not None


if __name__ == "__main__":
    today = datetime.now()
    today = today.strftime('%Y-%m-%d %H:%M')
    logging.info('Process started on {0}'.format(today))

    dt = datetime.now() - timedelta(days=2) #checking two days ago

    date_string = dt.strftime('%Y%m%d')
    logging.info('Checking for validation edges dated dated {0}'.format(date_string))
    all_edges = glob.glob('{0}ice_edge_hr_sh_{1}*.nc'.format(myo_edge_path, date_string))

    if all_edges == []:
        logging.info('No edges exists dated {0}'.format(date_string))
        logging.info('Exiting process')

    else:

        conn = psycopg2.connect("dbname=polarview user=polarview password=TCcTCC!67. host=postgres.nerc-bas.ac.uk port=5432")
        logging.info('Connection made to polarview db - postgres.nerc-bas.ac.uk')
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        for e in all_edges: # in case more than one
            myo_iceedge = os.path.basename(e)
            osi_iceedge = osi_files.format(date_string)
            if not check_if_already_done(cur, myo_iceedge):
                logging.info('Validation of {0} using {1} begun...'.format(osi_iceedge, myo_iceedge))
                
                # check osisaf edge ftp
                ftppath = 'archive/ice/edge/{0}/{1}/'.format(date_string[0:4], date_string[4:6])
                ftp = ftplib.FTP('osisaf.met.no')
                ftp.login()
                ftp.cwd(ftppath)

                ftp_listing = ftp.nlst()
              
                if osi_iceedge in ftp_listing:
                    osi_iceedge_path = os.path.join(osi_tmp_path, osi_iceedge)    
                    try:
                        ftp.retrbinary("RETR " + osi_iceedge, open(osi_iceedge_path, 'wb').write)
                        ftp.quit()
                        logging.info('Download of {0} successful.'.format(osi_iceedge))

                    except ftplib.all_errors:
                        logging.info('Failed to download {0}.'.format(osi_iceedge))
                    
                    v = OsisafValidate(e, osi_iceedge_path)
                    stats = v.run()
                    logging.info('Validation process run sucessfully.')
                    logging.info('Updating db table....')

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
                    logging.info('db table updated with results from validation of {0} using {1}'.format(osi_iceedge, myo_iceedge))            
                    os.remove('/users/polarview/scripts/osisaf/tmp/' + osi_iceedge)
           

                else:
                    logging.info('OSISAF edge for {0} not available'.format(datestring))

            else:
                logging.info('Validation of {0} using {1} already completed.'.format(osi_iceedge, myo_iceedge))


        cur.close()
        conn.close()
