"""
This script searches specified directories for files to archive and compress. It is built to find output from STACK and from twitter-timeline-scraper.
It uses the modified times for the files in each specified directory to decide which files to compress together. All files created on a given day are combined; all files created the previous day are combined in a different file; the day before in a different file; etc.
It only does anything with files that were last modified the previous calendar day.

The script is built to run forever. After it finishes compressing all the files it found to compress, it goes to sleep. It wakes up every hour and checks to see if it's a new day; if it is, it does its work again; if not, it goes back to sleep.
"""

import tarfile
import os
os.nice(15)                     # This lowers the priority of this process, which means other processes have higher priority for computational resources
import datetime
import time
import argparse
import pymongo
import subprocess
import sys
import shutil
import logging

from config import mongo_auth

dirs_to_zip = []
archive_path_base = '/mnt/data'
stack_dir = '/home/bits/stack'

ap = argparse.ArgumentParser()
ap.add_argument("-n","--name", type=str, required=True, help="The name of the server where this script is running")
ap.add_argument("-t","--time", type=int, required=True, choices=range(0,24), metavar="[0-23]", help="The hour (in 24 hour format) when this script should run")
ap.add_argument("-d","--delete", required=False, action='store_true', help="Include this option if you want to delete the files after they are compressed. By default, these files are not deleted")
ap.add_argument("-a","--archive", required=False, action='store_true', help="Include this option if you want to move the tar.gz files to the archive drive. By default, these files are not moved")
ap.add_argument("-m","--mongo", required=False, action='store_true', help="Include this option if you want the script to automatically build the list of dirs to zip based on active projects found in Mongo")
ap.add_argument("-s","--stack-path", required=False, help="Include this option if STACK is not installed in the default path (/home/bits/stack)")
ap.add_argument("-M", "--manual", type=str, required=False, help="Include this option if you want the code to work only on data in a specified folder. Use this option if the data you want the code to work on isn't STACK data")
ap.add_argument("-N", "--manual-name", type=str, required=False, help="Include this if using \"-M\" to specify the name of the project. This is important for archiving")
ap.add_argument("-l", "--log-name", required=False, help="Include this if you plan to run zipper more than once at the same time. (You might do that if you have data in different folders that you want to zip.)")
#args = vars(ap.parse_args('-n n -t 1'.split()))
args = vars(ap.parse_args())

server_name = args['name']
time_to_run = args['time']

if args['log_name']:
    log_file_name = args['log_name'] + '.log'
elif not args['log_name']:
    log_file_name = 'zipper.log'
logging.basicConfig(filename=log_file_name, filemode='a+', level=logging.DEBUG, format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s")

if args['stack_path']:
    stack_dir = args['stack_path']

if args['archive']:
    archive_drive_mounted = os.path.isdir(archive_path_base)
    if not archive_drive_mounted:
        logging.critical("Archive drive can't be found! The path is wrong or it isn't mounted.")
        raise Exception

if args['manual']:
    if not args['manual_name']:
        logging.critical("You didn't provide a name for the data you pointed STACKzip to manually.")
        raise Exception
    dir_to_zip = args['manual']
    if not os.path.isdir(args['manual']):
        logging.critical("The directory you provided with non-STACK data doesn't exist.")
        raise Exception
        
    
def dynamic_project_identification():
    """
    This function looks in Mongo and ps for active projects. Users can specify for the script to find project names
     and build dirs_to_zip or pass the paths for dirs_to_zip directly.
    """
    mongoClient = pymongo.MongoClient()
    if mongo_auth['AUTH']:
        mongoClient.admin.authenticate(mongo_auth['username'], mongo_auth['password'])
    dbs = mongoClient.database_names()
    projects = [f for f in dbs if '_' in f]
    projects = [f for f in projects if not 'delete' in f]
    projects = [f for f in projects if len(f) > 24]
    running_projects = []
    for project in projects:
        project_info_string = project.split('_')
        process_status = subprocess.getoutput('ps -ef | grep "{}"'.format(project_info_string[1])).split('\n')
        process_status = [x for x in process_status if "collect" in x]
        if len(process_status) > 0:
            running_projects.append(project)
    if len(running_projects) > 0:
        logging.info('Found {} running STACK projects'.format(len(running_projects)))
    elif len(running_projects) == 0:
        logging.warning('No running STACK projects found. Did you mean to manually specify a directory to archive?\n')
        raise Exception
    dirs_to_zip = []
    for p in running_projects:
        p = p.replace('_', '-')
        data_path = os.path.join(stack_dir, 'data', p, 'twitter', 'archive')
        if os.path.isdir(data_path):
            dirs_to_zip.append(data_path)
        elif not os.path.isdir(data_path):
            logging.critical('Problem! I can\'t find data for project {}. Are there multiple installs of STACK?\n'.format(p))
            raise Exception
    return dirs_to_zip


def stack_archiving():
    today = datetime.datetime.today()
    logging.info(today.replace(microsecond=0).isoformat())
    if args['mongo']:
        dirs_to_zip = dynamic_project_identification()
    logging.info('Checking {} for json files to compress'.format(' and '.join(dirs_to_zip)))

    for dir_to_zip in dirs_to_zip:
        project_name = dir_to_zip.split('/')
        project_name = [f for f in project_name if '-' in f]
        if len(project_name) == 1:
            project_name = project_name[0].split('-')[0]

            dir_contents = os.listdir(dir_to_zip)
            dir_contents = [os.path.join(dir_to_zip, f) for f in dir_contents]
            file_list = [f for f in dir_contents if os.path.isfile(f)]
            files_to_combine_and_compress = [f for f in file_list if not (('processed' in f) or ('tar.gz' in f))]
            processed_files = [f for f in file_list if 'processed' in f]
            processed_archive_path = os.path.join(dir_to_zip, 'processed_archive')
            os.makedirs(processed_archive_path, exist_ok=True)

            daily_files = {}
            for f in files_to_combine_and_compress:
                date = datetime.datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d')
                if not date in daily_files:
                    daily_files[date] = [f]
                elif daily_files[date]:
                    daily_files[date].append(f)

            logging.info("For project {0}, found {1} files corresponding to {2} days to combine and compress.".format(project_name, len(
                files_to_combine_and_compress), len(daily_files)))
            for date in daily_files:
                back_to_date = datetime.datetime.strptime(date, '%Y-%m-%d')
                gap = today - back_to_date
                if gap > datetime.timedelta(1) and len(daily_files[date]) > 1:
                    files = daily_files[date]
                    tar_name = '{0}-{1}-{2}.tar.gz'.format(server_name,project_name,date)
                    if args['archive']:                     # If you tell the script to archive zip files, it sends them to the archive folder
                        tar_path = os.path.join(archive_path_base, server_name, project_name)
                    elif not args['archive']:               # If you don't tell the script to archive zip files, it creates a folder for zip files in the folder that contained the original data
                        tar_path = os.path.dirname(os.path.join(dir_to_zip, 'tar_files'))
                    try:
                        os.makedirs(tar_path, exist_ok=True)
                    except:
                        logging.critical("Cannot make the tar file folder called {0}.".format(tar_path))
                        raise Exception
                    try:
                        with tarfile.open(tar_name, 'w:gz') as tar:
                            for file in files:
                                tar.add(file)
                    except:
                        logging.critical("Cannot make the tar file called {0}.".format(tarfile))
                        raise Exception
                    shutil.move(tar_name, os.path.join(tar_path, tar_name))
                    logging.info('Zipped {0} files to {1}'.format(len(files),tar_path))
                    if args['delete']:                      # This loop deletes files after they have been zipped.
                        for file in files:
                            os.remove(file)
                        logging.info('Deleted {} raw files'.format(len(files)))
                    elif not args['delete']:                # If raw files aren't deleted, they are moved to an archive folder
                        archive_path = os.path.join(dir_to_zip, 'archive')
                        os.makedirs(archive_path, exist_ok=True)
                        for file in files:
                            os.rename(file, os.path.join(archive_path, os.path.basename(file)))
                        logging.info('Moved {0} raw files to {1}'.format(len(files),os.path.join(dir_to_zip, 'archive')))

            daily_processed_files = {}
            for pf in processed_files:
                date = datetime.datetime.fromtimestamp(os.path.getmtime(pf)).strftime('%Y-%m-%d')
                if not date in daily_processed_files:
                    daily_processed_files[date] = [pf]
                elif daily_processed_files[date]:
                    daily_processed_files[date].append(pf)
            for date in daily_processed_files:
                back_to_date = datetime.datetime.strptime(date, '%Y-%m-%d')
                gap = today - back_to_date
                if gap > datetime.timedelta(1):
                    files = daily_processed_files[date]
                    for pf in files:
                        os.remove(pf)
                    logging.info('Deleted {} STACK processed files.'.format(len(files)))

        elif not len(project_name) == 1:
            logging.critical("Project name cannot be identified from path: \n\t{}\nCheck path and try again.\n".format(dir_to_zip))
            raise Exception


def other_archiving():
    today = datetime.datetime.today()
    logging.info(today.replace(microsecond=0).isoformat())
    logging.info('Checking {} for json files to compress'.format(dir_to_zip))

    # 1 zip file should have one cadidate's tweets from one dat
    project_name = args['manual_name']
    dir_contents = os.listdir(dir_to_zip)
    dir_contents = [os.path.join(dir_to_zip, f) for f in dir_contents]
    file_list = [f for f in dir_contents if os.path.isfile(f)]
    files_to_combine_and_compress = [f for f in file_list if not (('processed' in f) or ('tar.gz' in f))]
    candidates = list(set([f.split('-')[3] for f in files_to_combine_and_compress]))
    logging.info("Found {0} files to zip corresponding to {1} candidates".format(len(files_to_combine_and_compress), len(candidates)))
    processed_files = [f for f in file_list if 'processed' in f]
    processed_archive_path = os.path.join(dir_to_zip, 'processed_archive')
    os.makedirs(processed_archive_path, exist_ok=True)

    candidate_files = {}
    for c in candidates:
        daily_files = {}
        for f in files_to_combine_and_compress:
            if c in f:
                date = datetime.datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d')
                if not date in daily_files:
                    daily_files[date] = [f]
                elif daily_files[date]:
                    daily_files[date].append(f)
        candidate_files[c] = daily_files

    for c in candidate_files:
        daily_files = candidate_files[c]
        logging.info("For candidate {0}, found files corresponding to {1} days".format(c, len(daily_files)))
        for date in daily_files:
            back_to_date = datetime.datetime.strptime(date, '%Y-%m-%d')
            gap = today - back_to_date
            if gap > datetime.timedelta(1) and len(daily_files[date]) > 1:
                files = daily_files[date]
                tar_name = '{0}-{1}-{2}-{3}.tar.gz'.format(server_name, project_name, c, date)
                if args['archive']:  # If you tell the script to archive zip files, it sends them to the archive folder
                    tar_path = os.path.join(archive_path_base, server_name, project_name)
                elif not args['archive']:  # If you don't tell the script to archive zip files, it creates a folder for zip files in the folder that contained the original data
                    tar_path = os.path.dirname(os.path.join(dir_to_zip, 'tar_files'))
                try:
                    os.makedirs(tar_path, exist_ok=True)
                except:
                    logging.critical("Cannot make the tar file folder called {0}.".format(tar_path))
                    raise Exception
                try:
                    with tarfile.open(tar_name, 'w:gz') as tar:
                        for file in files:
                            tar.add(file)
                except:
                    logging.critical("Cannot make the tar file called {0}.".format(tarfile))
                    raise Exception
                shutil.move(tar_name, os.path.join(tar_path, tar_name))
                logging.info('Zipped {0} files to {1}'.format(len(files), tar_path))
                if args['delete']:  # This loop deletes files after they have been zipped.
                    for file in files:
                        os.remove(file)
                    logging.info('Deleted {} raw files'.format(len(files)))
                elif not args['delete']:  # If raw files aren't deleted, they are moved to an archive folder
                    archive_path = os.path.join(dir_to_zip, 'archive')
                    os.makedirs(archive_path, exist_ok=True)
                    for file in files:
                        os.rename(file, os.path.join(archive_path, os.path.basename(file)))
                    logging.info('Moved {0} raw files to {1}'.format(len(files), os.path.join(dir_to_zip, 'archive')))

if __name__ == '__main__':
    while True:
        if not args['manual']:
            stack_archiving()
        elif args['manual']:
            other_archiving()
        logging.info('Zipping complete. Sleeping until tomorrow.\n')
        sleep = True
        while sleep:
            time.sleep(60*60)
            now = datetime.datetime.now()
            if now.hour == time_to_run:
                sleep = False

