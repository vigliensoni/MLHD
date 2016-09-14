#! /opt/sharcnet/python/2.7.5/intel/bin/python

# sqsub -r 5m -f xeon -q mpi --mpp 2GB -o likert_level -n 2 python 4_SCRIPTS/MPI/17_liking-likert-scale-generator.py 0 '/work/vigliens/GV/2_ACOUSTICBRAINZ_DATA/acousticbrainz-MBIDS-20150112.pkl'

# for ((i=0; i<=3;i++)); do echo `sqsub -r 5m -f xeon -q mpi --mpp 1GB -o likert_level -n 2 python 4_SCRIPTS/MPI/17_liking-likert-scale-generator.py $i '/work/vigliens/GV/2_ACOUSTICBRAINZ_DATA/acousticbrainz-MBIDS-20150112.pkl'`; done

# for ((i=0; i<=17;i++)); do echo `sqsub -r 8h -f xeon -q mpi --mpp 2GB -o likert_level -n 32 python ~/Documents/2_CODE/4_SCRIPTS/MPI/17_liking-likert-scale-generator.py $i '/work/vigliens/GV/2_ACOUSTICBRAINZ_DATA/acousticbrainz-MBIDS-20150112.pkl'`; done

# ##################################################
# This scripts should
# 1. Open each TAR file
# 2. extract tracks listening frequencies for all users
# 3. output a CSV file with the resulting data
# ##################################################

from mpi4py import MPI
import os
import tarfile
from optparse import OptionParser
import tempfile
import GVM_classes
# import Features
import SEM
import shutil

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
# print "SIZE:{0}, RANK:{1}".format(size, rank)

TAR_TEMP_FOLDER = tempfile.mkdtemp(dir='/tmp/')  # ComputeCanada
CSV_TEMP_FOLDER = tempfile.mkdtemp(dir='/tmp/')  # ComputeCanada
print TAR_TEMP_FOLDER, CSV_TEMP_FOLDER
# print 'TEMP_FOLDER: ', TEMP_FOLDER
# TEMP_FOLDER = tempfile.mkdtemp(dir='/Users/gabriel/scratch/') # Local

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
try:
    SQ_JOBID = os.environ["SQ_JOBID"]
except:
    SQ_JOBID = '_STARR'

if __name__ == "__main__":
    usage = "usage: %prog [options] factor"
    opts = OptionParser(usage=usage)
    # opts.add_option('-f', '--hdf5', dest='h5')
    options, args = opts.parse_args()

    factor = args[0]
    # abdata = args[1] #AcousticBrainz data
    # LOCAL FILES
    input_dir = '/Users/gabriel/Documents/5_DATA/LISTENING_HISTORIES/TEST_IN'  # LOCAL GVM
    output_folder = '/Users/gabriel/Documents/5_DATA/LISTENING_HISTORIES/TEST_OUT/'
    # input_dir = '/Users/gabriel/9_TEST/IN_MEDIUM' # LOCAL GVM
    # input_dir = '/Users/gabriel/Dropbox/9_TEST/1_TARS/' # LOCAL STARR

    # COMPUTE CANADA FILES
    # input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/IN'  # simplest case
    # output_folder = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/OUT/'
    
    # input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #8 TAR files
    # output_folder = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/OUT/'

    # input_dir = '/work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
    # output_folder = tempfile.mkdtemp(dir='/scratch/vigliens/out/')

    # print '\nINPUT DIR:{0}'.format(input_dir)

    # ##########################################################
    # Extracts all files within a TAR to TEMP_FOLDER
    # TODO: Extract only in one node? (which one) and bcast to
    # the other ones
    # ##########################################################

    file_list = GVM_classes.folder_iterator(input_dir)  # List of all files in input_dir

    # print 'FILE_LIST:{0}'.format(file_list)
    list_factor = size * int(factor) + rank  # unique value for each parallel instance of the code
    # print "LIST FACTOR:{0}".format(list_factor)
    filename = file_list[list_factor]
    # print 'FILENAME:{0}'.format(filename)
    tar_object = tarfile.open('/'.join([filename]))
    tar_object.extractall(TAR_TEMP_FOLDER)

    print "SIZE:{0}, TAR FILENAME: {1}".format(size * int(factor) + rank, filename)

    # ################################
    # Saving
    # ################################

    # Iterate over all files in a TAR
    dp = SEM.DataPreprocessing()
    dp.likertcsvwriter(CSV_TEMP_FOLDER, output_folder)
    out_filename = GVM_classes.filenameparser(filename, 'basename')
    shutil.make_archive('/'.join([output_folder, out_filename]), 'gztar', output_folder)
    shutil.rmtree(TAR_TEMP_FOLDER)
    shutil.rmtree(CSV_TEMP_FOLDER)






