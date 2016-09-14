#! /opt/sharcnet/python/2.7.5/intel/bin/python

#                                 sqsub -r 30m -f xeon -q mpi --mpp 2GB -o rating -n 2 python 4_SCRIPTS/MPI/20_rating_generator.py 0

# for ((i=0; i<=3;i++)); do echo `sqsub -r 30m -f xeon -q mpi --mpp 2GB -o rating -n 2 python 4_SCRIPTS/MPI/20_rating_generator.py $i`; done

# for ((i=0; i<=72;i++)); do echo `sqsub -r 1h -f xeon -q mpi --mpp 2GB -o rating -n 8 python 4_SCRIPTS/MPI/20_rating_generator.py $i`; done

# for ((i=7300478; i<=7300550;i++)); do echo `sqkill $i`; done

# ##################################################
# This scripts should
# Create a rating file for each user in the form of
# usid, mbid, rating
# It is using weekday and weekend data
# ##################################################


from mpi4py import MPI
import os
import shutil
import tarfile
from optparse import OptionParser
import tempfile
import GVM_classes
import Features

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens')  # ComputeCanada


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

    # LOCAL FILES
    # input_dir = '/Users/gabriel/Documents/5_DATA/LISTENING_HISTORIES/TEST_IN/'

    # COMPUTE CANADA FILES
    # input_dir = '/work/vigliens/GV/1_LASTFM_DATA/3_TEST_8_TARS' #8 TAR files

    input_dir = '/work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
    output_folder = '/scratch/vigliens/out/'

    # print '\nINPUT DIR:{0}'.format(input_dir)

    # ##########################################################
    # Extracts all files within a TAR to TEMP_FOLDER
    # TODO: Extract only in one node? (which one) and bcast to
    # the other ones
    # ##########################################################

    file_list = GVM_classes.folder_iterator(input_dir)  # List of all files in input_dir
    list_factor = size * int(factor) + rank  # unique value for each parallel instance of the code
    filepath = file_list[list_factor]
    tar_object = tarfile.open('/'.join([filepath]))
    tar_object.extractall(TEMP_FOLDER)

    print "SIZE:{0}, TAR FILENAME: {1}".format(size * int(factor) + rank, filepath)

    # ################################
    # Processing
    # ################################

    file_list = GVM_classes.folder_iterator(TEMP_FOLDER)

    for file_in_tar in file_list:
        if file_in_tar.split('/')[-1].startswith('.'):
            continue
        lf = Features.ListeningFeatures(file_in_tar)

        time_brackets = ['weekday', 'weekend']
        for time_bracket in time_brackets:
            # generating ordered dict of frequencies
            frequencies = lf.timebracketextractor('artist', time_bracket)
            # generating rating values
            ratings = GVM_classes.frequencytoratingconverter(frequencies)
            ratings['usid'] = int(lf.lfid)
            ratings = ratings[['usid', 'mbid', 'freq', 'likert']]


            filename = GVM_classes.filenameparser(file_in_tar, 'basename').split('.')[0] + '.csv'
            fullpathfilename = os.path.join(output_folder, time_bracket, filename)

            ratings.to_csv(fullpathfilename,
                           sep='\t',
                           columns=['usid', 'mbid', 'freq', 'likert'],
                           index=False,
                           header=False)
        
    shutil.rmtree(TEMP_FOLDER)
