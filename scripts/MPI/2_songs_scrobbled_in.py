#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 10m -f xeon -q mpi -o extracting_scrobbles_for_billboard -n 2 python ./4_SCRIPTS/MPI/2_songs_scrobbled_in.py 0 (4 songs)

# #############################################################################
# Working scrip for extracting series of log UTS per user per song
# Returns a first line with all billboard data for a song, and then
# a list of users with all respective UTS for each log on that song.
# E.g.,
# 14032   Big Girls Don't Cry     Fergie  5/5/2007        41    3/29/2008   45  9/8/2007        1   48  18  40c8c738- ...
# 8117177 3 1200781349,1208145269,1209626674
# 40933029        6 1328120272,1332187536,1342536119,1342536768,1350714616,1365226575
# ...
# #############################################################################

from mpi4py import MPI
import os
import tarfile
import GVM_classes
import Features
import tempfile
from optparse import OptionParser

rank = MPI.COMM_WORLD.Get_rank()
size = MPI.COMM_WORLD.Get_size()
name = MPI.Get_processor_name()
# print "\nHelloworld! I am process {0} of {1} on {2}".format(rank, size, name)

rank_lead_zero = "%02d" % (rank + 1,)
TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]

if __name__ == "__main__":
    usage = "usage: %prog [options] factor"
    opts = OptionParser(usage=usage)
    opts.add_option('-f', '--hdf5', dest='h5')
    options, args = opts.parse_args()

    # Factor to overcome the 256 CPUs limitation.
    # Each run must have a different factor
    # E.g., to run the 583 TAR files, it would be
    # possible to do 256, 256, and 71. However
    # it is more balanced to do 192, 192, and 199; as in
    # sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

    factor = args[0]

    # # ############
    # # OPTIONS
    # # ############
    # if options.h5 == 'user':
    #   # output_filepath = os.path.join(args[1], 'consolidated_daily_freqs.txt')
    #   pass
    # elif options.h5 == 'song':
    #   # output_filepath = os.path.join(args[1], 'consolidated_weekly_freqs.txt')
    #   pass
    # elif options.h5 == 'billboard':
    #   # h5 = h5py.File('/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/BILLBOARD/billboard.h5')
    #   pass
    # else:
    #   'You must provide a destination for the hdf5 file'
    #   sys.exit()
    # Extracting song_data from the billboard file:
    billboard_file = open(PBS_O_WORKDIR + '/4_SCRIPTS/BILLBOARD/top_200_billboard_2005_2011_mbid_2_no_feat.tsv')
    billboard_lines = billboard_file.readlines()
    songs_data = [x.strip().split('\t') for x in billboard_lines[1:32] if x.strip().split('\t')[11] is not '']

    # Printing first line with song metadata first
    for song_data in songs_data:
        if rank == 0:
            # Catches userlogs with no data
            try:
                song_mbid = song_data[11]
                out_file = open(PBS_O_WORKDIR + '/BILLBOARD_DATA/' + song_mbid + '.dat', 'a')
                out_file.write('\t'.join(song_data) + '\n')
                print '{0}'.format('\t'.join(song_data))
                out_file.close()
            except Exception, e:
                print e, '\n'

    # Init parameteres for where userfiles are
    # Creates TAR object and extracts its members to TMP folder
    input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB'
    file_list = []  # List of all files in input_dir
    for root, subFolders, files in os.walk(input_dir):
        for f in files:
            file_list.append('/'.join([root, f]))
    tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
    tar_object.extractall(TEMP_FOLDER)

    # ##########################################################
    # Iterate over all files in a TAR, searching for all songs
    # ##########################################################
    for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER):
        user_features = Features.ListeningFeatures(file_in_tar)  # Initializes object for feature extraction
        for song_data in songs_data:    # Iterate over all songs in the list
            utc_times_per_song = []
            # Catches userlogs with no data
            try:
                song_mbid = song_data[11]
                out_file = open(PBS_O_WORKDIR + '/BILLBOARD_DATA/' + song_mbid + '.dat', 'a')
                utc_times_per_song = user_features.utc_times_per_song(song_mbid)
            except Exception, e:
                print file_list[size * int(factor) + rank], file_in_tar, e, '\n'

            # Print only if listener listened to the song
            if len(utc_times_per_song) > 0:
                utc_times_per_song_string = [str(l) for l in utc_times_per_song]  #making a string from list
                out_file.write('\t'.join([str(user_features.lfid), str(len(utc_times_per_song)), ','.join(utc_times_per_song_string), '\n']))               
            out_file.close()

    MPI.Finalize()
