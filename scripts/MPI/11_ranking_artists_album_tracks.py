#! /opt/sharcnet/python/2.7.5/intel/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 1h -f xeon -q mpi --mpp 4GB -o .ranking_tracks_test -n 8 python -u 4_SCRIPTS/MPI/11_ranking_artists_album_tracks.py 0

# sqsub -r 1h -f xeon -q mpi --mpp 4GB -o /scratch/vigliens/ranking_tracks_1h_4g_32n -n 32 python -u 4_SCRIPTS/MPI/11_ranking_artists_album_tracks.py 0

# for ((i=0; i<=11;i++)); do echo `sqsub -r 30m -f xeon -q mpi --mpp 4GB -o .ranking_tracks_test -n 48 python 4_SCRIPTS/MPI/11_ranking_artists_album_tracks.py $i`; done


# ################################################################################
# This script should extract listening behaviour features and store these in a 
# single HDF5  dataset file
# ################################################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
import GVM_classes, Features
from H5 import *
import tempfile
import h5py
from optparse import OptionParser
import copy
import shutil
import cPickle
import json
import os.path
# import subprocess


comm = MPI.COMM_WORLD 
size = comm.Get_size() 
rank = comm.Get_rank() 

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]

output_file = '/scratch/vigliens/track_ranking.pickle'


if __name__ == "__main__":
	usage = "usage: %prog [options] factor H5_FILES_ROOT"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# Factor to overcome the 256 CPUs limitation.
	# Each run must have a different factor
	# E.g., to run the 583 TAR files, it would be 
	# possible to do 256, 256, and 71. However
	# it is more balanced to do 192, 192, and 199; as in 
	# sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

	factor = args[0]
	# H5FILES_ROOT = args[1]

	# log_file = open('.extracting_temporal_features.log', 'a')

	# ##########################################################
	# Init parameteres for where userfiles are
	# Creates TAR object and extracts its members to TMP folder
	# ##########################################################	

	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS' #test case
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #8 TAR files
	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
	# H5_FILEPATH = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/number_of_songs.h5" # TEST
	# H5_FILEPATH = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/aggregated_listening.h5" # FULL
	

	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print size * int(factor) + rank, file_list[size * int(factor) + rank]

	# ##########################################################
	# Iterate over all files in a TAR, searching for all MBIDs
	# ##########################################################

	full_frequencies = dict()
	

	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER)[:]:

		# Data extraction
		listening_features = Features.ListeningFeatures(file_in_tar)	# Initializes object for feature extraction 
		# frequencies = listening_features.artist_mbid_frequencies() #Extracts artist MBID frequencies
		# frequencies = listening_features.album_mbid_frequencies()
		frequencies = listening_features.track_mbid_frequencies()

		# all_frequencies = {'artist': artist_mbid_frequencies, 
		# 					'album': album_mbid_frequencies, 
		# 					'track': track_mbid_frequencies}
		
		# Merging the new and old dictionary
		full_frequencies = GVM_classes.dict_merger_adder(full_frequencies, frequencies) 
		# dn = { k: d1.get(k, 0) + d2.get(k, 0) for k in set(d1) | set(d2) }
		# print 'file_in_tar:{0}, len_freq:{1}'.format(file_in_tar, len(full_frequencies.items()))

	print 'RANK ', rank, ' finished'

	shutil.rmtree(TEMP_FOLDER)
	comm.Barrier() 

	# ##########################################################
	# Gather on root
	# ##########################################################
	
	# dict_agg_feat_list = copy.copy(comm.gather(list_of_dict_agg_feat, root=0))
	dictionary_list_gathered = copy.copy(comm.gather(full_frequencies, root=0))
	del full_frequencies, frequencies, listening_features
	# dictionary_list_gathered = comm.gather(full_frequencies, root=0)
	# artist = dict()
	# album = dict()
	# track = dict()

	
	# del dictionary_list

	if rank == 0:
		dict_this_round = dict()

		# # Setting up the HDF5 file
		# h = H5(H5_FILEPATH)
		# h._groups_and_datasets_checker()
		# for dictionary in dictionary_list_gathered:
		# 	artist = GVM_classes.dict_merger_adder(artist, dictionary['artist'], method = 'merging')
			# album = GVM_classes.dict_merger_adder(album, dictionary['album'], method = 'merging')
			# track = GVM_classes.dict_merger_adder(track, dictionary['track'], method = 'merging')

		# # Writing data
		# for frequency in all_frequencies_from_single_file:

		# Merge dictionary data from all cores
		for dictionary in dictionary_list_gathered:
			print 'length_dictionaries:{0}'.format(len(dictionary.items()))
			dict_this_round = GVM_classes.dict_merger_adder(dict_this_round, dictionary)
			print 'length_dict_this_round:{0}'.format(len(dict_this_round.items()))
		
		################################################################
		# Merge dictionary data from this core batch with previous batch
		################################################################
		# Create file if it does not exist
		if not os.path.isfile(output_file): 
			with open(output_file, 'wb') as handle:
				print 'CREATING RANKING FILE'
				cPickle.dump(dict(), handle)

		
		# Opening previous file
		with open(output_file, 'rb') as input_file:
			print 'LOADING RANKING FILE'
			dict_previous_round = cPickle.load(input_file)
		# Merging new and old dictionary
		dict_new_round = GVM_classes.dict_merger_adder(dict_previous_round, dict_this_round)

		# Storing the new dictionary
		with open(output_file, 'wb') as handle:
			print 'UPDATING RANKING FILE'
  			cPickle.dump(dict_new_round, handle)




