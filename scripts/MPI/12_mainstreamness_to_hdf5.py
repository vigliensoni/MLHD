#! /opt/sharcnet/python/2.7.5/intel/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 5m -q mpi --mpp 6GB -o test_mainstreamness.log -n 2 python 4_SCRIPTS/MPI/12_mainstreamness_to_hdf5.py 0 /scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS/ /scratch/vigliens/GV/8_RESULTS/mainstreamness.h5 /scratch/vigliens/GV/8_RESULTS/grouped_ranking_artists.dump /scratch/vigliens/GV/8_RESULTS/grouped_ranking_albums.dump /scratch/vigliens/GV/8_RESULTS/grouped_ranking_tracks.dump

# for ((i=0; i<=5;i++)); do echo `sqsub -r 3h -q mpi --mpp 6GB -o mainstreamness_3h_6g_96n_saw.log -n 96 python 4_SCRIPTS/MPI/12_mainstreamness_to_hdf5.py $i /work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB/ /scratch/vigliens/GV/8_RESULTS/mainstreamness.h5 /scratch/vigliens/GV/8_RESULTS/grouped_ranking_artists.dump /scratch/vigliens/GV/8_RESULTS/grouped_ranking_albums.dump /scratch/vigliens/GV/8_RESULTS/grouped_ranking_tracks.dump`; done


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
import collections
import time


comm = MPI.COMM_WORLD 
size = comm.Get_size() 
rank = comm.Get_rank() 


try:
	TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
except:
	print '\nCreating TEMP folder in /var/'
	TEMP_FOLDER = tempfile.mkdtemp()

try:
	SQ_JOBID = os.environ["SQ_JOBID"]
except:
	SQ_JOBID = '_STARR'

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]

# input_folder, H5_FILEPATH, precomputed_artist_ranking, precomputed_album_ranking, precomputed_track_ranking



def run(input_folder, H5_FILEPATH, precomputed_artist_ranking, precomputed_album_ranking, precomputed_track_ranking):
	"""
	Running the core code. Taking this approach for creating a different namespace and avoiding memory issues
	"""		

	# def _dump_to_dict(dump_filepath):
	# 	"""
	# 	Convert a numpy array in the form (('k1', v1), ('k2', v2), ... , ('kn', vn)) to a dictionary. It also deletes an empty key (''), and the dictionary is converted to a collection and is ordered by value
	# 	"""
	# 	with open(dump_filepath, 'rb') as handle:
	# 		f = cPickle.load(handle)
	# 	t0 = time.time()
	# 	d = {k : v for k, v in f}; del f	
	# 	print '{0} secs for creating dict from dump {1}'.format(int(time.time() - t0), dump_filepath),
	# 	# do not consider empty MBID's
	# 	if d.has_key(''): d.pop('', None) 
	# 	# return sorted ranking by value
	# 	return collections.OrderedDict(sorted(d.items(), key=lambda t: t[1])) 



	global size
	global rank

	# Generating ordered dictionaries of the rankings
	t0 = time.time()
	overall_ranking_artist = GVM_classes.dump_to_dict(precomputed_artist_ranking)
	# if rank == 0: print ' size: {0}'. format(sys.getsizeof(overall_ranking_artist))

	overall_ranking_album = GVM_classes.dump_to_dict(precomputed_album_ranking)
	# if rank == 0: print ' size: {0}'. format(sys.getsizeof(overall_ranking_album))

	overall_ranking_track = GVM_classes.dump_to_dict(precomputed_track_ranking)
	# if rank == 0: print ' size: {0}'. format(sys.getsizeof(overall_ranking_track))
	print 'Rank', rank, 'features in', str(int(time.time() - t0)), 'secs'

	# ##########################################################
	# Iterate over all files in a TAR, searching for all MBIDs
	# ##########################################################

	file_list = [] # List of all files in input_folder
	for root, subFolders, files in os.walk(input_folder):
		for f in files:
			if f.split('/')[-1].startswith('.'):
				continue
			file_list.append('/'.join([root,f]))

	# print 'RANK:', rank, '\nFILE_LIST:', file_list
	# print 'FILE: ', file_list[size * int(factor) + rank]
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	# print size * int(factor) + rank, file_list[size * int(factor) + rank]

	#list with dictionaries of aggregated features
	list_of_dict_agg_feat= []



	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER)[:]:
		listening_features = Features.ListeningFeatures(file_in_tar) 
		try:
			# Metadata
			
			
			# Feature Extraction
			collected_features = dict()

			collected_features['metadata'] = listening_features.metadata_dict()
			collected_features['mainstreamness'] = listening_features.mainstreamness(overall_ranking_artist, overall_ranking_album, overall_ranking_track)

			

			list_of_dict_agg_feat.append(collected_features)

			# print "In file {0}, there are {1} extracted users".format(file_list[size * int(factor) + rank], len(list_of_dict_agg_feat))

		except:
			print file_list[size * int(factor) + rank].split('/')[-1], file_in_tar.split('/')[-1], sys.exc_info()

	return list_of_dict_agg_feat


if __name__ == "__main__":
	usage = "usage: %prog [options] factor input_folder output_folder H5_FILEPATH precomputed_ranking_filepath precomputed_album_ranking precomputed_track_ranking"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# ###########################################
	#  ARGS
	# ###########################################

	if len(args) != 6:
		print 'You should provide a:\n\n \
				1) factor\n \
				2) input_folder with TAR files\n \
				3) H5 file outpath\n \
				3) A filepath of the precomputed ranking for artist\n \
				4) A filepath of the precomputed ranking for album\n \
				5) A filepath of the precomputed ranking for track'
		sys.exit("\nEnter the proper values\n")

	t00 = time.time()
	factor = args[0]
	input_folder = args[1]
	H5_FILEPATH = args[2]
	precomputed_artist_ranking = args[3]
	precomputed_album_ranking = args[4]
	precomputed_track_ranking = args[5]
	
	list_of_dict_agg_feat = run(input_folder, H5_FILEPATH, precomputed_artist_ranking, precomputed_album_ranking, precomputed_track_ranking)

	# print 'list_of_dict_agg_feat', list_of_dict_agg_feat



	# ##########################################################
	# Gather on root
	# ##########################################################
	list_all_dicts = comm.gather(list_of_dict_agg_feat, root=0)

	# print TEMP_FOLDER
	shutil.rmtree(TEMP_FOLDER)
	del list_of_dict_agg_feat
	
	print 'Rank {0} finished in {1} seconds'.format(rank, str(int(time.time() - t00)))

	if rank == 0:

		def _writing_hdf5(dict_data, feature_path, feature_name):
			"""
			"""
			# Setting up the HDF5 file
			# print 'LENGTH FEATURE ALL DICTS:{0}'.format(len(dict_data))
			h = H5(H5_FILEPATH)
			h._groups_and_datasets_checker()

			# Writing data
			for data in dict_data:
				for d in data:
					# Populating the compound dataset using H5 library
					# print 'd: ', d
					h.populate_compound_dataset(feature_path, d[feature_name])
			h.close()

		_writing_hdf5(list_all_dicts, 'features/listening_behaviour/mainstreamness', 'mainstreamness')
		