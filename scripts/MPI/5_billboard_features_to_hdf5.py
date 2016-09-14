#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 20m -f xeon -q mpi --mpp 2GB -o billboard_to_h5 -n 2 python ./4_SCRIPTS/MPI/5_billboard_features_to_hdf5.py 0 (6 songs)
# sqsub -r 40m -f xeon -q mpi --mpp 2GB -o billboard_to_h5 -n 2 python ./4_SCRIPTS/MPI/5_billboard_features_to_hdf5.py 0 (32 songs)

# ################################################################################
# Script for calculating the frequency of MBIDs (track, album, artist) for each user.
# It saves the resulting data in several HDF5 object that should be merged together
# afterwards with hdf5_dataset_catenator.py
# ################################################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
import GVM_classes
import Features
import tempfile
import h5py
from optparse import OptionParser


rank = MPI.COMM_WORLD.Get_rank() 
size = MPI.COMM_WORLD.Get_size() 
name = MPI.Get_processor_name()
# print "\nHelloworld! I am process {0} of {1} on {2}".format(rank, size, name)


TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]


if __name__ == "__main__":
	usage = "usage: %prog [options] factor"
	opts = OptionParser(usage = usage)
	opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# Factor to overcome the 256 CPUs limitation.
	# Each run must have a different factor
	# E.g., to run the 583 TAR files, it would be 
	# possible to do 256, 256, and 71. However
	# it is more balanced to do 192, 192, and 199; as in 
	# sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

	factor = args[0]
	number_of_songs = 33 #(33 is only number 1 hits)
	rank_lead_zero = "%03d" % (int(factor) * size + ( rank + 1 ) ,)

	# Extracting song_data from the billboard file:
	billboard_file = open(PBS_O_WORKDIR + '/4_SCRIPTS/BILLBOARD/top_200_billboard_2005_2011_mbid_2_no_feat.tsv')
	billboard_lines = billboard_file.readlines()
	songs_data = [x.strip().split('\t') for x in billboard_lines[1:number_of_songs] if x.strip().split('\t')[11] is not '']

	# Printing first line with song metadata first
	for song_data in songs_data:
		if rank == 0:
			# Catches userlogs with no data
			try:
				song_mbid = song_data[11]
				print '{0}'.format('\t'.join(song_data))
				# out_file.close()
			except Exception, e:
				print 'EXC1', e, '\n'


	# HDF5 container destination
	h5_file_path = '/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/BILLBOARD/billboard_{0}.h5'.format(rank_lead_zero)
	# h5_file_path = '/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/BILLBOARD/billboard.h5'
	h = h5py.File(h5_file_path, 'a', driver='core')
	# h = h5py.File('/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/BILLBOARD/billboard.h5', 'a', driver='mpio', comm=MPI.COMM_WORLD)
	h.require_group('songs')


	# Init parameteres for where userfiles are
	# Creates TAR object and extracts its members to TMP folder
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB'
	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print rank, file_list[size * int(factor) + rank]

	# ##########################################################
	# Iterate over all files in a TAR, searching for all songs
	# ##########################################################
	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER):
		user_features = Features.ListeningFeatures(file_in_tar)	# Initializes object for feature extraction 
		
		for song_data in songs_data:	# Iterate over all songs in the list

			# Catches userlogs with no data
			try:
				song_mbid = song_data[11]
				song_name = song_data[1]
				artist_name = song_data[2]
				billboard_dates = [song_data[3], song_data[7], song_data[5]] #debut, peak, exit		
				no_scrobbles, number_scrobbles_per_ranking_zone = user_features.feature_metric_per_ranking_zone(song_mbid, billboard_dates)

			except Exception, e:
				print 'EXC2', file_list[size * int(factor) + rank], file_in_tar, e
				continue

			if no_scrobbles != 0:	
				# Checking if dataset exists, if it was already created and shape doesn't match, it will
				# raise a TypeError which we will catch to extending the dataset.
				try:
					h['songs'].require_dataset(song_mbid, (0, ), ([('lfid', '<i4'), \
																('no_scrobbles', '<i4'), \
																('zones', '<f8', (4,))]), \
																exact=False,  maxshape=(None,))					
					# Writing metadata as dataset attributes
					dset = h['songs'][song_mbid]
					dset.attrs['song_name'] = song_name
					dset.attrs['artist_name'] = artist_name					
					
				except TypeError:
					dset = h['songs'][song_mbid]
				
				# Extending the dataset
				h['songs'][song_mbid].resize((dset.len()+1, ))
					
				# writing the zone data to the dataset
				h['songs'][song_mbid][dset.len()-1] = (user_features.lfid, no_scrobbles, number_scrobbles_per_ranking_zone)			



	# h.close()
	MPI.Finalize()


