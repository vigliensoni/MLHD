#!/home/vigliens/python/bin/python

# sqsub -r 1h -f xeon -q mpi --mpp 8GB -o 23_graphlab -n 8 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/23_graphlab_mini_example.py


# for ((i=0; i<=99;i++)); do echo `sqsub -r 4h -f xeon -q threaded --mpp 8GB -o 27_graphlab_0_1_factor_comparison_1e07_$i -n 4 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/27_graphlab_0_1_factor_comparison_1e_07.py $i`; done

# 00 [(1e-07, 5, ['usid']),
# 01 (1e-07, 10, ['usid']),
# 02 (1e-07, 15, ['usid']),
# ...
# 97 (1e-07, 490, ['usid']),
# 98 (1e-07, 495, ['usid']),
# 99 (1e-07, 500, ['usid'])]

# ##################################################
# This scripts should
# 1. Run full demographics features experiment in 10% of the data
# with a .9/.1 split for train/testing
# ##################################################


from mpi4py import MPI
import os
from optparse import OptionParser
# import tempfile
import graphlab as gl
import GraphlabHelpers as GH
import itertools
import GVM_classes


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
print "SIZE:{0}, RANK:{1}".format(size, rank)

# TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens')  # ComputeCanada
OUT_FOLDER = '/scratch/vigliens/out_factors_1e07/'  # ComputeCanada
# OUT_FOLDER = '/scratch/vigliens/out/'  # COPLAND
# print 'TEMP_FOLDER: ', TEMP_FOLDER
# TEMP_FOLDER = tempfile.mkdtemp(dir='/Users/gabriel/scratch/') # Local

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
try:
    SQ_JOBID = os.environ["SQ_JOBID"]
except:
    SQ_JOBID = '_STARR'

print 'SQ_JOBID:{0}'.format(SQ_JOBID)


def init_parameters():
    """
    Returns a list with all combinations of init parameters in the form

    [regularization_val, num_factor, side_data_combination]

    [1e-05, 50, ['usid', 'age']]

    """
    # 6 features generate 32 combinations
    # No side-data features
    side_data_combinations = GVM_classes.permutations([])
    # side_data_combinations = GVM_classes.permutations(['age',
    #                                                    'gender',
    #                                                    'country',
    #                                                    'mainstreamness_artist',
    #                                                    'exploratoryness_artist'])
    # adding the user ID only
    side_data_combinations = GVM_classes.listprepender(side_data_combinations, field='usid')
    side_data_combinations.insert(0, ['usid'])

    # num_factors = [5 * (x + 1) for x in range(100)]  #5, 10, ..., 500  #5, 10, ..., 500
    # num_factors = [(x + 30) for x in range(11)]  # 30, 31, 32, ..., 40 # 1e-06
    num_factors = [(x + 11) for x in range(15)]  # 10, 11, 12, ..., 30 #1e-07
    regularization_vals = [1e-07]

    parameters = [regularization_vals, num_factors, side_data_combinations]
    parameter_combination = list(itertools.product(*parameters))  #combinatory of all parameters
    return parameter_combination


# def observed_data_loader(path='/scratch/vigliens/9_GL_MODELS/0_1/full'):
#     """
#     """
#     observed_data = gl.SFrame(path)
#     full_train, full_test = GH.datasetsplit(observed_data, split=0.9)
#     return full_train, full_test


def model_training(observation_data,
                   num_factors,
                   regularization_vals,
                   side_data_factorization,
                   user_data,
                   solver):
    """
    Create a factorization recommender.

    Returns a model
    """
    model = gl.factorization_recommender.create(
        observation_data=observation_data,
        user_id='usid',
        item_id='mbid',
        target='rating',
        user_data=user_data,
        side_data_factorization=side_data_factorization,
        max_iterations=50,
        num_factors=num_factors,
        regularization=regularization_vals,
        solver=solver,
        sgd_trial_sample_proportion=0.25)
    return model


def storing_results(out_folder, file_no, regularization_vals, num_factors, user_side_data_par, train, test, time):
    """
    """
    with open(out_folder + str(file_no), 'w') as out_file:
        out_file.write('\t'.join([str(train),
                                 str(test),
                                 str(regularization_vals),
                                 str(num_factors),
                                 str(int(time)),
                                 str('.'.join(user_side_data_par))
                                 ]))
        out_file.write('\n')

if __name__ == "__main__":
    usage = "usage: %prog [options] factor"
    opts = OptionParser(usage=usage)
    # # opts.add_option('-f', '--hdf5', dest='h5')
    options, args = opts.parse_args()
    i = int(args[0])
    # data_local = "/Users/gabriel/Documents/5_DATA/FREQUENCY_LIKERT/RATINGS_BY_TIME_BRACKETS/0_01/full_filtered"

    # 1. INITIALIZATION PARAMETERS
    par_combination = init_parameters()

    file_no = i * size + rank
    regularization_vals = par_combination[file_no][0]
    num_factors = par_combination[file_no][1]
    user_side_data_par = par_combination[file_no][2]

    print ",".join([str(file_no), str(regularization_vals), str(num_factors), str(user_side_data_par)])

    # 2. LOADING DATA. TRAINING AND TESTING SETS SHOULDNT BE FIXED????
    # observation data
    # Local data
    # full_train = gl.SFrame('/Users/gabriel/Documents/5_DATA/FREQUENCY_LIKERT/RATINGS_BY_TIME_BRACKETS/0_01/full_filtered_train_0_9/')
    # full_test = gl.SFrame('/Users/gabriel/Documents/5_DATA/FREQUENCY_LIKERT/RATINGS_BY_TIME_BRACKETS/0_01/full_filtered_test_0_1')
    # CC 0_01 data
    # full_train = gl.SFrame('/scratch/vigliens/9_GL_MODELS/0_01/full_train')
    # full_test = gl.SFrame('/scratch/vigliens/9_GL_MODELS/0_01/full_test')
    
    # CC 0_1 data
    full_train = gl.SFrame('/scratch/vigliens/9_GL_MODELS/0_1/full_train')
    full_test = gl.SFrame('/scratch/vigliens/9_GL_MODELS/0_1/full_test')
    

    # userside data
    usd = gl.SFrame('/scratch/vigliens/9_GL_MODELS/METADATA/metadata_and_three_features_preprocessed/')  #CC
    # usd = gl.SFrame('/Users/gabriel/Documents/5_DATA/FREQUENCY_LIKERT/DEMOGRAPHICS/metadata_and_features')  #LOCAL
    usdf = usd.select_columns(user_side_data_par)

    for i in range(10):  # number of created models
        model = model_training(observation_data=full_train,
                               num_factors=num_factors,
                               regularization_vals=regularization_vals,
                               side_data_factorization=True,
                               user_data=usdf,
                               solver='adagrad')

        # compare model with test test
        fact, reg, train, test = GH.model_comparison_pandas(model, full_test)

    # gl.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 2)

        storing_results(OUT_FOLDER, '-'.join([format(file_no, '03'), str(i)]), regularization_vals, num_factors, user_side_data_par, train, test, model.training_time)











