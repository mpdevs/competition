# coding: utf-8

import pandas as pd
import numpy as np
import math
import os
import tqdm
import random
from scipy.spatial.distance import *
import matplotlib.pyplot as plt
import seaborn as sns
import time

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier

from sklearn.grid_search import RandomizedSearchCV, GridSearchCV
from sklearn.feature_selection import RFECV
from sklearn.cross_validation import train_test_split, cross_val_score
from sklearn.learning_curve import learning_curve
from sklearn.metrics import classification_report, precision_recall_curve, average_precision_score
from sklearn.metrics import f1_score, auc, recall_score, precision_score, r2_score, mean_squared_error, \
    mean_absolute_error

mingw_path = 'C:\\Program Files\\mingw-w64\\x86_64-6.2.0-posix-seh-rt_v5-rev1\\mingw64\\bin'
os.environ['PATH'] = mingw_path + ';' + os.environ['PATH']
import xgboost as xgb


category_dict = {162116: '蕾絲', 1623: '半身裙', 121412004: '背心吊帶', 162104: '村杉',
                 50000671: 'T恤', 162103: '毛衣', 50008901: '風衣', 50011277: '短外套'}


# Feature Extraction
def create_dummy_attr(file_name_prefix, category):
    print file_name_prefix[:-1]
    # need enoding so that we can compare it with unicode(材质成分)
    attr = pd.read_csv(file_name_prefix + str(category) + '.csv', encoding='utf-8-sig')
    # 用attr.dorp會出現error
    attr = attr.ix[:, attr.columns != u'材质成分']
    attr = attr.ix[:, attr.columns != u'品牌']

    dummy = pd.DataFrame()
    for col in tqdm.tqdm(attr.columns[2:]):
        if sum(attr[col].notnull()) / float(
                len(attr[col])) >= 0.01:  # skip if over 99% of the content in the column are NAN
            #  turn col into dummy variables and ignore NAN
            dummy = pd.concat([dummy, attr[col].str.get_dummies(sep=',')], axis=1)
        else:
            print 'Skipped', col
    dummy = pd.concat([attr.iloc[:, 1], dummy], axis=1)
    dummy.to_csv(file_name_prefix + str(category) + '_dummy.csv', encoding='utf-8-sig')

    return dummy
# e.g. create_dummy_attr('attr_train_full_', category)


def get_column_word_vector(columns_list, word_vectors, wordID, word_vector_dict, size=100):
    """
    遍例欄名,取出相應word vector,若無則賦值0向量,可保證之後index時不出error
    """
    mew_word_vector_dict = word_vector_dict.copy()
    for i, col in enumerate(columns_list):

        if col not in word_vector_dict.keys():
            try:
                mew_word_vector_dict[col] = list(word_vectors[wordID.index(col)])
            except:
                mew_word_vector_dict[col] = np.zeros(size)
                pass
    return mew_word_vector_dict


def all_average_word_vector(row, column_word_vector_dict, full_column_list, size=100):
    """
    取所有单字的word vector平均

    Params:
        row: attributes of an item, Pandas Series
        column_word_vector_dict: the dictionary stores the corresponding word vector of words that appear in columns
        column_list: list of words in columns

    Return:
        row: row with averaged word vectors appended
    """
    # Double Check whether we skipped the ItemID in column_list or not
    if len(full_column_list) == len(row):
        return 'Use df.columns[1:] to skip ItemID'

    # filter the cols that has 0 value, exclude ItemID
    full_column_list = full_column_list[row[2:] != 0]

    vector = np.zeros(size)
    for col in full_column_list:
        vector += column_word_vector_dict[col]

    vector /= len(full_column_list)
    row['word_vec_average_all'] = vector
    return row


def column_average_word_vector(row, column_word_vector_dict, full_column_list, size=100):
    """
    取每格内所有单字的word vector平均

    Params:
        row: attribute of an item, Pandas Series
        column_word_vector_dict: the dictionary stores the corresponding word vector of words that appear in columns
        column_list: list of words in columns

    Return:
        row: row with averaged word vectors
    """
    # Double Check whether we skipped the ItemID in column_list or not
    if len(full_column_list) == len(row):
        return 'Use df.columns[1:] to skip ItemID'
    # filter the cols that equak NaN, exclude ItemID & itemDesc
    column_list = full_column_list[row[2:].notnull() == 1]

    vector = []
    for col in full_column_list:
        if col in column_list:
            temp = np.zeros(size)
            i = 0
            for value in row[col].split(',')[:-1]:  # -1: 去除空格
                try:
                    temp += column_word_vector_dict[value]
                    i += 1.
                except:
                    # print value#, 'not in word vector'
                    pass

            if i == 0:
                i = 1
            row[col] = temp / i
    return row


def calculate_distance(row, item_word_vector, column):
    """
    Params:
        item_word_vector: col_avg, connect, all_avg
    """
    # select the item & its word vector
    item1 = item_word_vector[item_word_vector[u'ItemID'].values == row[1]][:1]
    item2 = item_word_vector[item_word_vector[u'ItemID'].values == row[2]][:1]



    try:
        vector1 = item1[column].values[0]
        vector2 = item2[column].values[0]
    except:
        print column

    df = pd.Series()
    try:
        df[column + u'_cosine'] = cosine(vector1, vector2)
        df[column + u'_cityblock'] = cityblock(vector1, vector2)
        df[column + u'_euclidean'] = euclidean(vector1, vector2)
        df[column + u'_chebyshev'] = chebyshev(vector1, vector2)
        df[column + u'_canberra'] = canberra(vector1, vector2)
        df[column + u'_braycurtis'] = braycurtis(vector1, vector2)

    except:
        df[column + u'_cosine'] = np.nan
        df[column + u'_cityblock'] = np.nan
        df[column + u'_euclidean'] = np.nan
        df[column + u'_chebyshev'] = np.nan
        df[column + u'_canberra'] = np.nan
        df[column + u'_braycurtis'] = np.nan
        pass

    return df


def generate_distance_df(attr, dummy, data_set, column_word_vector_dict, is_training_set=True):
    """
    生成distance metric

    Params:
        attr: attr_train/test_full/tagged, DataFrame
        dummy: attr_train/test_full/tagged_dummy, DataFrame
        data_set: train/test_categoryID, , DataFrame
        column_word_vector_dict: 儲存所有features的 word vector, dict
        is_training_set: if it is training data, append label, boolean
    Return:

    """

    column_word_vector_dict = get_column_word_vector(dummy.columns[1:], word_vectors, wordID, column_word_vector_dict)
    word_vec = pd.DataFrame(dummy.ix[:, 1])  # initiate with itemID

    col_name = 'column_avg'
    word_vec[col_name] = dummy.apply(
        all_average_word_vector, column_word_vector_dict=column_word_vector_dict, full_column_list=dummy.columns[2:],
        axis=1).ix[:, -1]

    word_vec = pd.concat([word_vec, attr.apply(
        column_average_word_vector, column_word_vector_dict=column_word_vector_dict, full_column_list=attr.columns[2:],
        axis=1).ix[:, 2:]], axis=1)

    for ind, i in enumerate(word_vec.values):
        if i[0] == 35263287096:
            print i
        if ind == 3960:
            print i
        continue
    #     print word_vec
    for col in word_vec:
        if sum(word_vec[col].notnull()) / float(len(word_vec)) < 0.01:
            word_vec.drop(col, inplace=True, axis=1)

    distance = pd.DataFrame()
    for col in tqdm.tqdm(word_vec.columns[1:]):
        distance = pd.concat(
            [distance, data_set.apply(calculate_distance, item_word_vector=word_vec, column=col, axis=1)], axis=1)

    distance = pd.concat([data_set.ix[:, 1:3], distance], axis=1)  # Insert ItemID pair

    # Skip columns without valid values
    for col in distance.columns:
        if sum(distance[col].notnull()) == 0 or sum(distance[col]) == 0:
            distance.drop(col, inplace=True, axis=1)

    if is_training_set:
        distance = pd.concat([distance, data_set.ix[:, -1]], axis=1)  # Append Label

    return distance


def read_csv_data(is_training_set, is_full, category):
    if is_training_set:
        attr_name = 'attr_train_'
        data = pd.read_csv('train_' + str(category) + '.csv', encoding='utf8')
    else:
        attr_name = 'attr_test_'
        data = pd.read_csv('test_' + str(category) + '.csv', encoding='utf8')
        data['Label'] = 0.0

    if is_full:
        attr_name += 'full_'
    else:
        attr_name += 'tagged_'

    attr = pd.read_csv(attr_name + str(category) + '.csv', encoding='utf-8-sig')
    dummy = pd.read_csv(attr_name + str(category) + '_dummy.csv', encoding='utf-8-sig')

    return attr, dummy, data


# model fitting part
def separate_positive_negative(train, threshold=0.5):
    """
    classification: thresold=0.5
    regression: threhold= 0
    """
    train_positive = train[train['Label'] > threshold]
    train_negative = train[train['Label'] <= threshold]
    return train_positive, train_negative


# down sample test set to use as negative training set
# Size of the test set= ratio* size of train set
def down_sample_testset(train_positive, test, ratio, is_random=False):
    if is_random:
        print 'Random Under Sampling'
    else:
        np.random.seed(2016)
    random_index = np.random.randint(0, len(test), ratio * len(train_positive))
    sampled_test = test.iloc[random_index, :]
    sampled_test = sampled_test.reset_index(drop=True)

    return sampled_test


def generate_model_input(train_positive, sampled_test, train_distance_positive, test_distance, input_type=2):
    if input_type == 1:  # BOW
        data = train_positive.iloc[:, 1:].append(sampled_test.iloc[:, 1:], ignore_index=True)
    elif input_type == 2:  # word vector distance
        data = train_distance_positive.append(test_distance, ignore_index=True)
    else:  # BOW + word vector
        data = train_positive.iloc[:, 1:-1].append(sampled_test.iloc[:, 1:-1], ignore_index=True)  # skip labels first
        word_vec = train_distance_positive.append(test_distance, ignore_index=True).iloc[:,
                   2:]  # skip customer & competitor ID
        data = pd.concat([data, word_vec], axis=1)

    data = data.fillna(0)

    X = data.drop(['Label', 'ID_competitor', 'ID_customer', 'Unnamed: 0'], axis=1)  # skipped ID, label
    y = data['Label']
    return X, y


def select_features_rfe(X, y, estimator, step=1, cv=5, scoring=None, verbose=True):
    # The "accuracy" scoring is proportional to the number of correct
    # classifications
    rfecv = RFECV(estimator, step, cv, scoring)
    rfecv.fit(X, y)

    if verbose:
        print("Optimal number of features : %d" % rfecv.n_features_)
        if scoring == None:
            scoring = 'CV score of default metric:'
        print 'Optimal {0}: {1}'.format(scoring, np.max(rfecv.grid_scores_))
        # Plot number of features VS. cross-validation scores
        plt.figure()
        plt.xlabel("Number of features selected")
        plt.ylabel("Cross validation score (nb of correct classifications)")
        plt.plot(range(1, len(rfecv.grid_scores_) + 1), rfecv.grid_scores_)
        plt.show()

    X_transform = rfecv.transform(X)

    return X_transform, rfecv.support_


# SMOTE:  2組 test sets
def clf_model_evaluation(model, X_train, y_train, X_test, y_test, iteration=3, verbose=True):
    f_list, p_list, r_list = [], [], []

    for i in range(iteration):
        model.fit(X_train, y_train)
        f_list.append(f1_score(y_test, model.predict(X_test)))
        p_list.append(precision_score(y_test, model.predict(X_test)))
        r_list.append(recall_score(y_test, model.predict(X_test)))

    f1 = np.mean(f_list)
    precision = np.mean(p_list)
    recall = np.mean(r_list)

    if verbose:
        # Learning Curve
        print 'Learning Curve on training set'
        train_sizes, train_scores, test_scores = learning_curve(model, X_train, y_train, cv=10, n_jobs=-1,
                                                                train_sizes=[0.2, 0.4, 0.6, 0.8, 1])

        train_scores_mean = np.mean(train_scores, axis=1)
        train_scores_std = np.std(train_scores, axis=1)
        test_scores_mean = np.mean(test_scores, axis=1)
        test_scores_std = np.std(test_scores, axis=1)

        plt.plot(train_sizes, train_scores_mean, 'o-', color="r",
                 label="Training score")
        plt.plot(train_sizes, test_scores_mean, 'o-', color="g",
                 label="Cross-validation score")
        plt.fill_between(train_sizes, train_scores_mean - train_scores_std,
                         train_scores_mean + train_scores_std, alpha=0.1,
                         color="r")
        plt.fill_between(train_sizes, test_scores_mean - test_scores_std,
                         test_scores_mean + test_scores_std, alpha=0.1, color="g")

        plt.legend(loc="best")
        plt.xlabel('Training Size')
        plt.ylabel('Scores')
        plt.show()
    return precision, recall, f1


def tune_model(estimator, X_train, y_train, param, cv=5, scoring=None, verbose=True):
    gridCV_model = GridSearchCV(estimator, param, cv=cv, n_jobs=-1, scoring=scoring)
    gridCV_model.fit(X_train, y_train)

    if verbose:
        print 'Best Training Score:', gridCV_model.best_score_
        print 'Param of the best estimator:', gridCV_model.best_params_, '\n'

    return gridCV_model.best_estimator_, gridCV_model.best_score_


def get_prediction_for_tagging(category, num_pos, num_neg):
    with open('cuo.txt', 'r') as f:
        cuo = pd.Series(f.readlines())
        cuo = [x[:-1] for x in cuo]

    # visualization
    temp_pred = pd.read_csv('prediction_proba_' + str(category) + '.csv', encoding='utf8')
    temp_pred = temp_pred.drop('Unnamed: 0', axis=1)
    temp_pred = temp_pred.median(axis=1)
    print plt.hist(temp_pred)
    plt.show()

    prediction_set = pd.read_csv('prediction_' + str(category) + '.csv', encoding='utf8')
    txt = pd.concat([prediction_set.iloc[:, 1:3], pd.Series(temp_pred, name='median')], axis=1)
    txt.sort_values('median', inplace=True, ascending=False)

    print txt.shape
    print txt.head()

    # write to csv
    count_pos = 1
    count_neg = 1
    txt_name = str(category) + '_proba_median.txt'
    with open(txt_name, 'w') as text_file:
        for row in tqdm.tqdm(range(10000)):
            # pos
            if str(long(txt.iloc[row, 0])) in cuo or str(long(txt.iloc[row, 1])) in cuo or count_pos > num_pos:
                continue
            else:
                if str(long(txt.iloc[row, 0])) == str(long(txt.iloc[row, 1])):
                    print str(long(txt.iloc[row, 0])), str(long(txt.iloc[row, 1]))
                else:
                    text_file.write(
                        '{0}	{1}	{2}\n'.format(str(long(txt.iloc[row, 0])), str(long(txt.iloc[row, 1])),
                                                     txt.iloc[row, 2]))
                    count_pos += 1
        for row in tqdm.tqdm(range(1, 10000)):
            # neg
            if (str(long(txt.iloc[-row, 0])) in cuo or str(long(txt.iloc[-row, 1])) in cuo) or count_neg > num_neg:
                continue
                print count_neg
            else:
                text_file.write('{0}	{1}	{2}\n'.format(str(long(txt.iloc[-row, 0])), str(long(txt.iloc[-row, 1])),
                                                             txt.iloc[-row, 2]))
                count_neg += 1
    print '____________________________________________________________________________________________________'
    return None


def get_photo_id(category):
    print category
    data = pd.read_csv(str(category) + '_proba_median' + '.txt', sep='\t')

    d = 'photo_' + str(category) + '.txt'

    for f, txt in zip([d], [data]):
        with open(f, 'w') as text_file:
            for row in range(len(txt)):
                text_file.write('{0}\n'.format(str(long(txt.iloc[row, 0]))))
                text_file.write('{0}\n'.format(str(long(txt.iloc[row, 1]))))
    return


def split_tagging_set(category, num_of_files=2, len_per_file=200):
    txt_name = str(category) + '_proba_median.txt'

    with open(txt_name, 'r') as txt_file:
        data = txt_file.readlines()
    np.random.shuffle(data)

    txt_name = str(category) + '_proba_median_shuffle.txt'
    with open(txt_name, 'w') as txt_file:
        for i in data:
            txt_file.write(i)

    for i in range(1, num_of_files + 1):
        output = str(category) + '_' + str(i) + '.txt'

        with open(output, 'w') as txt_file:
            start = len_per_file * (i - 1)
            end = len_per_file * i
            for i in data[start:end]:
                txt_file.write(i)

    return None


def get_train_distance(category):
    print category_dict[category], category
    column_word_vector_dict = {}

    # train
    print 'Train'
    start = time.time()

    attr, dummy, train = read_csv_data(is_training_set=True, is_full=True, category=category)
    train_distance = generate_distance_df(category, attr, dummy, train, column_word_vector_dict, is_training_set=True)
    train_distance.to_csv('train_distance_' + str(category) + '.csv', encoding='utf8')

    threshold = 0.5
    train_positive, train_negative = separate_positive_negative(train, threshold)
    print 'Positive training sample size:', len(train_positive), '\n'
    end = time.time()
    print end - start


def get_prediction_distance(category):
    print category_dict[category], category
    column_word_vector_dict = {}
    print 'Prediction'
    start = time.time()
    prediction_set = pd.read_csv('prediction_' + str(category) + '.csv', encoding='utf8')
    attr, dummy, train = read_csv_data(is_training_set=True, is_full=True, category=category)

    # exclude those appeared in training set
    #   construct training ID dict that stores all the ID pairs
    train_id_dict = {}
    train_id_list = list()
    for i in range(len(train)):
        try:
            # save customer id as key and competitor id as value
            if train_id_dict[train.iloc[i, 1]]:
                train_id_dict[train.iloc[i, 1]] = train_id_dict[train.iloc[i, 1]].append(train.iloc[i, 2])
        except:
            train_id_dict[train.iloc[i, 1]] = [train.iloc[i, 2]]
        try:
            # save competitor id as key and customer id as value
            if train_id_dict[train.iloc[i, 2]]:
                train_id_dict[train.iloc[i, 2]] = train_id_dict[train.iloc[i, 2]].append(train.iloc[i, 1])
        except:
            train_id_dict[train.iloc[i, 2]] = [train.iloc[i, 1]]

        train_id_list.append(train.iloc[i, 1])
        train_id_list.append(train.iloc[i, 2])
    train_id_list = set(train_id_list)

    # filter prediction set
    for i in range(len(prediction_set)):
        try:
            customer_id = prediction_set.iloc[i, 1]
            competitor_id = prediction_set.iloc[i, 2]
            if customer_id in train_id_list:
                try:
                    if competitor_id in train_id_dict[customer_id]:
                        prediction_set.drop(prediction_set.index[[i]], inplace=True)
                except:
                    pass

            if competitor_id in train_id_list:
                try:
                    if customer_id in train_id_dict[competitor_id]:
                        prediction_set.drop(prediction_set.index[[i]], inplace=True)
                except:
                    pass
        except:
            print i

    # construct prediction features
    attr_name = 'attr_prediction_full_'
    attr = pd.read_csv(attr_name + str(category) + '.csv', encoding='utf-8-sig')
    dummy = pd.read_csv(attr_name + str(category) + '_dummy.csv', encoding='utf-8-sig')

    if len(prediction_set) > 400000:
        random_index = set(np.random.randint(0, len(prediction_set), 400000))
        sampled_prediction = prediction_set.iloc[list(random_index), :]
        sampled_prediction = sampled_prediction.reset_index(drop=True)
    else:
        sampled_prediction = prediction_set.copy()
    sampled_prediction.to_csv("sampled_prediction_" + str(category) + '.csv', encoding='utf8')
    print "Length of sample test", len(sampled_prediction)

    prediction_distance = generate_distance_df(attr, dummy, sampled_prediction, column_word_vector_dict, is_training_set=False)
    #     prediction_distance = pd.read_csv("prediction_distance_" + str(category)+ '.csv', encoding='utf8')
    print 'Length of prediction set:', len(prediction_distance)

    prediction_distance.to_csv('prediction_distance_' + str(category) + '.csv', encoding='utf8')
    end = time.time()
    print end - start
    print '___________________________________________________________________________________________________________________'


def easyensemble_prediction(category):
    model_dict = {'LR': {'model': LogisticRegression()},
                  'GBDT': {'model': GradientBoostingClassifier()},
                  'KNN': {'model': KNeighborsClassifier()},
                  'RF': {'model': RandomForestClassifier()},
                  'NB': {'model': GaussianNB()},
                  'Ada': {'model': AdaBoostClassifier()},
                  'SVM': {'model': SVC()},
                  'XGB': {'model': xgb.sklearn.XGBClassifier()}}
    RATIO = 1.0
    is_random = True
    column_word_vector_dict = {}
    input_type = 3

    print category_dict[category], category
    start = time.time()
    prediction_proba_dict, prediction_dict = {}, {}

    # Construct training distance metrics
    attr, dummy, train = read_csv_data(is_training_set=True, is_full=True, category=category)
    train_distance = pd.read_csv('train_distance_' + str(category) + '.csv', encoding='utf8')

    # Define problem type
    threshold = 0.5
    train_distance_positive, train_distance_negative = separate_positive_negative(train_distance, threshold)
    train_positive, train_negative = separate_positive_negative(train, threshold)
    print 'Positive training sample size:', len(train_positive), '\n'
    train_positive['Label'] = 1.0
    train_distance_positive['Label'] = 1.0

    # construct prediction features
    prediction_set = pd.read_csv('prediction_' + str(category) + '.csv', encoding='utf8')
    prediction = pd.DataFrame()
    prediction_proba = pd.DataFrame()

    prediction_distance = pd.read_csv('prediction_distance_' + str(category) + '.csv', encoding='utf8')
    print 'Length of prediction set:', len(prediction_distance)
    prediction_full_features = pd.concat([prediction_set.iloc[:, 3:], prediction_distance.iloc[:, 2:]], axis=1)
    prediction_full_features.fillna(0, inplace=True)
    try:
        print prediction_full_features['Label']
    except:
        print 'Good, label is not included'

    # Save the RAM
    prediction_distance = 0
    prediction_set = 0

    attr, dummy, test = read_csv_data(is_training_set=False, is_full=True, category=category)

    num_iterations = 15
    #     num_adding_fp = num_iterations / 3
    for iteration in range(num_iterations):
        print 'Iteration:', iteration

        print 'Random under sampling'
        # Construct testing distance mectrics
        #         attr, dummy, test = read_csv_data(is_training_set=False, is_full=True, category=category)
        sampled_test = down_sample_testset(train_distance_positive, test, RATIO, is_random)
        #         test = 0 # save ram
        test_distance = generate_distance_df(category, attr, dummy, sampled_test, column_word_vector_dict,
                                             is_training_set=False)
        print 'Negative training sample size:', len(test_distance), '\n'

        # Construct features
        X, y = generate_model_input(
            train_positive, sampled_test, train_distance_positive, test_distance, input_type=input_type, random_state=2015)

        prediction_full_features_copy = prediction_full_features.copy()
        for i in X.columns:
            if i not in prediction_full_features.columns:
                print 'Drop:', i
                X.drop(i, axis=1, inplace=True)
        prediction_full_features_copy = prediction_full_features_copy.loc[:, X.columns]
        prediction_full_features_copy.columns = X.columns

        for key in model_dict.keys():
            model = model_dict[key]['model']

            # Predict
            model.fit(X, y)
            temp = prediction_full_features_copy
            temp.columns = X.columns
            p = pd.Series(model.predict(temp))
            try:
                # keep the probability for class == 1
                prob = pd.Series(model.predict_proba(temp)[:, 1])
            except:
                print key, 'does not have predict proba'
                prob = pd.Series(model.predict(temp))

            prediction = pd.concat([prediction, p], axis=1)
            prediction_proba = pd.concat([prediction_proba, prob], axis=1)

            # Print Feature Importance
            if key == 'XGB' and iteration == 0:
                features_importance = pd.Series(model.feature_importances_, index=X.columns.values)
                print features_importance.sort_values(ascending=False) * 100

        prediction_proba_dict[category] = prediction_proba
        prediction_dict[category] = prediction

        print '----------------------------------------------------------------------------------------------------'

    prediction_proba_dict[category].to_csv('prediction_proba_' + str(category) + '.csv', encoding='utf8')
    print 'Done'

    end = time.time()
    print 'Time:', end - start

