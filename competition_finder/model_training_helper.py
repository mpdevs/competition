# coding: utf-8
# __author__: u"Peter"

import pandas as pd
import numpy as np
import math
import os
import tqdm
import random
from scipy.spatial.distance import *
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier

from sklearn.grid_search import RandomizedSearchCV, GridSearchCV
from sklearn.feature_selection import RFECV
from sklearn.cross_validation import train_test_split, cross_val_score
from sklearn.learning_curve import learning_curve
from sklearn.metrics import classification_report, precision_recall_curve, average_precision_score, f1_score, auc
from imblearn.over_sampling import SMOTE


def read_word_vector(path, size=20000):
    os.chdir(path)
    # Word2Vec
    wordID_file = open('wordid')
    wordID = wordID_file.read().split('\n')[0:size]
    wordID = pd.Series(wordID).str.split('\t')  # Row: 的   1
    # 去除多餘index 留下文字
    wordID = list(wordID.str[0])
    wordID_file.close()
    wordID = [word.decode('utf-8') for word in wordID]

    word_vectors_file = open('WordVectors.txt')
    word_vectors = word_vectors_file.read().split('\n')[:size]
    word_vectors_file.close()
    word_vectors = [pd.Series(vec.split('\t')) for vec in word_vectors]
    word_vectors = [x.astype(float) for x in word_vectors]

    return wordID, word_vectors


def get_column_word_vector(columns_list, word_vectors, wordID, word_vector_dict, size=size):
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


def all_average_word_vector(row, column_word_vector_dict, full_column_list, size=size):
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

    vector = vector / len(full_column_list)
    row['word_vec_average_all'] = vector

    return row


def column_average_word_vector(row, column_word_vector_dict, full_column_list, size=size):
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

    for col in full_column_list:
        if col in column_list:
            temp = np.zeros(size)
            i = 0
            for value in row[col].split(',')[:-1]:  # -1: 去除空格
                try:
                    temp += column_word_vector_dict[value]
                    i += 1.
                except:
                    #                     print value#, 'not in word vector'
                    pass

            if i == 0:
                i = 1
            row[col] = temp / i
    return row


def calculate_distance(row, item_word_vector, column, size=size):
    """
    Params:
        item_word_vector: col_avg, connect, all_avg
    """
    # select the item & its word vector
    item1 = item_word_vector[item_word_vector[u'ItemID'].values == row[1]]
    item2 = item_word_vector[item_word_vector[u'ItemID'].values == row[2]]

    # select the word vector of the column and turn Pandas Series into ndarray
    vector1 = item1[column].values[0]
    vector2 = item2[column].values[0]

    # df = row[1:3] # ID pairs
    df = pd.Series()
    cos_name = column + u'_cosine'
    city_name = column + u'_cityblock'
    eu_name = column + u'_euclidean'

    try:
        df[cos_name] = cosine(vector1, vector2)
        df[city_name] = cityblock(vector1, vector2)
        df[eu_name] = euclidean(vector1, vector2)
        df[column + u'_chebyshev'] = chebyshev(vector1, vector2)
        df[column + u'_correlation'] = correlation(vector1, vector2)
        df[column + u'_canberra'] = canberra(vector1, vector2)

    except:
        df[cos_name] = np.nan
        df[city_name] = np.nan
        #         df[eu_name] = np.nan
        #       df[col + '_cosine'] = np.nan
        #       df[col + '_cityblock'] = np.nan
        #       df[col + '_euclidean'] = np.nan
        pass

    return df


def generate_distance_df(category, attr, dummy, data_set, column_word_vector_dict, is_training_set=True):
    """
    生成distance metric

    Params:
        category: 品類, int
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
    print word_vec
    for col in word_vec:
        if sum(word_vec[col].notnull()) / float(len(word_vec)) < 0.01:
            word_vec.drop(col, inplace=True, axis=1)

    distance = pd.DataFrame()
    for col in tqdm.tqdm(word_vec.columns[1:]):
        distance = pd.concat(
            [distance, data_set.apply(calculate_distance, item_word_vector=word_vec, column=col, axis=1)], axis=1)

    distance = pd.concat([data_set.ix[:, 1:3], distance], axis=1)  # Insert ItemID pair

    if is_training_set:
        distance = pd.concat([distance, data_set.ix[:, -1]], axis=1)  # Append Label

    return distance


if __name__ == u"__main__":
    path = 'D:\workspace\preprocess\competition_finder\data_with_ID'
    wordID, word_vectors = read_word_vector(path, size=20000)

