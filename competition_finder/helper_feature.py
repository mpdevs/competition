# coding: utf-8

import pandas as pd
import numpy as np
import tqdm
from scipy.spatial.distance import *
from common.common_helper import *
from helper import *
from settings import *


SIZE = 100
def get_word_vector(SIZE=100):
    """
    讀取詞向量
    :param SIZE: word vector 向量長度
    :return: wordID: list, 單字
    :return: word_vectors: list, list of vectors that correspond to each word in wordID
    """
    # Get word vectors
    # Word2Vec
    wordID_file = open('wordid')
    wordID = wordID_file.read().split('\n')[0:100000]
    wordID = pd.Series(wordID).str.split('\t')
    wordID = list(wordID.str[0])
    wordID_file.close()
    wordID = [word.decode('utf-8') for word in wordID]

    word_vectors_file = open('WordVectors.txt')
    word_vectors = word_vectors_file.read().split('\n')[:100000]
    word_vectors_file.close()
    word_vectors = [pd.Series(vec.split('\t')) for vec in word_vectors]
    word_vectors = [x.astype(float) for x in word_vectors]

    return wordID, word_vectors
# Feature Extraction


def create_dummy_attr(file_name_prefix, category):
    """
    生成維度值的亞變量
    :param file_name_prefix: str, eg: attr_train_full
    :param category: int
    :return: DataFrame
    """
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

    export_csv(dummy, file_name_prefix + str(category) + '_dummy.csv')
    return dummy
# e.g. create_dummy_attr('attr_train_full_', category)

@time_elapse
def get_column_word_vector(columns_list, word_vectors, wordID, word_vector_dict, size):
    """
    遍例欄名(即維度值),取出相應word vector,若無則賦值0向量,可保證之後index時不出error
    :param columns_list: list, 欄名
    :param word_vectors: list
    :param wordID: list
    :param word_vector_dict: dict
    :param size: int
    :return:
    """
    new_word_vector_dict = word_vector_dict.copy()
    for i, col in enumerate(columns_list):
        if col not in word_vector_dict.keys():
            try:
                new_word_vector_dict[col] = list(word_vectors[wordID.index(col)])
            except:
                new_word_vector_dict[col] = np.zeros(size)
                pass
    return new_word_vector_dict

@time_elapse
def all_average_word_vector(row, column_word_vector_dict, full_column_list, size):
    """
    取所有欄位中所有单字的word vector平均

    :param row: row of the DataFrame (dummy)
    :param column_word_vector_dict: dict, key為分詞 value為對應之詞向量
    :param full_column_list: list, 欄名列表
    :param size: int, 詞向量長度
    :return: 新增一個欄位的row
    """
    # Double Check whether we skipped the ItemID in column_list or not
    if len(full_column_list) == len(row):
        return 'Use df.columns[1:] to skip ItemID'

    # filter the cols that has 0 value, exclude ItemID
    full_column_list = full_column_list[row[2:] != 0]

    vector = np.zeros(size)
    # 將所有維度值的詞向量加起來
    for col in full_column_list:
        vector += column_word_vector_dict[col]

    # 平均
    vector /= len(full_column_list)
    row['word_vec_average_all'] = vector
    return row

@time_elapse
def column_average_word_vector(row, column_word_vector_dict, full_column_list, size):
    """
    分別取每格内所有单字的word vector平均
    :param row: row of the DataFrame (attr)
    :param column_word_vector_dict: dict, key為維度值 value為對應之詞向量
    :param full_column_list: list, 欄名(維度值)列表
    :param size: int, 詞向量長度
    :return: 包含新features的row
    """

    # Double Check whether we skipped the ItemID in column_list or not
    if len(full_column_list) == len(row):
        return 'Use df.columns[1:] to skip ItemID'
    # filter the cols that equal NaN, exclude ItemID & itemDesc
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
                    pass
            if i == 0:
                i = 1
            row[col] = temp / i
    return row

@time_elapse
def calculate_distance(row, item_word_vector, column):
    """
    計算一個指定維度的距離
    :param row: row of the DataFrame, 即一件商品的特徵向量
    :param item_word_vector:
    :param column: 要計算距離的維度
    :return:
    """
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

@time_elapse
def generate_distance_df(attr, dummy, data_set, column_word_vector_dict, word_vectors, wordID, size, is_training_set=True):
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
    # 引入word vector
    column_word_vector_dict = get_column_word_vector(dummy.columns[1:], word_vectors, wordID, column_word_vector_dict, size)
    word_vec = pd.DataFrame(dummy.ix[:, 1])  # initiate with itemID

    col_name = 'all_avg'
    word_vec[col_name] = dummy.apply(
        all_average_word_vector, column_word_vector_dict=column_word_vector_dict, full_column_list=dummy.columns[2:],
        size=size, axis=1).ix[:, -1]

    word_vec = pd.concat([word_vec, attr.apply(
        column_average_word_vector, column_word_vector_dict=column_word_vector_dict, full_column_list=attr.columns[2:],
        size=size, axis=1).ix[:, 2:]], axis=1)

    # 去除 nan(超過99%) 太多的維度
    for col in word_vec:
        threshold = 0.01
        if sum(word_vec[col].notnull()) / float(len(word_vec)) < threshold:
            word_vec.drop(col, inplace=True, axis=1)

    # 計算距離
    distance = pd.DataFrame()
    for col in tqdm.tqdm(word_vec.columns[1:]):
        distance = pd.concat(
            [distance, data_set.apply(calculate_distance, item_word_vector=word_vec, column=col, axis=1)], axis=1)

    distance = pd.concat([data_set.ix[:, 1:3], distance], axis=1)  # Insert ItemID pair

    # 去除全為 nan 或 0 的維度
    for col in distance.columns:
        # if sum(distance[col].notnull()) == 0 or sum(distance[col]) < 0.1:
        #     distance.drop(col, inplace=True, axis=1)

        if sum(distance[col].notnull()) == 0:
            distance.drop(col, inplace=True, axis=1)
            print "Null:", col
        if sum(distance[col]) == 0:
            distance.drop(col, inplace=True, axis=1)
            print "Zeros:", col

    # Training set 需要有label
    if is_training_set:
        distance = pd.concat([distance, data_set.ix[:, -1]], axis=1)  # Append Label
    return distance


def read_csv_data(is_training_set, category):
    """
    :param is_training_set: boolean
    :param category: int
    :returns:
        attr: 商品attributes
        dummy: attr的dummy variable版
        data:　商品對的Jaccard相似度
    """
    if is_training_set:
        attr_name = 'attr_train_'
        data = pd.read_csv('train_' + str(category) + '.csv', encoding='utf8')
    else:
        attr_name = 'attr_test_'
        data = pd.read_csv('test_' + str(category) + '.csv', encoding='utf8')
        data['Label'] = 0.0

    attr_name += 'full_'
    attr = pd.read_csv(attr_name + str(category) + '.csv', encoding='utf-8-sig')
    dummy = pd.read_csv(attr_name + str(category) + '_dummy.csv', encoding='utf-8-sig')
    return attr, dummy, data


# model fitting part
def separate_positive_negative(train, threshold=0.5):
    """
    拆解成正負例
    :param train: 訓練數據
    :param threshold: 閾值
    :return: 正負例
    """
    """
    classification: thresold=0.5
    """
    train_positive = train[train['Label'] > threshold]
    # train_negative = train[train['Label'] <= threshold]
    return train_positive


# down sample test set to use as negative training set
# Size of the test set= ratio* size of train set
def down_sample_testset(train_positive, test, ratio, is_random=False):
    """
    下採樣隨機商品對
    :param train_positive: 正例
    :param test: 全量負例
    :param ratio: 下採樣比例
    :param is_random: 是否設置random state
    :return: 下採樣之負例
    """
    if is_random:
        print 'Random Under Sampling'
    else:
        np.random.seed(2016)
    random_index = np.random.randint(0, len(test), ratio * len(train_positive))
    sampled_test = test.iloc[random_index, :]
    sampled_test = sampled_test.reset_index(drop=True)

    return sampled_test


def generate_model_input(train_positive, sampled_test, train_distance_positive, test_distance):
    """
    生成最終模型使用的數據
    :param train_positive: 正例
    :param sampled_test: 下採樣之負例
    :param train_distance_positive: 正例之距離特徵
    :param test_distance: 負例之距離特徵
    :return:
        X: 正例+負例的特徵維度
        y: label
    """
    data = train_positive.iloc[:, 1:-1].append(sampled_test.iloc[:, 1:-1], ignore_index=True)  # skip labels first
    word_vec = train_distance_positive.append(test_distance, ignore_index=True).iloc[:, 2:]  # skip customer & competitor ID
    data = pd.concat([data, word_vec], axis=1)
    data = data.fillna(0)

    X = data.drop(['Label', 'ID_competitor', 'ID_customer', 'Unnamed: 0'], axis=1)  # skipped ID, label
    y = data['Label']

    return X, y



#
# for cid in [50010850, 50000697, 50008898, 162201]:
#     create_dummy_attr('attr_train_full_', cid)
#     create_dummy_attr('attr_test_full_', cid)
# create_dummy_attr('attr_test_full_', 162205)