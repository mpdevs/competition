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
from helper_feature import *


def select_features_rfe(X, y, estimator, step=1, cv=5, scoring=None, verbose=True):
    """
    隨機特徵選擇
    :param X: 訓練數據
    :param y: label
    :param estimator: 分類器
    :param step: 步距
    :param cv: cross validation數
    :param scoring: 計分函數
    :param verbose: 是否打印特徵選則訊息
    :return:
        X_transform: 篩選後的X
        rfecv.support_: list of boolean value, 對應每個特徵維度 True為保留 False為刪除
    """
    # The "accuracy" scoring is proportional to the number of correct classifications
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


def clf_model_evaluation(model, X_train, y_train, X_test, y_test, iteration=3, verbose=True):
    """
    評估/比較模型
    """
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
    """
    隨機網格調參
    :param estimator: 分類器
    :param X_train:
    :param y_train:
    :param param: 參數表
    :param cv: cross validation數
    :param scoring: 計分函數
    :param verbose: 是否打印調參訊息
    :return:
        gridCV_model.best_estimator_: 最優模型
        gridCV_model.best_score_: 最優分數
    """
    gridCV_model = GridSearchCV(estimator, param, cv=cv, n_jobs=-1, scoring=scoring)
    gridCV_model.fit(X_train, y_train)

    if verbose:
        print 'Best Training Score:', gridCV_model.best_score_
        print 'Param of the best estimator:', gridCV_model.best_params_, '\n'

    return gridCV_model.best_estimator_, gridCV_model.best_score_


