# coding: utf-8
# __author__: u"John"
"""
基于训练模型
"""
from helper import *
from db_apis import *
from collections import OrderedDict
from common.db_apis import *
from common.settings import *
from common.pickle_helper import pickle_dump
from common.debug_helper import info
from datetime import datetime
from helper_modeling import *
from helper_feature import *
from settings import *

import sys
reload(sys)
sys.setdefaultencoding("utf8")


class CalculateCompetitiveItems(object):
    # region 内部所有属性的初始化
    # 舊的 source table: u"itemmonthlysales2015"
    def __init__(self, industry=u"mp_women_clothing", source_table=u"itemmonthlysales2015",
                 target_table=u"itemmonthlyrelation_2015", shop_id=None, cid=None):
        info(u"{0} 正在连接数据库 ...".format(datetime.now()))
        # region 必要
        self.industry = industry
        self.source_table = source_table
        self.target_table = target_table
        self.customer_shop_id = shop_id
        self.date_range = get_max_date_range(db=self.industry, table=self.source_table)
        self.date_range_list = None
        info(u"{0} 正在获取品类信息... ".format(datetime.now()))
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.industry)}
        self.category_id = cid
        # 用来将商品标签向量化
        info(u"{0} 正在抽取标签元数据... ".format(datetime.now()))
        self.attribute_meta = get_attribute_meta(db=self.industry)

        self.feature_type = 'Jaccard'
        # self.items_attributes = None
        # key=CategoryID value=CategoryName
        # CID: CNAME
        info(u"{0} 正在生成商品的标签dict... ".format(datetime.now()))
        self.tag_dict = tag_to_dict(df=transform_attr_value_dict(self.attribute_meta))
        self.training_data = None
        self.new_training_data = None
        self.train_x = None
        self.train_x_positive = dict()
        self.train_x_negative = dict()
        self.test_x = dict()
        self.train_y = None
        # key=category value=model
        self.model = OrderedDict()
        self.predict_x = None
        self.predict_y = None
        self.message = None
        self.customer_shop_items = None
        self.competitor_items = None
        self.item_pairs = None
        self.es_item_pairs = None
        self.data_to_db = None
        # endregion
        # region 可选
        # 必要维度法
        self.use_essential_tag_dict = True
        self.essential_tag_data = pd.read_csv('essential_tag_data.csv', encoding='utf8')
        self.essential_tag_dict_all = parse_essential_dimension(self.essential_tag_data) \
            if self.use_essential_tag_dict else None
        self.essential_tag_dict = None

        # 重要維度法
        self.use_important_tag_dict = True
        self.important_tag_data = pd.read_csv('important_tag_data.csv', encoding='utf8')
        self.important_tag_dict_all = parse_essential_dimension(self.important_tag_data) \
            if self.use_important_tag_dict else None
        self.important_tag_dict = None
        # self.essential_tag_dict = parse_essential_dimension(get_essential_dimensions(db=self.industry)) \
        #     if self.use_essential_tag_dict else None
        self.statistic_info = []
        # endregion
        # region 用于子类的继承
        self.construct_prediction_feature = construct_prediction_feature
        # endregion
        self.SIZE = 100
        self.wordID, self.word_vectors = get_word_vector(self.SIZE)
        self.column_word_vector_dict = {}
        # self.category_dict = {162116: '蕾絲', 1623: '半身裙', 121412004: '背心吊帶', 162104: '村杉',
        #                       50000671: 'T恤', 162103: '毛衣', 50008901: '風衣', 50011277: '短外套'}
        self.model_dict = {'LR': {'model': LogisticRegression()},
                      'GBDT': {'model': GradientBoostingClassifier()},
                      'KNN': {'model': KNeighborsClassifier()},
                      'RF': {'model': RandomForestClassifier()},
                      'NB': {'model': GaussianNB()},
                      'Ada': {'model': AdaBoostClassifier()},
                      'SVM': {'model': SVC()},
                      'XGB': {'model': xgb.sklearn.XGBClassifier()}}
        self.RATIO = 1.0
        self.is_random = True
        self.sampling_times = 15

        return

    # endregion

    # region print实例的值
    def __str__(self):
        """
        调试程序
        :return:
        """
        string = u"industry={0}\ncustomer_shop_id={1}\nCategoryName={2}\n".format(
            self.industry, self.customer_shop_id, self.category_dict[self.category_id])
        return string

    # endregion

    # 崴
    def build_train_raw_feature(self):
        """
        輸出訓練數據中正例的原始數據
        :return:
        """
        info(u"{0} 正在获取训练数据... ".format(datetime.now()))
        self.training_data = get_training_data(cid=self.category_id)
        self.new_training_data = get_new_training_data(cid=self.category_id)

        self.training_data = pd.concat([self.training_data, self.new_training_data], ignore_index=True)
        self.training_data.columns = [u'attr1', u'attr2', u'score', u'ItemID', u'ItemID2']
        self.new_training_data.columns = [u'attr1', u'attr2', u'score', u'ItemID', u'ItemID2']
        self.training_data = self.training_data.sort_values(
            [self.training_data.columns[3], self.training_data.columns[4]])

        index_to_drop = list()
        for i in range(len(self.training_data) - 1):
            if self.training_data.iloc[i, 3] == self.training_data.iloc[i + 1, 3] \
                    and self.training_data.iloc[i, 4] == self.training_data.iloc[i + 1, 4]:

                index_to_drop.append(i + 1)
        print 'len of training set:', len(self.training_data)
        self.training_data.drop(self.training_data.index[index_to_drop], inplace=True)
        print len(self.training_data)

        info(u"{0} 正在构造训练数据的特征矩阵, 行数为{1}... ".format(datetime.now(), len(self.training_data)))
        print self.category_id

        # 未打標籤 labeling = False
        attr_train_full = construct_train_raw_feature(raw_data=self.training_data.values.tolist(),
                                                      tag_dict=self.tag_dict[self.category_id], labeling=False)
        export_csv(attr_train_full, 'attr_train_full_' + str(self.category_id) + '.csv')

        return None

    def get_result(self):
        result = get_result_data(cid=self.category_id)
        result.to_csv('result' + str(self.category_id) + '.csv', encoding='utf8')

        return

    # region 特征(feature)构造
    def build_train_feature(self):
        """
        輸出正例的Jaccard距離特徵
        :return:
        """
        info(u"{0} 正在获取训练数据... ".format(datetime.now()))
        self.training_data = get_training_data(cid=self.category_id)
        self.new_training_data = get_new_training_data(cid=self.category_id)

        self.training_data = pd.concat([self.training_data, self.new_training_data], ignore_index=True)
        self.training_data.columns = [u'attr1', u'attr2', u'score', u'ItemID', u'ItemID2']

        self.training_data = self.training_data.sort_values([self.training_data.columns[3], self.training_data.columns[4]])
        self.training_data.iloc[:, 3:5].astype(long)

        index_to_drop = list()
        for i in range(len(self.training_data)-1):
            if self.training_data.iloc[i, 3] == self.training_data.iloc[i + 1, 3] \
                    and self.training_data.iloc[i, 4] == self.training_data.iloc[i + 1, 4]:
                # print self.training_data.iloc[i, 3], self.training_data.iloc[i+1, 3]
                # print self.training_data.iloc[i, 4], self.training_data.iloc[i+1, 4]
                # print '__________________________________________'
                index_to_drop.append(i+1)
        print 'len of training set:', len(self.training_data),
        self.training_data.drop(self.training_data.index[index_to_drop], inplace=True)
        print len(self.training_data)


        # 构造训练数据
        # training_data: attr1, attr2, score
        info(u"{0} 正在构造训练数据的特征矩阵, 行数为{1}... ".format(datetime.now(), len(self.training_data)))
        print self.category_id

        self.train_x, self.train_y, ID1, ID2 = construct_train_feature(raw_data=self.training_data.values.tolist(),
                                                                       tag_dict=self.tag_dict[self.category_id],
                                                                       demo=False)

        # 崴 extract training set (X & y) with ItemID to csv
        training_set = pd.concat([pd.DataFrame(self.train_x), pd.DataFrame(self.train_y)], axis=1)
        training_set = pd.concat([pd.DataFrame(ID2), pd.DataFrame(training_set)], axis=1)
        training_set = pd.concat([pd.DataFrame(ID1), pd.DataFrame(training_set)], axis=1)

        col = ['ID_customer', 'ID_competitor']
        for i in self.tag_dict[self.category_id].keys():
            col.append(i)
            print i
        col.append(u'Label')
        print "length of col: ", len(col)
        print training_set.shape
        csv_name = "train_" + str(self.category_id) + ".csv"
        export_csv(pd.DataFrame(training_set), csv_name, header=col)

        # # region 用于echarts展示
        # train_x_demo, train_y_demo, id, id2, attr1, attr2 = construct_train_feature(raw_data=self.training_data.values.tolist(),
        #                                                      tag_dict=self.tag_dict[self.category_id], demo=True)
        # df_x = pd.DataFrame(train_x_demo)
        # df_y = pd.DataFrame(train_y_demo, columns=[u"y"])
        # df = pd.concat([df_x, df_y], axis=1, join_axes=[df_x.index])
        # self.train_x_positive[self.category_id] = df[df.y > 0.5].values
        # self.train_x_negative[self.category_id] = df[df.y <= 0.5].values
        # self.train_x, self.train_y = sample_balance(train_x=self.train_x, train_y=self.train_y)
        # # endregion

        return

    def build_test_feature(self):
        """
        輸出測試集和負例Jaccard距離特徵
        :return:
        """
        self.source_table = u"itemmonthlysales2015"

        info(u"{0} 正在获取预测数据... ".format(datetime.now()))
        # TaggedItemAttr, item_id, shop_id DataFrame
        info(u"{0} 正在获取客户商品信息... ".format(datetime.now()))
        self.customer_shop_items = get_customer_shop_items(
            db=self.industry, table=self.source_table, shop_id=self.customer_shop_id, date_range=self.date_range,
            category_id=self.category_id)
        # 构造预测数据
        info(u"{0} 正在获取竞争对手数据... ".format(datetime.now()))
        # 获取客户店铺之外所有对应品类下的商品信息
        self.competitor_items = get_competitor_shop_items(
            db=self.industry, table=self.source_table, category_id=self.category_id,
            date_range=self.date_range)
        info(u"{0} 正在构造预测用数据的特征矩阵 品类是<{1}>, 月份为<{2}>... ".format(
            datetime.now(), self.category_id, self.date_range))
        # 如果没有必要维度的品类
        self.essential_tag_dict = None

        # 崴 extract prediction set raw features to one csv
        # 未打標籤 labeling = False
        attr1_test_full = construct_prediction_raw_feature(
            raw_data=self.customer_shop_items.values, tag_dict=self.tag_dict[self.category_id], labeling=False)

        attr2_test_full = construct_prediction_raw_feature(
            raw_data=self.competitor_items.values, tag_dict=self.tag_dict[self.category_id], labeling=False)

        attr_test_full = attr1_test_full.append(attr2_test_full, ignore_index=True)
        attr_test_full.to_csv('attr_test_full_' + str(self.category_id) + '.csv')

        info(u"{0} 正在构造预测用数据的Jacaard矩阵 品类是<{1}>, 月份为<{2}>... ".format(
            datetime.now(), self.category_id, self.date_range))

        # John 原本的: 建構 Jaccard Similarity
        self.predict_x, self.item_pairs, self.es_item_pairs = construct_prediction_feature(
            customer_data=self.customer_shop_items.values,
            competitor_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id], essential_tag_dict=self.essential_tag_dict, important_tag_dict=self.important_tag_dict, )

        # 崴 Extract testing set as CSV files
        testing_set = pd.DataFrame(self.predict_x)
        testing_set = pd.concat([pd.DataFrame(self.item_pairs), testing_set], axis=1)

        col = ['ID_customer', 'ID_competitor']
        print self.category_id
        for i in self.tag_dict[self.category_id].keys():
            col.append(i)
            # print i
        # print "length of col: ", len(col)
        csv_name = "test_" + str(self.category_id) + ".csv"
        export_csv(pd.DataFrame(testing_set), csv_name, header=col)

        return

    def build_prediction_feature(self):
        """
        輸出預測集Jaccard距離特徵
        :return:
        """
        self.source_table = u"itemmonthlysales_201607"
        self.date_range = u'20160701'

        info(u"{0} 正在获取预测数据... ".format(datetime.now()))
        # TaggedItemAttr, item_id, shop_id DataFrame
        info(u"{0} 正在获取客户商品信息... ".format(datetime.now()))
        self.customer_shop_items = get_customer_shop_items(
            db=self.industry, table=self.source_table, shop_id=self.customer_shop_id, date_range=self.date_range,
            category_id=self.category_id)
        # 构造预测数据
        info(u"{0} 正在获取竞争对手数据... ".format(datetime.now()))
        # 获取客户店铺之外所有对应品类下的商品信息
        self.competitor_items = get_competitor_shop_items(
            db=self.industry, table=self.source_table, category_id=self.category_id,
            date_range=self.date_range)
        info(u"{0} 正在构造预测用数据的特征矩阵 品类是<{1}>, 月份为<{2}>... ".format(
            datetime.now(), self.category_id, self.date_range))
        # 如果没有必要维度的品类
        # essential_tag_dict = None
        try:
            # self.essential_tag_dict = self.essential_tag_dict_all[self.category_id]
            self.essential_tag_dict = None
            self.important_tag_dict = self.important_tag_dict_all[self.category_id]
        except KeyError:
            pass
        except TypeError:
            pass

        # 崴 extract prediction set raw features to one csv
        # 未打標籤 labeling = False
        attr1_test_full = construct_prediction_raw_feature(
            raw_data=self.customer_shop_items.values, tag_dict=self.tag_dict[self.category_id], labeling=False)

        attr2_test_full = construct_prediction_raw_feature(
            raw_data=self.competitor_items.values, tag_dict=self.tag_dict[self.category_id], labeling=False)

        attr_test_full = attr1_test_full.append(attr2_test_full, ignore_index=True)
        attr_test_full.to_csv('attr_prediction_full_' + str(self.category_id) + '.csv')

        info(u"{0} 正在构造预测用数据的Jacaard矩阵 品类是<{1}>, 月份为<{2}>... ".format(
            datetime.now(), self.category_id, self.date_range))

        # John 原本的: 建構 Jaccard Similarity
        self.predict_x, self.item_pairs, self.es_item_pairs = construct_prediction_feature(
            customer_data=self.customer_shop_items.values,
            competitor_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id],
            essential_tag_dict=self.essential_tag_dict, important_tag_dict=self.important_tag_dict)

        # 崴 Extract testing set as CSV files
        testing_set = pd.DataFrame(self.predict_x)
        testing_set = pd.concat([pd.DataFrame(self.item_pairs), testing_set], axis=1)

        col = ['ID_customer', 'ID_competitor']
        print self.category_id
        for i in self.tag_dict[self.category_id].keys():
            col.append(i)
            # print i
        # print "length of col: ", len(col)
        csv_name = "prediction_" + str(self.category_id) + ".csv"
        export_csv(pd.DataFrame(testing_set), csv_name, header=col)

        # Extract item pairs excluded by Essential Tag method
        es_df = pd.DataFrame(self.es_item_pairs)
        try:
            export_csv(es_df, 'essential_item_pairs_' + str(self.category_id) + ".csv",
                       header=['ID_customer', 'ID_competitor'])
        except:
            pass

        return

    # endregion

    # region 計算word vector距離
    def get_train_distance(self):
        """
        生成正例基於word vector的距離特徵
        """
        create_dummy_attr('attr_train_full_', self.category_id)
        print self.category_dict[self.category_id], self.category_id

        # train
        print 'Train'
        start = time.time()

        attr, dummy, train = read_csv_data(is_training_set=True, category=self.category_id)
        train_distance = generate_distance_df(attr, dummy, train, self.column_word_vector_dict,
                                              self.word_vectors, self.wordID, size=self.SIZE, is_training_set=True)
        train_distance.to_csv('train_distance_' + str(self.category_id) + '.csv', encoding='utf8')

        threshold = 0.5
        train_positive = separate_positive_negative(train, threshold)
        print 'Positive training sample size:', len(train_positive), '\n'
        end = time.time()
        print end - start
        return

    def get_prediction_distance(self):
        """
        生成預測數據的基於word vector距離特徵
        """
        create_dummy_attr('attr_prediction_full_', self.category_id)
        print self.category_dict[self.category_id], self.category_id
        print 'Prediction'
        start = time.time()
        prediction_set = pd.read_csv('prediction_' + str(self.category_id) + '.csv', encoding='utf8')
        attr, dummy, train = read_csv_data(is_training_set=True, category=self.category_id)


        # 記錄所有商品對ID
        train_id_dict = {}
        train_id_list = list()
        for i in range(len(train)):
            try:
                # save customer id as key and competitor id as value
                # 若此id存在於dict的key list中 將competitor id append到已有的list上
                if train_id_dict[train.iloc[i, 1]]:
                    train_id_dict[train.iloc[i, 1]] = train_id_dict[train.iloc[i, 1]].append(train.iloc[i, 2])
            except:
                # 若不存在 則intiate一個list
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

        # 排除預測集中出現在訓練數據的商品對
        index_to_drop = list()
        for i in range(len(prediction_set)):
            try:
                customer_id = prediction_set.iloc[i, 1]
                competitor_id = prediction_set.iloc[i, 2]
                if customer_id in train_id_list:
                    try:
                        if competitor_id in train_id_dict[customer_id]:
                            index_to_drop.append(i)
                    except:
                        pass
                if competitor_id in train_id_list:
                    try:
                        if customer_id in train_id_dict[competitor_id]:
                            index_to_drop.append(i)
                    except:
                        pass
            except:
                print i
                index_to_drop.append(i)
        prediction_set.drop(prediction_set.index[[i]], inplace=True)

        # construct prediction features
        attr_name = 'attr_prediction_full_'
        attr = pd.read_csv(attr_name + str(self.category_id) + '.csv', encoding='utf-8-sig')
        dummy = pd.read_csv(attr_name + str(self.category_id) + '_dummy.csv', encoding='utf-8-sig')

        # 限制預測集大小以控制訓練時間
        max_size = 400000
        if len(prediction_set) > max_size:
            random_index = set(np.random.randint(0, len(prediction_set), max_size))
            sampled_prediction = prediction_set.iloc[list(random_index), :]
            sampled_prediction = sampled_prediction.reset_index(drop=True)
        else:
            sampled_prediction = prediction_set.copy()
        export_csv(sampled_prediction, "sampled_prediction_" + str(self.category_id) + '.csv')
        print "Length of sample test", len(sampled_prediction)

        prediction_distance = generate_distance_df(attr, dummy, sampled_prediction, self.column_word_vector_dict,
                                                   self.word_vectors, self.wordID, size=self.SIZE,
                                                   is_training_set=False)
        print 'Length of prediction set:', len(prediction_distance)

        export_csv(prediction_distance, 'prediction_distance_' + str(self.category_id) + '.csv')
        end = time.time()
        print end - start
        print '___________________________________________________________________________________________________'

    # end region

    # region 模型训练和预测
    # def train(self):
    #     model = GradientBoostingRegressor()
    #     info(u"{0} 正在生成训练模型... ".format(datetime.now()))
    #     self.model[self.category_dict[self.category_id]] = model.fit(self.train_x, self.train_y)
    #     return
    #
    # def predict(self):
    #     info(u"{0} predict_x 行数为{1}".format(datetime.now(), self.predict_x.shape[0]))
    #     if self.predict_x.shape[0] > 0:
    #         self.predict_y = self.model[self.category_dict[self.category_id]].predict(self.predict_x)
    #         df_x = pd.DataFrame(self.predict_x)
    #         df_y = pd.DataFrame(self.predict_y, columns=[u"y"])
    #         df = pd.concat([df_x, df_y], axis=1, join_axes=[df_x.index])
    #         self.test_x[self.category_id] = df.values
    #     # 对客户的每个店铺进行搜索，计算同表内不同商店的商品的竞品相似度
    #     return

    def easyensemble_prediction(self):
        """
        隨機負採樣，並集成八個模型預測概率
        :return:
        """
        # print self.category_dict[self.category_id], self.category_id
        start = time.time()
        prediction_proba_dict, prediction_dict = {}, {}

        # Construct training distance metrics
        attr, dummy, train = read_csv_data(is_training_set=True, category=self.category_id)
        train_distance = pd.read_csv('train_distance_' + str(self.category_id) + '.csv', encoding='utf8')

        threshold = 0.5
        train_distance_positive = separate_positive_negative(train_distance, threshold)
        train_positive = separate_positive_negative(train, threshold)
        print 'Positive training sample size:', len(train_positive), '\n'
        train_positive['Label'] = 1.0
        train_distance_positive['Label'] = 1.0

        # construct prediction features
        prediction_set = pd.read_csv('prediction_' + str(self.category_id) + '.csv', encoding='utf8')
        prediction = pd.DataFrame()
        prediction_proba = pd.DataFrame()

        prediction_distance = pd.read_csv('prediction_distance_' + str(self.category_id) + '.csv', encoding='utf8')
        print 'Length of prediction set:', len(prediction_distance)
        prediction_full_features = pd.concat([prediction_set.iloc[:, 3:], prediction_distance.iloc[:, 2:]], axis=1)
        prediction_full_features.fillna(0, inplace=True)
        try:
            print prediction_full_features['Label']
        except:
            print 'Good, label is not included'

        # 手動釋放RAM
        prediction_distance = 0
        prediction_set = 0

        attr, dummy, test = read_csv_data(is_training_set=False, category=self.category_id)

        self.sampling_times = 15
        for iteration in range(self.sampling_times):
            print 'Iteration:', iteration
            sampled_test = down_sample_testset(train_distance_positive, test, self.RATIO, self.is_random)
            test_distance = generate_distance_df(attr, dummy, sampled_test, self.column_word_vector_dict,
                                                 self.word_vectors, self.wordID, size=self.SIZE, is_training_set=False)
            print 'Negative training sample size:', len(test_distance), '\n'

            # Construct features
            X, y = generate_model_input(
                train_positive, sampled_test, train_distance_positive, test_distance)

            # 去除在prediction set中有 但training set中沒有的特徵
            prediction_full_features_copy = prediction_full_features.copy()
            for i in X.columns:
                if i not in prediction_full_features.columns:
                    print 'Drop:', i
                    X.drop(i, axis=1, inplace=True)
            prediction_full_features_copy = prediction_full_features_copy.loc[:, X.columns]
            prediction_full_features_copy.columns = X.columns

            # 記綠每個模型的預測
            for key in self.model_dict.keys():
                model = self.model_dict[key]['model']
                # Predict
                model.fit(X, y)
                temp = prediction_full_features_copy
                temp.columns = X.columns
                # p = pd.Series(model.predict(temp))
                try:
                    # keep the predicted probability for class == 1
                    prob = pd.Series(model.predict_proba(temp)[:, 1])
                except:
                    print key, 'does not have predict proba'
                    prob = pd.Series(model.predict(temp))

                # prediction = pd.concat([prediction, p], axis=1)
                prediction_proba = pd.concat([prediction_proba, prob], axis=1)

                # Print Feature Importance
                if key == 'XGB' and iteration == 0:
                    features_importance = pd.Series(model.feature_importances_, index=X.columns.values)
                    print features_importance.sort_values(ascending=False) * 100

            prediction_proba_dict[self.category_id] = prediction_proba
            prediction_dict[self.category_id] = prediction

            print '----------------------------------------------------------------------------------------------------'

        export_csv(prediction_proba_dict[self.category_id], 'prediction_proba_' + str(self.category_id) + '.csv')
        print 'Done'
        end = time.time()
        print 'Time:', end - start
    # endregion

    # region 入库
    def set_data(self):
        """
        将最后的score更新到数据库中, 分数要在0.5以上
        :return:
        """
        delete_score(db=self.industry, table=self.target_table, shop_id=self.customer_shop_id,
                     category_id=self.category_id, date_range=self.date_range)
        if self.predict_x.shape[0] == 0:
            info(u"{0} 没有预测用的X，跳过品类{1}".format(datetime.now(), self.category_id))
            return
        self.data_to_db = []
        for row in zip(self.item_pairs, self.predict_y):
            if row[1] >= 0.5:
                self.data_to_db.append(
                    (0, row[0][0], row[0][1], round(row[1], 4), self.date_range, self.category_id))
        info(u"{0} 开始删除店铺ID={1},品类为<{2}>,月份为<{3}>的竞品数据...".format(
            datetime.now(), self.customer_shop_id, self.category_id, self.date_range))
        info(u"{0} 开始将预测结果写入表{1}... 行数为{2}".format(
            datetime.now(), self.target_table, len(self.data_to_db)))
        set_scores(db=self.industry, table=self.target_table, args=self.data_to_db)
        self.statistic_info.append(
            {u"CID": self.category_id, u"DateRange": self.date_range, u"rows": len(self.data_to_db)})
        return

    # endregion

    # region 程序入口
    def main(self):
        """
        1. 读数据库
        2. 处理不同品类的标签, 构造特征
        3. 训练数据
        4. 预测数据
        5. 写入数据库
        6. 数据会进行分割，先按CategoryID，再按DaterRange进行双重循环
        :return:
        """

        for cid in (self.category_dict.keys()):
            self.category_id = cid
            # self.get_result()
            self.build_train_raw_feature()
            # self.build_train_feature()
            self.get_train_distance()

            # self.build_test_feature()

            # self.build_prediction_feature()
            # self.get_prediction_distance()
            # self.easyensemble_prediction()

            # self.train()
            # self.predict()
            # self.set_data()

        # region pickle用于运算检验一致性
        # pickle_dump(TRAIN_MODEL_PICKLE, self.model)
        # pickle_dump(TRAIN_X_POSITIVE_PICKLE, self.train_x_positive)
        # pickle_dump(TRAIN_X_NEGATIVE_PICKLE, self.train_x_negative)
        # pickle_dump(TEST_X_PICKLE, self.test_x)
        # self.tag_dict = tag_to_dict(df=self.attribute_meta[[u"CID", u"DisplayName", u"AttrValue"]])
        # pickle_dump(TAG_DICT_PICKLE, self.tag_dict)
        # import pandas as pd
        # return pd.DataFrame(self.statistic_info)["rows"].sum()
        # endregion
        return

    # endregion

    # region 手动垃圾回收
    def clear_training_data(self):
        self.training_data = None
        return
        # endregion


if __name__ == u"__main__":
    c = CalculateCompetitiveItems()
    c.main()
    # result = []
    # for i in range(100):
    #     result.append(c.main(industry="mp_women_clothing", source_table="itemmonthlysales2015", target_table="itemmonthlyrelation_2015", shop_id=66098091))
    # sum_value = 0
    # max_value = result[0]
    # min_value = result[0]
    # for value in result:
    #     sum_value += value
    #     if min_value > value:
    #         min_value = value
    #     if max_value < value:
    #         max_value = value
    # infou"min_value={0}, max_value={1}, avg_value={2}".format(min_value, max_value, sum_value / len(result))

# coding: utf-8
# from competitive_item_calculation import CalculateCompetitiveItems as CC
# c, c.cid = CC(), 1623L
# c.build_train_feature()
# c.build_prediction_feature()
# 100times min_value=10627, max_value=775740, avg_value=396130
