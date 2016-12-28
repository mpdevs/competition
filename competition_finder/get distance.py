# coding:utf-8
# __author__:YoungDream
from helper_feature import *
from helper_modeling import *


def get_prediction_distance(category_id):
    create_dummy_attr('attr_prediction_full_', category_id)
    column_word_vector_dict
    print category_dict[category_id], category_id
    print 'Prediction'
    start = time.time()
    prediction_set = pd.read_csv('prediction_' + str(category_id) + '.csv', encoding='utf8')
    attr, dummy, train = read_csv_data(is_training_set=True, category=category_id)

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
    attr = pd.read_csv(attr_name + str(category_id) + '.csv', encoding='utf-8-sig')
    dummy = pd.read_csv(attr_name + str(category_id) + '_dummy.csv', encoding='utf-8-sig')

    if len(prediction_set) > 100000:
        random_index = set(np.random.randint(0, len(prediction_set), 100000))
        sampled_prediction = prediction_set.iloc[list(random_index), :]
        sampled_prediction = sampled_prediction.reset_index(drop=True)
    else:
        sampled_prediction = prediction_set.copy()
    sampled_prediction.to_csv("sampled_prediction_" + str(category_id) + '.csv', encoding='utf8')
    print "Length of sample test", len(sampled_prediction)

    prediction_distance = generate_distance_df(attr, dummy, sampled_prediction, column_word_vector_dict,
                                               word_vectors, wordID, size=SIZE, is_training_set=False)
    print 'Length of prediction set:', len(prediction_distance)

    prediction_distance.to_csv('prediction_distance_' + str(category_id) + '.csv', encoding='utf8')

category_dict = {162116: '蕾絲', 1623: '半身裙', 121412004: '背心吊帶', 162104: '村杉',
                              50000671: 'T恤', 162103: '毛衣', 50008901: '風衣', 50011277: '短外套'}

RATIO = 1.0
is_random = True
input_type = 3

wordID, word_vectors = get_word_vector()
SIZE = 100
column_word_vector_dict = {}

categoryID_list = [50011277]
for cid in categoryID_list:
    print cid
    get_prediction_distance(cid)