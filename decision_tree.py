import random
from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_curve, f1_score
from matplotlib import pyplot as plt
def create_tree(url_dict):
  features = []
  classes = []
  for url in url_dict:
    classes.append(url_dict[url]['class'])
    features.append(url_dict[url]['features'])
  feature_class_data = list(zip(features, classes))
  random.shuffle(feature_class_data)
  shuffled_features, shuffled_classes = zip(*feature_class_data)
  sub_samples = create_ten_folds([shuffled_features, shuffled_classes])
  accuracy_sum = 0.0
  f_score_sum = 0.0
  for i in range(len(sub_samples)):
    training_sub_samples = sub_samples[:i] + sub_samples[i+1:]
    training_features = [tup[0] for tup in training_sub_samples]
    X_train = [item for sublist in training_features for item in sublist]
    training_classes = [tup[1] for tup in training_sub_samples]
    Y_train = [item for sublist in training_classes for item in sublist]

    X_test = [x for x in sub_samples[i][0]]
    Y_test = [x for x in sub_samples[i][1]]
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(X_train, Y_train)
    predicted_vals = clf.predict(X_test)
    accuracy = accuracy_score(Y_test, predicted_vals)
    f_measure = f1_score(Y_test, predicted_vals)
    accuracy_sum += accuracy
    f_score_sum += f_measure
  print "Total accuracy: ", accuracy_sum / len(sub_samples)
  print "F measure: ", f_score_sum / len(sub_samples)

def create_ten_folds(data):
  features = data[0]
  classes = data[1]
  sub_sample_size = len(features) / 10
  sub_samples = []
  for i in range(10):
    start = sub_sample_size*i
    end = sub_sample_size*(i + 1)
    if i == 9:
      end = len(features)
    sub_samples.append((features[start:end], classes[start:end]))
  return sub_samples