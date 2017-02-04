# -*- coding: utf-8 -*-
"""
Created on Fri Feb 03 12:39:13 2017

@author: Dawson

session length
"""

import pandas
import seaborn as sns
import matplotlib.pyplot as plt



dataset=pandas.read_csv(r'F:\app\data\workload\sessionlength2.csv', engine='python' )
dataset=dataset.dropna()

trainY=dataset['sessionlen'].values
trainX=dataset.ix[:,[1,2,3,4]].values
                 
from sklearn.svm import SVR
Model_Svr=SVR(C=5.0, epsilon=0.5)
Model_Svr.fit( trainX , trainY )

testY=Model_Svr.predict(trainX)


plt.plot(trainY , color='b')
plt.plot(testY , color='g')
plt.show()






