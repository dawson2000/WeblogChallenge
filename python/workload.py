# -*- coding: utf-8 -*-
"""
Created on Thu Feb 02 19:25:32 2017

@author: Dawson

Model : LSTM for time serial 

Base on Keras

"""

import pandas
import seaborn as sns
import matplotlib.pyplot as plt



dataset=pandas.read_csv(r'F:\app\data\workload\works.csv', engine='python' )

dataset['nextload']=dataset['workload'].shift(-1)


dataset['loadhour']=map( lambda x: int(str(x)[8:10] )  , dataset['time'] )
dataset['loadmin']=map( lambda x: int(str(x)[10:12] ) , dataset['time'] )

labelset=dataset.dropna()

del labelset['time']
print  labelset.describe()
print  labelset.corr()

plt.figure( figsize=(20, 15) ) 
#sns.heatmap( labelset.corr() )


count=labelset.count()[0]
dataY=labelset['nextload'].values
dataX=labelset.loc[:,['loadhour','loadmin','workload']].values
                  
# create model using  SVR
                  
from sklearn.svm import SVR
Model_Svr=SVR(C=13.0, epsilon=0.1)
Model_Svr.fit( dataX , dataY )

testY=Model_Svr.predict(dataX)


plt.plot(testY , color='b')
plt.plot(dataY , color='g')
plt.show()


# create model using tree
from sklearn.tree import DecisionTreeRegressor


Model_DT=DecisionTreeRegressor(max_depth=50)
Model_DT.fit( dataX  , dataY )


dataY = Model_DT.predict( dataX )
plt.figure( figsize=(20, 15) ) 
plt.plot(testY , color='r')
plt.plot(dataY , color='g')
plt.show()





















