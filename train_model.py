import sys, os, re
import datetime, iso8601
import findspark
import pyspark

from pyspark.sql.types import *
from pyspark.sql.functions import col, udf

#model training stages 
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, IDFModel, StopWordsRemover
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit


def preprocess(text):
    words = []
    for w in text.split():
        if not w.startswith('@') and not w.startswith('#') and w != 'RT':
            words.append(w) 
    word_str = (" ").join(words)        
    clean_words = re.sub("[^a-zA-Z]", " ", word_str).lower().split()
    return clean_words

spark = pyspark.sql.SparkSession.builder \
            .master("local[4]") \
            .appName('train model') \
            .getOrCreate()



schema = StructType([
        StructField('target' , IntegerType(), False),
        StructField('id'     , StringType(), False),
        StructField('date'   , StringType(), False),
        StructField('flag'  , StringType(), False),
        StructField('user'  , StringType(), False),
        StructField('text'  , StringType(), False),
])

PROJECT_HOME = ""
filename = PROJECT_HOME + "./training.1600000.processed.noemoticon.csv"
users = spark.read.csv(filename, header=False, schema=schema)    

pp_udf = udf(lambda t: preprocess(t), ArrayType(StringType()))
spark.udf.register("pp_udf", pp_udf)

words = users.withColumn('Words', pp_udf('text'))


#remove stop words
remover = StopWordsRemover(inputCol="Words", outputCol="filtered")
removed = remover.transform(words)

filtered = removed.select(col('target').alias('label'), 'id', 'text', 'Words', 'filtered')

#perform term frequency
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=200)
featurized = hashingTF.transform(filtered)
#hashingTF_path = PROJECT_HOME + 'tmp/hf'
#hashingTF.save(hashingTF_path)


#perform idf
featurized.cache()
idf = IDF(inputCol="rawFeatures", outputCol="features")
model = idf.fit(featurized)
result = model.transform(featurized)

#save idf and idf model
idf_path = PROJECT_HOME + 'tmp/idf'
#idf.save(idf_path)
idfmodel_path = PROJECT_HOME + 'tmp/idfmodel'
#model.save(idfmodel_path)
#load via following
#loadedModel = IDFModel.load(idfmodel_path)

#fit single rf model
rf = RandomForestClassifier(numTrees=100, labelCol="label", seed=42)
rf_model = rf.fit(result)
rf_path = PROJECT_HOME + 'tmp/rf'
rf.save(rf_path)
rfmodel_path = PROJECT_HOME + 'tmp/rfmodel'
rf_model.save(rfmodel_path)
"""
#Prepare Train Test Split
train, test = result.randomSplit([0.8, 0.2], seed=42)

# Configure an ML pipeline, which consists of tree stages: hashingTF, idf and RandomForestClassifier.
rf = RandomForestClassifier(labelCol="label", seed=42)
pipeline = Pipeline(stages=[rf])

#grid search
paramGrid = ParamGridBuilder().addGrid(rf.numTrees, [100]).addGrid(rf.maxDepth, [5]).build()

crossval = CrossValidator(estimator=pipeline,
                            estimatorParamMaps=paramGrid,
                            evaluator=BinaryClassificationEvaluator(),
                            numFolds=3)  # use 3+ folds in practice

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(train)

# Make predictions on test documents. cvModel uses the best model found (lrModel).
prediction = cvModel.transform(test)
selected = prediction.select("text", "probability", "prediction")

#cv score
print (cvModel.avgMetrics)
#test score
evaluator = BinaryClassificationEvaluator()
print ("Test Score is {}".format(evaluator.evaluate(cvModel.transform(test))))
cvModel.getEstimatorParamMaps()

#save model
cv_path = PROJECT_HOME + 'tmp/cv'
cvModel.bestModel.save(cv_path)
if __name__ == "__main__":
    main(sys.argv[1])

"""

