import sys, os, re
import findspark
findspark.init()
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, IDFModel, StopWordsRemover
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.tuning import CrossValidator

def preprocess(text):
    words = []
    for w in text.split():
        if not w.startswith('@') and not w.startswith('#') and w != 'RT':
            words.append(w) 
    word_str = (" ").join(words)        
    clean_words = re.sub("[^a-zA-Z]", " ", word_str).lower().split()
    return clean_words

class Predictor():
    def __init__(self):
        #print ("Configuring Path ... \n")
        self._add_path()
        #print ("Loading Saved Models ...\n")
        self._load_models()
        
        self.pp_udf = udf(preprocess, ArrayType(StringType()))
        self.remover = StopWordsRemover(inputCol="Words", outputCol="filtered")

    def _add_path(self):
        self.params_path = './tmp/{}'

    def _load_models(self):
        hf_path = self.params_path.format('hf')
        idf_path = self.params_path.format('idfmodel')
        rf_path = self.params_path.format('rfmodel')

        self.hashingTF = HashingTF.load(hf_path)
        self.idfmodel = IDFModel.load(idf_path)
        self.rf = RandomForestClassificationModel.load(rf_path)

    def predict(self, df):
        words = df.withColumn('Words', self.pp_udf(df.text))
        removed = self.remover.transform(words)
        featureized = self.hashingTF.transform(removed)
        result = self.idfmodel.transform(featureized)
        prediction = self.rf.transform(result)
        return prediction
