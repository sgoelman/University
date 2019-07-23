

# Yossi                             ours
# review =                          FilteredColumn
# split_sentence_into_words         wordsFromSeentence



from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import Tokenizer
import pyspark.sql.functions as func


data =spark.read.option("header", "true").\
    option("mode", "DROPMALFORMED")\
    .csv("winemag-data-130k-v2.csv")

# _____________________________________________________________
# todo: delete
data.describe('description')\
    .show()

# _____________________________________________________________
# todo: delete
data.describe('points')\
    .show()

# _____________________________________________________________
# todo: delete
data.printSchema()


# _____________________________________________________________
# _____________________________________________________________

# todo: delete
data.count()

# _____________________________________________________________
# _______________________________________________

data = data.select('points', 'description')\
    .dropna()
data.count()


# _____________________________________________________________
# _____________________________________________________________


data = data.where(length(col('description')) >= 75)

# _____________________________________________________________

# _____________________________________________________________


# todo: delete
data.count()

# _____________________________________________________________
# todo: change columns
def filterData(column):

    return trim(lower(regexp_replace(column, '[^\sa-zA-Z0-9]', ''))).alias('FilteredColumn')

filteredData = data.select([filterData(data['description']), col("points").alias("label")])

# נבחר לעשות  שלילי וחיובי
filteredData = filteredData.withColumn("label", when(col("label") > 80, 1.0).otherwise(0.0))

# _____________________________________________________________



dataFrame = data.withColumn('descriptionWordCount', func.size(func.split(func.col('description'), ' ')))
dataFrame.agg(func.mean('descriptionWordCount'), func.count('descriptionWordCount')).show()

# _____________________________________________________________
# _____________________________________________________________




# _____________________________________________________________
# _____________________________________________________________
# 60
# Now split each sentence into words, also called word tokenization.

token = Tokenizer(inputCol="FilteredColumn", outputCol="split_sentence_into_words")
wordsDF = token.transform(filteredData)

# _____________________________________________________________
# _____________________________________________________________


# Remove stop words
remover = StopWordsRemover(inputCol="split_sentence_into_words", outputCol="clean_word")
wordsDF2 = remover.transform(wordsDF)

# _____________________________________________________________

# convert to binary
hashingTF_binary = HashingTF(inputCol="clean_word", outputCol="features", binary = True)
words_binary_df = hashingTF_binary.transform(wordsDF2)
words_binary_df.show()

# _____________________________________________________________





# Split data into training and testing set
(training, test) = words_binary_df.randomSplit([0.7, 0.3],seed=100)



# _____________________________________________________________


class Evaluator():
    def __init__(self, model):
        self.model = model

    def run(self, training, test):
        fitModel = self.model.fit(training)
        self.predictions = fitModel.transform(test)

    def classification_evaluators(self):
        # Evaluate result with accuracy
        evaluator1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                       metricName="accuracy")
        accuracy = evaluator1.evaluate(self.predictions)
        print("Model Accuracy: ", accuracy)

        # Evaluate result with ROC
        evaluator2 = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
        AUC = evaluator2.evaluate(self.predictions)
        print("area under ROC: ", AUC)

    def get_score(self):
        lr_label_predictions_df = self.predictions.select(["label", "prediction"])

        tp_count = lr_label_predictions_df.filter("label=1.0 and prediction=1.0").count()

        prediction_one = lr_label_predictions_df.filter("prediction=1.0").count()
        precision = float(tp_count) / prediction_one

        label_one = lr_label_predictions_df.filter("label=1.0").count()
        recall = float(tp_count) / label_one

        print("recall", recall, ", precision", precision)

# _____________________________________________________________


nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
naiveBayesEvaluator = Evaluator(nb)
naiveBayesEvaluator.run(training,test)
naiveBayesEvaluator.classification_evaluators()
naiveBayesEvaluator.get_score()



# _____________________________________________________________




lr = LogisticRegression(maxIter=10)
logisticRegressionEvaluator = Evaluator(lr)
logisticRegressionEvaluator.run(training,test)
logisticRegressionEvaluator.classification_evaluators()
logisticRegressionEvaluator.get_score()

# _____________________________________________________________



class EvaluatorClustering():
    def __init__(self,model):
        self.modelInstance = model
    def run(self,featureTable,reviewTable):
        self.model =  self.modelInstance.fit(featureTable)
        self.predictions = self.model.transform(reviewTable)

    def evaluators(self):
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(self.predictions)
        print("Silhouette with squared euclidean distance = " + str(silhouette))
        # Shows the result.
        centers = self.model.clusterCenters()
        print("Cluster Centers: ")
        for center in centers:
            print(center)

# _____________________________________________________________




ec = EvaluatorClustering(KMeans().setK(2).setSeed(100))
featureTable = words_binary_df.select('features')
reviewTable = words_binary_df.select(['label','FilteredColumn','features'])

ec.run(featureTable,reviewTable)
ec.evaluators()




# _____________________________________________________________

wordsDF2.show()


# _____________________________________________________________


idf = IDF(inputCol="tf", outputCol="features")
idfModel = idf.fit(wordsDF)
words_tf_idf_DF = idfModel.transform(wordsDF)
words_tf_idf_DF.show()

# _____________________________________________________________




(training, test) = words_tf_idf_DF.randomSplit([0.7, 0.3],seed=100)



# _____________________________________________________________


nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
naiveBayesEvaluator = Evaluator(nb)
naiveBayesEvaluator.run(training,test)
naiveBayesEvaluator.classification_evaluators()
naiveBayesEvaluator.get_score()




# _____________________________________________________________


ec = EvaluatorClustering(KMeans().setK(2).setSeed(1))
featureTable = words_tf_idf_DF.select('features')
reviewTable = words_tf_idf_DF.select(['label','FilteredColumn','features'])

ec.run(featureTable,reviewTable)
ec.evaluators()



