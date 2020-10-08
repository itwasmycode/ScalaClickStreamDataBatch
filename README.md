# ScalaClickStreamDataBatch

## Project Description
ClickStream Analysis Project using Frameless-Spark API on Scala using YooChoose ClickStream Dataset. This Dataset taken from real shut down company. This project will show how a Data Science Project is done by structuring well-organized clean code with Railway transformation style.

#### Training Data = 1.5GB
#### Test Data = 400MB

### Technologies
* Scala
* Spark
* Frameless
* Jupyter Notebook

### Methods Used
* Descriptive Statistics
* Machine Learning
* Predictive Modeling
* Data Cleaning
* Information Extraction
* Data Transformation
* Data Wrangling
* SQL Queries
* Statistical Feature Elimination Algorithms

## Customized Plan for My Data Science Project
### ETL Phase
 * First one is looking, examining, different portion of data and write ideas to digital or physical paper.
 * After that, extracting what type of informations do we need to apply to data to make them ready for modelling.
 * If extracting and transforming features will be used with Spark SQL or similar SQL Framework. Hold queries to a private file or pull them when it is required or make them package object. My choice was implementing as an Interface solution.
 * Most likely, if data is real world data, data is unstructured even dates are described as Linux TimeStamp, extract which features are going to be cleaned.
 * Start to make railway Data Transformation Solution, use as less as possible mutable variables and avoid to global variable, objects. Use placeholder, namingspaces.
 * In order to data should be accessible later, after every transformation, write or hold shape of data after transformation look like.
 
 ### Modelling Phase
  * Start with linear models.
  * Build simplest model you can get result and make it baseline model.
  * If data is imbalanced, use Precision,Recall,F-score,AUC scores in order to don't miss information.
  * Build one non-linear model, and look how they perform. If there is not a lot of feature at the first, probably your model will not lie about overfitting. If non-linear model performs well, continue with non-linear one and make it baseline.
  * Since there is no best model, try to understand, experiment it adding, using different variables, variables combination, removing, adding features.
  **Note: Alternative way to measuring data by looking accuracy is using oversampling or undersampling algorithms. I didn't choose that for this project.
  
 ### Deliverable Notebook
 * [Notebook](https://nbviewer.jupyter.org/github/controlmachine/ScalaClickStreamDataBatch/blob/master/src/main/notebooks/Scala%20CStream.ipynb)
