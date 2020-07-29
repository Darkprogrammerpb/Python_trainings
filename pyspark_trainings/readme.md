Google Colab links
pyspark_trials(part1):- https://colab.research.google.com/drive/1QpCVdbSpPRHq9lOor4WqfzYo05EgF3Ko <br/>
part 2 :- https://colab.research.google.com/drive/1orNRYhHnn9uEsx2aLwvv-kkvpMLv4l0Q <br/>
part 3 :- https://colab.research.google.com/drive/18Cz4B7Y71XLtOf4OpsB36H00i4nB0Sjs <br/>
part 4 :- https://colab.research.google.com/drive/1JvPSj3wZP7NQh3V8zvHK2qszhvZ8szRb <br/>
part 5 :- https://colab.research.google.com/drive/1Xt0ffE6GgI0vZuu9xzGAjiulI8PHp2YQ <br/>
part 6 :- https://colab.research.google.com/drive/14T-upswXWj8uR7EwxT_LWy-XEhOJKX7g

## Some updates in the colab notebooks and codes ##

###### First block to load pyspark once (run this only once) .. In case it gives an error in future, just check  https://www.apache.org/dyn/closer.lua/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz andput ######## the mirror for windows

!apt-get install openjdk-8-jdk-headless -qq > /dev/null <br/>
!wget -q http://apachemirror.wuchna.com/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz <br/>
!tar xf spark-2.4.5-bin-hadoop2.7.tgz <br/>
!pip install -q findspark <br/>


#### Block to start pyspark session 

import os            <br/>
import findspark <br/>

import numpy as np <br/>
os.environ["JAVA_HOME"]   = "/usr/lib/jvm/java-8-openjdk-amd64" <br/>
os.environ["SPARK_HOME"]  = "/content/spark-2.4.5-bin-hadoop2.7" <br/>
findspark.init("spark-2.4.5-bin-hadoop2.7")# SPARK_HOME <br/>
from pyspark.sql import SparkSession <br/>
import pyspark.sql.functions as f <br/>
import pandas as pd <br/>
from pyspark.sql.functions import pandas_udf <br/>
from pyspark.sql.functions import PandasUDFType <br/>
from pyspark.sql.types import * <br/>
spark                      = SparkSession.builder.master("local[*]").getOrCreate() <br/>
