Google Colab links
pyspark_trials(part1):- https://colab.research.google.com/drive/1QpCVdbSpPRHq9lOor4WqfzYo05EgF3Ko <br/>
part 2 :- https://colab.research.google.com/drive/1orNRYhHnn9uEsx2aLwvv-kkvpMLv4l0Q <br/>
part 3 :- https://colab.research.google.com/drive/18Cz4B7Y71XLtOf4OpsB36H00i4nB0Sjs <br/>
part 4 :- https://colab.research.google.com/drive/1JvPSj3wZP7NQh3V8zvHK2qszhvZ8szRb <br/>
part 5 :- https://colab.research.google.com/drive/1Xt0ffE6GgI0vZuu9xzGAjiulI8PHp2YQ <br/>
part 6 :- https://colab.research.google.com/drive/14T-upswXWj8uR7EwxT_LWy-XEhOJKX7g

###### First block to load pyspark once (run this only once) .. In case it gives an error in future, just check  https://www.apache.org/dyn/closer.lua/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz andput ######## the mirror for windows

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://apachemirror.wuchna.com/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
!tar xf spark-2.4.5-bin-hadoop2.7.tgz
!pip install -q findspark


#### Block to start pyspark session 

import os
import findspark

import numpy as np
os.environ["JAVA_HOME"]   = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"]  = "/content/spark-2.4.5-bin-hadoop2.7"
findspark.init("spark-2.4.5-bin-hadoop2.7")# SPARK_HOME
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import *
spark                      = SparkSession.builder.master("local[*]").getOrCreate()
