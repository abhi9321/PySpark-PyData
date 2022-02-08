pip install pyspark

source .venv/bin/activate

pip install pyspark

cd dist 

spark-submit \
 --py-files jobs.zip,shared.zip,libs.zip \
 --files config.json \
 main.py  --job movies
