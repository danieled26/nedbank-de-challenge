FROM nedbank-de-challenge/base:1.0

COPY pipeline/ /app/pipeline/
COPY config/ /app/config/
COPY requirements.txt /app/requirements.txt

WORKDIR /app

ENV PYTHONPATH=/app
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"

RUN pip install --no-cache-dir -r requirements.txt

# Pre-resolve Delta jars at image build time, not runtime.
RUN python - <<'PY'
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .master("local[1]")
    .appName("prefetch-delta-jars")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.stop()
print("Delta jars prefetched")
PY

CMD ["python", "-m", "pipeline.run_all"]