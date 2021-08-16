# WSB-BigData-Project2
UK Data traffic analysis

# Setup

1. Umieszcamy potrzebne pliki z danymi w Google Storage.
2. Piszemy program. Można dla ułatwienia pisać w Zeppelinie, a potem przenosić działający kod do IntelliJ żeby wygenerować JARa.
3. Dodajemy konfiguracje JARa. (Project Structure -> Artifacts)
4. Generujemy JARa. (Build -> Build Artifacts)
# Run

1. Odpalamy klaster. Przykładowo:

```
CLUSTER_NAME=DO_ZMIANY
PROJECT_ID=DO_ZMIANY
BUCKET_NAME=DO_ZMIANY

gcloud beta dataproc clusters create ${CLUSTER_NAME} --enable-component-gateway --bucket ${BUCKET_NAME} --region europe-west3 --zone europe-west3-b --master-machine-type n1-standard-2 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 --image-version preview-debian10 --optional-components ZEPPELIN --project ${PROJECT_ID} --max-age=3h
```

2. Przenosimy pliki ze Storage do hdfsa.

Przykładowo:
```
hadoop fs -copyToLocal gs://{sciezka}/regionsScotland.csv 
hadoop fs -copyToLocal gs://{sciezka}/authoritiesScotland.csv 
hadoop fs -mkdir -p proj/spark 
hadoop fs -copyFromLocal *.csv proj/spark
```

3. Wrzucamy wygenerowanego wcześniej JARa do klastra, np. przez SSH i opcję Upload Files.
4. Uruchamiamy JARa:

```
spark-submit --class com.wsb.project.TempObject \
    --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 \
    temp-jar.jar
```

Gdzie **com.wsb.project.TempObject** to nasza klasa, a **temp-jar.jar** to nazwa wygenerowanego JARa.

Przykładowy output, dla TempObject:

```
21/01/12 21:37:22 INFO org.sparkproject.jetty.util.log: Logging initialized @3412ms to org.sparkproject.jetty.util.log.Slf4jLog
21/01/12 21:37:22 INFO org.sparkproject.jetty.server.Server: jetty-9.4.z-SNAPSHOT; built: 2019-04-29T20:42:08.989Z; git: e1bc35120a6617ee3df052294e433f3a25ce7097; jvm 1.8.0_275-b01
21/01/12 21:37:22 INFO org.sparkproject.jetty.server.Server: Started @3571ms
21/01/12 21:37:22 INFO org.sparkproject.jetty.server.AbstractConnector: Started ServerConnector@2b27cc70{HTTP/1.1,[http/1.1]}{0.0.0.0:46349}
21/01/12 21:37:23 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at big-data-processing-wsb-cluster-1-m/10.156.15.196:8032
21/01/12 21:37:23 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at big-data-processing-wsb-cluster-1-m/10.156.15.196:10200
21/01/12 21:37:24 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
21/01/12 21:37:24 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
21/01/12 21:37:25 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1610485272095_0004
21/01/12 21:37:26 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at big-data-processing-wsb-cluster-1-m/10.156.15.196:8030
+---+
|  1|
+---+
|  1|
+---+

21/01/12 21:37:38 INFO org.sparkproject.jetty.server.AbstractConnector: Stopped Spark@2b27cc70{HTTP/1.1,[http/1.1]}{0.0.0.0:0}
```
