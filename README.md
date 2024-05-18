## Processamento de Dados
O código da solução deverá ser desenvolvido utilizando o PySpark, podendo ser utilizadas as APIs: RDD, DataFrames ou Spark SQL.

### Camada Trusted
A solução deverá ser capaz de processar os dados contidos nos arquivos .CSV da pasta **datalake/raw/covid19**, efetuando uma unificação dos registros em uma única tabela e armazenando o seu resultado no diretório **datalake/trusted**.

A tabela desenvolvida nesta camada deverá atender a seguinte estrutura:
|Coluna|Descrição|Formato|
|---|---|---|
|pais|Deverá conter a descrição do País|string|
|estado|Deverá conter a descrição do Estado ou Província|string|
|latitude|Deverá conter coordenada geográfica de latitude|double|
|longitude|Deverá conter coordenada geográfica de longitude|double|
|data|Deverá conter a data do registro|timestamp|
|quantidade_confirmados|Deverá conter a quantidade de **novos** casos Confirmados na data específica|long|
|quantidade_mortes|Deverá conter a quantidade de **novas** Mortes na data específica|long|
|quantidade_recuperados|Deverá conter a quantidade de **novos** Recuperados na data específica|long|
|ano|Coluna de partitionamento que deverá conter o ano extraído da coluna data|int|
|mes|Coluna de partitionamento que deverá conter o mes extraído da coluna data|int|

### Camada Refined
A solução deverá ser capaz de processar os dados contidos na tabela anteriormente criada na camada **trusted**, efetuando uma agregação e cálculo das médias móveis dos 3 tipos de casos nos últimos 7 dias, armazenando o seu resultado no diretório **datalake/refined**.

A tabela desenvolvida nesta camada deverá atender a seguinte estrutura:
|Coluna|Descrição|Formato|
|---|---|---|
|pais|Deverá conter a descrição do País|string|
|data|Deverá conter a data do registro|timestamp|
|media_movel_confirmados|Deverá conter a média móvel dos últimos 7 dias de casos Confirmados até data específica|long|
|media_movel_mortes|Deverá conter a média móvel dos últimos 7 dias de Mortes até data específica|long|
|media_movel_recuperados|Deverá conter a média móvel dos últimos 7 dias de Recuperações até data específica|long|
|ano|Coluna de partitionamento que deverá conter o ano extraído da coluna data|int|

### Armazenamento de Dados
- Os dados nas camadas **trusted** e **refined** devem ser armazenados no formato **PARQUET**
- Os dados nas camadas **trusted** deverão estar particionados pelas colunas **ano e mes**
- Os dados nas camadas **refined** deverão estar particionados somente pela coluna **ano**
- Os dados nas camadas **trusted** e **refined** deverão conter apenas 1 arquivo **PARQUET** em cada Partição.
