#!/usr/bin/env python
# coding: utf-8

# # 7. Faça uma rotina que mensalmente colete os dados do ultimo mes e adiciona apenas os dados que sejam novos.Essa rotina deve rodar automaticamente todos os meses, escolha a forma que preferir para essa atividade.
# ---

# - Fontes: 
# 1. [Arquivo_Agosto](https://opendata.nhsbsa.net/dataset/english-prescribing-data-epd/resource/bc3cb910-f841-47e0-9fb6-203d141c57d6)
# 2. [Arquivo_julho](https://opendata.nhsbsa.net/dataset/english-prescribing-data-epd/resource/1ebdc71d-6285-4a89-abaf-ffaa406671bd)
# 
# ### Ultilizei de uma massa de dados reduzida para o exemplo, não tornando tão onoroso o processamento.

# ## Passos para filtragem e redução da massa de dados:
# #### Clicando o com o botão direito do mouse em cima do nome das tabelas, podemos filtrar quais colunas queremos para exportação dos dados.
# ![Selecao1](importacao_seletiva1.png)
# 

# ### A baixo temos as colunas que filtrei interessantes para o processo e um breve resumo a respeito delas.
# #### Para maiores detalhes consultar a documentação fornecida no site.
# 
# - **YEAR_MONTH** = Ano e mes
# - **REGIONAL_OFFICE_NAME** = Região geográfica nomeada pela NHS England.
# - **ICB_NAME** = Divisão menor de uma região.
# - **PCO_NAME** = Organização do NHS que encomenda ou presta serviços de prescrições. 
# - **PRACTICE_NAME** = Organização/Clinica que emprega um ou mais prescritores que emitem prescrições.
# - **CHEMICAL_SUBSTANCE_BNF_DESCR** = O nome do principio ativo de um medicamento ou o tipo de aparelho.
# - **BNF_DESCRIPTION** = tipo específico, força e formulação de um medicamento; ou, o tipo específico de um aparelho.
# - **QUANTITY** = A quantidade de um medicamento, curativo ou aparelho para o qual um item individual foi prescrito.
# - **ITEMS** = Vezes que um produto aparece em um formulário de prescrição.
# - **TOTAL_QUANTITY** = Total de um medicamento ou aparelho que foi prescrito. ( Poderia ter sido descondiderada, pois é o resultado das tabelas QUANTITY X ITEMS).
# - **ADQUSAGE** = A dose diária típica de um medicamento.
# - **NIC** = Custo Líquido do Ingrediente
# - **ACTUAL_COST** = Custo real
# 
# ![Selecao2](importacao_seletiva2.png)
# 
# #### Agora podemos baixar os dados selecionados de forma zipada (.gz) em particões.
# - Aqui temos em media 18~19 partições por cada arquivo ao baixar, então verificar o pop-up do seu navegador.
# ---

# #### Caso não queira fazer a descompactação manual, sugiro ultilizar a biblioteca do patoolib para descompactação:
# - **Passo a passo**
# 1. !pip install patool
# 2. import patoolib
# 3. patoolib.extract_archive("SeuDiretorioDoArquivo/Exemplo/EPD_202207-00.csv.gz",outdir="DiretorioDestino")
# 
# ---

# ### Fazendo as importações dos frameworks/metodos necessarios para executar as funções do jupyter 

# In[5]:


import pandas as pd
import pyspark.sql.functions as func

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()


# In[6]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
spark = SparkSession.builder.getOrCreate()


# ### Com os arquivos já extraidos seguimos da seguinte formar:
# - Criação de 2 DFs referentes a cada mês (agosto e junho)

# In[7]:


df_amostra_ultimo = spark.read.csv(r'C:\Users\arthu\Desktop\EPD_202208-00.csv', sep = ',', inferSchema=True, header=True)
df_amostra_anterior = spark.read.csv(r'C:\Users\arthu\Desktop\EPD_202207-00.csv', sep = ',', inferSchema=True, header=True)


# ### Analizando os dados importados e suas informações:

# In[9]:


print("Esquema amostra de Agosto:\n"
      "Total de linhas: ", df_amostra_ultimo.count(),"\n" # Conferindo se todas as linhas foram importadas por completo
      "Total de colunas: ", len(df_amostra_ultimo.columns)) 
df_amostra_ultimo.printSchema()


print("Esquema amostra de Julho:\n"
      "Total de linhas: ", df_amostra_anterior.count(),"\n" # Conferindo se todas as linhas foram importadas por completo
      "Total de colunas: ", len(df_amostra_anterior.columns)) 
df_amostra_anterior.printSchema()


# ---
# ### Criando um novo DF, onde só iram ser acrescentados apenas os novos dados unicos!
# - Observe que todos os valores por colunas precisam ser estritamente iguais para **não ser adicionado** ao DF.
# - Casos necessario poderiamos considerar apenas 'N' colunas para fazer essa comparação.

# In[45]:


Rotina_mensal = df_amostra_anterior.union(df_amostra_ultimo).distinct()

#Rotina_mensal.show()


# ### Exportando o resultado para um csv e conferindo a disponibilidade / armazenamento

# In[46]:


Rotina_mensal.toPandas().to_csv('Warehouse/Rotina_mensal.csv', index=False)
Rotina_mensal_pd = pd.read_csv(r'Warehouse/Rotina_mensal.csv')
Rotina_mensal_pd


# ### Por conta do tamanho do arquivo de saida, resolvi zipar o arquivo para subir no git depois, tendo em vista que o tamanho maximo para upload no git é de 25mb.

# In[37]:


from zipfile import ZipFile

with ZipFile("Warehouse/Rotina_mensal.zip", "w") as zip:
    zip.write("Warehouse/Rotina_mensal.csv")
    zip.close()


# ### Ainda assim o arquivo apresentava um tamanho maior que 25mb.
# 
# ![imagem](Warehouse/tamanho.png)
# 
# #### Por conta do tempo limitado, não encontrei uma solução satisfatoria para splitar o .zip por linha de comando. Então resolvi compactar manualmente para .rar e subir no github
# 
# ---

# ### linhas repetidas não importadas na amostra reduzida:
# - Conferindo a quantidade de linhas que não foram importadas.
# - Como fazlei anteriormanete, observe que todos os valores por colunas precisam ser estritamente iguais para **não ser adicionado** ao DF.

# In[47]:


print("Total de linhas repetidas e não adicionadas: ",
      df_amostra_ultimo.count()+df_amostra_anterior.count()-Rotina_mensal_pd.shape[0])


# ## Essa rotina deve rodar automaticamente todos os meses, escolha a forma que preferir para essa atividade.
# 
# 1. Alternativa para isso é ultilizar o agendador de tarefas do proprio windows:
# > Salvando esse notebook em formado .py (python) e agendando as configurações para executar mensalmente.
# - Mas essa solução só seria local!
# 
# 2. Dessa forma busquei ultilizar um job do GCP para auxiliar no agendamento.
# 

# In[ ]:




