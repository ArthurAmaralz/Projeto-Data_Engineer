# Desafio - Data_Engineer
Resolução dos desafios:
- [Dados de Agosto](Resultado_Desafio/Desafio_mes_agosto.ipynb)

### Objetivo: 

`O presente projeto tem como objetivo simular requisições referentes a 3 meses distintos para extração, validação, tratamento, analise e persistencia de dados.`


![1591992418037](https://user-images.githubusercontent.com/61892694/198920194-af876621-dda5-46e4-b426-c12d9d75df71.png)


### Preparação e escolha do ambiente:

A principio iria fazer todo o processo pela minha VM linux/Ubuntu, pois é onde eu já tenho todo o ambiente com spark e o ecossistema hadoop.
Para minha infelicidade, vulgo lei de murphy em ação, encontrei diversos problemas na hora de importar os dados, tanto atravez de API ( que não conseguia exportar os dados online ) como permissões de transferência e acesso aos arquivos baixados localmente na comunicação da minha maquina ( windows ) com a VM Ubuntu.

O cenário ideal seria realizar todo o desafio em Cloud, mas por ter tido uma péssima experiência financeira com a AWS ( Sim amigos, eu deixei todos meu ambiente AWS + databricks rodando 24/7 por 2 semanas $$$$$ ) e estava com receio de ter os mesmo custos com a Azure ou GCP, principalmente por conta do volume de dados (Só o EPD_202208.csv = 6,4GB) e precisar de um cluster mais potente para processamento, tornando mais $$$ que minha experiencia anterior.

>P.s.: Também pensei em utilizar no formato .ZIP ( 586GB ), mas ainda iria ser onoroso para os processamentos e transformações...
>Por isso no final optei por utilizar o Jupyter (Anaconda) localmente junto de seus recursos para seguir todos o processo .

### Para realizar as atividades propostas:

Todo processamento foi realizado local e ultilizando os 
framework apache spark e a biblioteca do pandas, juntamente de SQL

A alternancia entre Pyspark e Pandas se deu pelo seguinte motivo:
- Pyspark : para importação e processamento dos grandes volumes de dados
- Pandas: para manipulacao, exportação e melhor vizualização dos dados


### Atividades:

1.	Realize a extração dos dados dos 3 ultimos meses de prescrição (english-prescribing-data-epd).
2.	Crie um processo para validação dos dados extraídos.
3.	Persista os dados da forma que achar melhor. Exemplo: arquivos, mysql, postgreSQL, sqlite, mongodb, delta, store em cloud, etc.
4.	Gere scripts que atendam as solicitações abaixo
    * Crie um dataframe contendo os 10 principais produtos químicos prescritos por região. 
    * Quais produtos químicos prescritos tiveram a maior somatória de custos por mês? Liste os 10 primeiros. 
    * Quais são as precrições mais comuns? 
    * Qual produto químico é mais prescrito por cada prescriber?
    * Quantos prescribers foram adicionados no ultimo mês? 
    * Quais prescribers atuam em mais de uma região? Ordene por quantidade de regiões antendidas.
    * Qual o preço médio dos químicos prescritos em no ultimo mês coletado?
    * Gere uma tabela que contenha apenas a prescrição de maior valor de cada usuário.
 5. Faça uma rotina que mensalmente colete os dados do ultimo mes e adiciona apenas os dados que sejam novos. Essa rotina deve rodar automaticamente todos os meses, escolha a forma que preferir para essa atividade. `Rotina concluida com soluções tecnicas alternativas, porpulamente conhecida por Gambiarra`

----
Arthur Amaral de Lima --- [Linkedin](https://www.linkedin.com/in/arthuramaral-py/) --- arthur.absens@gmail.com
