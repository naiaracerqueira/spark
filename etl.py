from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicialização da sessão do Spark
spark = SparkSession.builder.appName("ExemploVendas").getOrCreate()

# Carregar dados de um arquivo CSV de vendas
caminho_arquivo_vendas = "teste_csv_normal.csv"
dados_vendas = spark.read.csv(caminho_arquivo_vendas, header=True, inferSchema=True)

# Mostrar os primeiros registros para verificar se os dados foram carregados corretamente
dados_vendas.show()


# Configurações de acesso ao banco de dados
url_banco = "jdbc:mysql://localhost:3306/seu_banco"
propriedades = {
    "user": "USER",
    "password": "PASSWORD",
    "driver": "com.mysql.jdbc.Driver"
}

# Consulta ao banco de dados para carregar dados dos clientes
consulta_clientes = "(SELECT * FROM tabela_clientes) clientes"
dados_clientes = spark.read.jdbc(url_banco, table=consulta_clientes, properties=propriedades)

# Mostrar os primeiros registros para verificar se os dados foram carregados corretamente
dados_clientes.show()

# Remover dados duplicados e preencher valores nulos
dados_vendas_limpos = dados_vendas.dropDuplicates().na.fill(0)

# Selecionar apenas colunas relevantes (exemplo: ID do cliente, produto e valor da venda)
dados_vendas_filtrados = dados_vendas_limpos.select("id_cliente", "produto", "valor_venda")

# Filtrar vendas com valores maiores que $100
dados_vendas_filtrados = dados_vendas_filtrados.filter(dados_vendas_filtrados["valor_venda"] > 100)

# Mostrar os resultados das transformações
dados_vendas_filtrados.show()


# Mapear dados: converter tipo de uma coluna e renomear coluna
dados_clientes_mapeados = dados_clientes.withColumn("idade", F.col("idade").cast("integer")) \
    .withColumnRenamed("cidade", "localidade")

# Agrupar por localidade e contar o número de clientes em cada localidade
clientes_agrupados = dados_clientes_mapeados.groupBy("localidade").agg(F.count("id_cliente").alias("num_clientes"))

# Mostrar os resultados das transformações
clientes_agrupados.show()

clientes_agrupados.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://<seu_host>:<sua_porta>/seu_banco") \
    .option("dbtable", "dados_vendas_filtrados") \
    .option("user", "seu_usuario") \
    .option("password", "sua_senha") \
    .save()

