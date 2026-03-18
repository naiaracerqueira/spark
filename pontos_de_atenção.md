## ATENÇÃO!

### 1) Lazy Evaluation

O Spark não faz nada quando você manda ele fazer algo.

Você escreve .filter(), .select(), .groupBy()... e o Spark simplesmente anota.

Nada é executado. Só quando você chama uma 𝗮𝗰𝘁𝗶𝗼𝗻 (count(), .show(), .write()).

O Spark olha tudo que você escreveu, monta um plano de execução e aí sim começa a trabalhar.

Isso tem um nome: 𝗹𝗮𝘇𝘆 𝗲𝘃𝗮𝗹𝘂𝗮𝘁𝗶𝗼𝗻.

E não é um bug. É uma das features mais importantes do Spark.

𝗣𝗼𝗿 𝗾𝘂𝗲?

Porque antes de executar, o Catalyst Optimizer analisa o plano inteiro e decide a melhor forma de rodar.

Ele pode reordenar filtros, eliminar colunas desnecessárias, combinar operações. Tudo antes de mover um byte de dado.

Se o Spark executasse cada linha imediatamente, como um Pandas, você perderia todas essas otimizações.

Na prática isso significa:
• Escrever .filter() antes do .join() não é obrigatório — o Catalyst já faz isso por você
• Encadear várias transformações não custa nada — elas viram um único plano otimizado
• O custo real só aparece na action

### 2) SHUFFLE

Nem toda transformação no Spark custa igual. Você escreve .filter(). O Spark lê a partição, aplica o filtro, pronto. Cada partição trabalha sozinha. Sem depender das outras. Sem conversa entre executores. Isso é uma 𝗻𝗮𝗿𝗿𝗼𝘄 𝘁𝗿𝗮𝗻𝘀𝗳𝗼𝗿𝗺𝗮𝘁𝗶𝗼𝗻.

Agora você escreve .groupBy().agg(). O Spark precisa juntar dados de partições diferentes para agregar. Isso significa mover dados entre executores pela rede. Isso é uma 𝘄𝗶𝗱𝗲 𝘁𝗿𝗮𝗻𝘀𝗳𝗼𝗿𝗺𝗮𝘁𝗶𝗼𝗻 e gera 𝘀𝗵𝘂𝗳𝗳𝗹𝗲. O shuffle é a operação mais cara do Spark. Serializa dados, move pela rede, desserializa do outro lado.

𝗣𝗼𝗿 𝗾𝘂𝗲 𝗶𝘀𝘀𝗼 𝗶𝗺𝗽𝗼𝗿𝘁𝗮 𝗻𝗮 𝗽𝗿á𝘁𝗶𝗰𝗮?
• Narrow: .filter(), .select(), .map() — baratos, paralelizáveis, rápidos
• Wide: .groupBy(), .join(), .distinct() — custosos, geram shuffle, criam stage boundaries

Quando você entende a diferença, começa a escrever transformações de forma diferente:

Filtra antes de fazer join. Reduz o volume de dados antes do shuffle.

Evita .distinct() desnecessário.


### 3) withColumn()

Many people use withColumn() to add or modify columns in a DataFrame. But when used multiple times, it can make your transformation plan unnecessarily complex.
Example:
df = df.withColumn("a", expr("x + 1"))
df = df.withColumn("b", expr("a * 2"))
df = df.withColumn("c", expr("b + 10"))

Each withColumn() adds another step to the logical plan.
While Spark’s optimizer helps, chaining many withColumn() calls can still make the execution plan longer and harder to optimize.

A cleaner alternative in many cases is using select().
Example:
df = df.select(
 "*",
 expr("x + 1").alias("a"),
 expr("(x + 1) * 2").alias("b"),
 expr("((x + 1) * 2) + 10").alias("c")
)

Here we create multiple columns in one transformation, which keeps the plan simpler.
In simple terms:
- withColumn() → good for adding or modifying a single column
- select() → better when creating multiple columns at once
Small design choice, but it can help keep your Spark pipelines cleaner and easier to optimize.


