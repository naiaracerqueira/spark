## ATENÇÃO!

### 1) Lazy Evaluation

O Spark não faz nada quando você manda ele fazer filter(), select(), groupBy().

Tudo só é executado quando você chama uma **𝗮𝗰𝘁𝗶𝗼𝗻** como count(), show(), write().

O Spark olha tudo que você escreveu e antes de executar, o Catalyst Optimizer analisa o plano inteiro e decide a melhor forma de rodar. Ele pode reordenar filtros, eliminar colunas desnecessárias, combinar operações. Tudo antes de mover um byte de dado. Se o Spark executasse cada linha imediatamente, como um Pandas, você perderia todas essas otimizações.

Na prática isso significa:
• Escrever .filter() antes do .join() não é obrigatório — o Catalyst já faz isso por você
• Encadear várias transformações não custa nada — elas viram um único plano otimizado
• O custo real só aparece na action

### 2) SHUFFLE

Nem toda transformação no Spark custa igual: um filter(), select(), map() lê a partição, aplica a transformação e pronto. Cada partição trabalha sozinha. Sem depender das outras. Sem conversa entre executores. Isso é uma **𝗻𝗮𝗿𝗿𝗼𝘄 𝘁𝗿𝗮𝗻𝘀𝗳𝗼𝗿𝗺𝗮𝘁𝗶𝗼𝗻**, são baratos, paralelizáveis, rápidos.

Quanto você tem groupBy(), orderBy(), distinct() e join(). O Spark precisa juntar dados de partições diferentes para agregar, ou seja, mover dados entre executores pela rede. Isso é uma **𝘄𝗶𝗱𝗲 𝘁𝗿𝗮𝗻𝘀𝗳𝗼𝗿𝗺𝗮𝘁𝗶𝗼𝗻** e gera **𝘀𝗵𝘂𝗳𝗳𝗹𝗲**.

O **𝘀𝗵𝘂𝗳𝗳𝗹𝗲** ocorre quando o Spark precisa redistribuir dados entre os nós do cluster. Para garantir que linhas com a mesma chave terminem no mesmo nó físico, o Spark realiza uma transferência intensa de dados pela rede. O shuffle exige etapas extremamente custosas para o hardware:
- Shuffle Write: Cada executor precisa escrever dados no disco para que outros possam lê-los.
- Shuffle Read: Os executores leem esses arquivos via rede.
- Serialização: Os dados precisam ser transformados em um formato transferível, consumindo CPU.
- Tráfego de Rede: O gargalo físico da comunicação entre as máquinas.

Antes de sair rodando qualquer transformação, é vital avaliar se aquela operação custosa é realmente necessária para o seu resultado final. Muitas vezes, um filtro aplicado mais cedo, remover um distinct desnecessário ou uma mudança na estratégia de particionamento pode evitar esse caos de leitura e escrita em disco.


