# Pipeline de Engenharia de Dados e BI: Consolidação em Larga Escala

> **Nota de Confidencialidade:** Este repositório contém uma versão adaptada de um projeto real que desenvolvi. Para respeitar integralmente os acordos de confidencialidade (NDA) da empresa contratante, todos os dados, nomes e informações estratégicas foram substituídos por valores genéricos. A lógica de programação, a arquitetura do código e os desafios técnicos resolvidos aqui, no entanto, refletem exatamente o meu trabalho.

## Contexto

Construir um pipeline de dados do zero sempre traz desafios interessantes, e este projeto nasceu para resolver uma dor clássica de operações descentralizadas. O objetivo era claro: como consolidar de forma automatizada e confiável os dados de produção que chegam todos os meses de 24 unidades totalmente independentes?

O resultado desse esforço foi um pipeline de ETL desenhado em **Python**, criado não apenas para processar dados, mas para transformar planilhas completamente despadronizadas em inteligência de negócio no **Power BI**.

## Desafio: O Caos das Planilhas

O cenário inicial era complexo. Todos os meses, recebíamos dezenas de arquivos Excel por e-mail, separados por abas referentes a cada setor da operação. O grande problema é que, embora existisse um modelo "ideal" de preenchimento, o fator humano gerava bases praticamente impossíveis de cruzar via automação simples:

* **Sem âncoras fixas:** As tabelas começavam em linhas diferentes a cada mês e as colunas mudavam de ordem frequentemente.


* **Ruído nos dados:** Linhas de "Total Geral", "Subtotal" e até espaços para assinaturas ficavam perdidas no meio das linhas de dados brutos.


* **Células Mescladas:** O uso estético do Excel quebrava a lógica de leitura estruturada, ocultando dados ao serem importados pelo código.


* **Criatividade na Nomenclatura:** Os usuários utilizavam uma infinidade de abreviações e erros ortográficos para descrever exatamente o mesmo serviço.

## Como resolvi isso (A Arquitetura)

Para colocar ordem na casa, desenvolvi um sistema focado em resiliência estrutural e aprendizado contínuo, dividido em três grandes frentes:

### 1. Extração Dinâmica e Limpeza

Em vez de programar o script para ler posições fixas de células (o que faria o código quebrar logo no mês seguinte), apliquei lógicas de localização dinâmica:

* O algoritmo varre as primeiras 25 linhas de cada arquivo até encontrar o verdadeiro cabeçalho da tabela, ignorando qualquer texto estético que esteja acima.


* Para lidar com as células mescladas, implementei o método de `forward fill`, que preenche de forma inteligente as linhas vazias geradas pela mesclagem, propagando a informação correta para baixo e garantindo a precisão dos cálculos numéricos.


* Antes de carregar os dados, um filtro varre a base procurando termos como `"TOTAL GERAL"` ou `"RESUMO"` e exclui essas linhas, evitando que totalizadores entrem na conta como dados novos.



### 2. Normalização Inteligente (Fuzzy Matching)

Esse foi o coração do projeto. Para resolver os erros de digitação e a falta de padronização, utilizei a biblioteca `RapidFuzz` cruzada com um dicionário oficial de nomenclaturas ("Gold Standard"):

* Se o termo da planilha tiver $92\%$ ou mais de similaridade com o termo oficial, o sistema corrige silenciosamente.


* Se a similaridade for baixa, o script pausa e exibe um **Menu Interativo** no terminal, onde eu posso confirmar uma sugestão aproximada, mapear um novo termo ou ignorar aquela linha .


* O mais interessante dessa etapa é que a decisão humana é salva em um arquivo `aprendizado.json`. Na rodada seguinte, o sistema consulta essa "memória" e resolve a inconsistência sozinho.



### 3. Estruturação Analítica

No fim do processamento, transformo aquele formato horizontal bagunçado em um modelo tabular limpo. Para garantir a rastreabilidade, cada linha ganha metadados (Código da Unidade, Origem do Arquivo, Setor). Para evitar perda de histórico, o lote processado é sempre salvo com um identificador único de execução (Ex: *Rodada 45921*).

## Impacto

A base consolidada passou a alimentar um modelo semântico diretamente no **Power BI**. Com essa automação, o impacto foi muito além de economizar horas de trabalho manual:

* Passamos a ter visibilidade real da operação.


* Tornou-se possível cruzar o volume de produção com os recursos alocados, identificando gargalos, ociosidade de equipes e otimizando processos.


* Graças à arquitetura do código, o pipeline é escalável. Adicionar uma nova unidade ao processo exige apenas atualizar o diretório de entrada, sem necessidade de alterar a lógica do motor.



## Ferramentas

* **Linguagem:** Python  (`pandas` para manipulação de dados)


* **Algoritmos de Texto:** `RapidFuzz` (Fuzzy String Matching)


* **Armazenamento de Estado:** Manipulação de arquivos JSON (`aprendizado.json`)


* **Data Visualization:** Microsoft Power BI


