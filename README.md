[eventos_de_fluxo.csv.gz](https://github.com/startxbra/spark_scala_shell/files/7150806/eventos_de_fluxo.csv.gz)
[concorrentes.csv](https://github.com/startxbra/spark_scala_shell/files/7150808/concorrentes.csv)
[bairros.csv](https://github.com/startxbra/spark_scala_shell/files/7150809/bairros.csv)
[populacao.json.csv](https://github.com/startxbra/spark_scala_shell/files/7150822/populacao.json.csv)
# spark_scala_shell
spark_scala_shell Test ETL, Transform, DataFrame

===================================================================================
export SPARK_HOME=/u01/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

spark-shell

scala> sc.version
res10: String = 3.1.1

--Create DataFrame
var bairrosDF = spark.read.option("header","true").csv("/u01/files/bairros.csv")

var concorrentesDF = spark.read.option("header","true").csv("/u01/files/concorrentes.csv")

var eventos_de_fluxoDF = spark.read.option("header","true").csv("/u01/files/eventos_de_fluxo.csv.gz")

var populacaoDF = spark.read.option("header","true").json("/u01/files/populacao.json")

--Transform Object
eventos_de_fluxoDF = eventos_de_fluxoDF.withColumn("Hour",hour($"datetime")).
|withColumn("Min",minute($"datetime")).
|withColumn("Sec",second($"datetime")).
|withColumn("monthofyear",date_format($"datetime","M/L")).
|withColumn("Dayofweek",dayofweek($"datetime")).
|withColumn("DayofMonth",date_format($"datetime","d")).
|withColumn("DayofYear",date_format($"datetime","D")).
|withColumn("hourofday",date_format($"datetime","H")).
|withColumn("minuteofhour",date_format($"datetime","m")).
|withColumn("dayofweekFull",date_format($"datetime","EEEE")).
|withColumn("Periodo", expr("case when hour < '12' then 'Morning' " +
                "when hour < '18' then 'Afternoon' " +
                       "else 'Night' end"))

--Create Views
eventos_de_fluxoDF.createOrReplaceTempView("Ve")
populacaoDF.createOrReplaceTempView("Vp")
bairrosDF.createOrReplaceTempView("Vb")
concorrentesDF.createOrReplaceTempView("Vc")

--Final Query
spark.sql("select c.codigo cod_concorrente, c.nome nome_concorrente, c.endereco, cast(c.faixa_preco as decimal(9,2)) preco_praticado, e.dayofweekFull diaSemana, e.periodo, cast(mean(e.hour) as decimal (9,2))as AvgDia, cast(mean(e.dayofweek) as decimal (9,2)) as AvgSemana, b.area bairro, p.populacao, cast(p.populacao/b.area as decimal (13)) densidade from Vc as c inner join Vb as b inner join Ve as e inner join Vp as p group by c.nome, c.codigo , c.endereco, c.faixa_preco, b.area, e.dayofweekFull, e.periodo, p.populacao")


--Export Json format

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val resultDF = sqlContext.sql("select c.codigo cod_concorrente, c.nome nome_concorrente, c.endereco, cast(c.faixa_preco as decimal(9,2)) preco_praticado, e.dayofweekFull diaSemana, e.periodo, cast(mean(e.hour) as decimal (9,2))as AvgDia, cast(mean(e.dayofweek) as decimal (9,2)) as AvgSemana, b.area bairro, p.populacao, cast(p.populacao/b.area as decimal (13)) densidade from Vc as c inner join Vb as b inner join Ve as e inner join Vp as p group by c.nome, c.codigo , c.endereco, c.faixa_preco, b.area, e.dayofweekFull, e.periodo, p.populacao")


resultDF.write.
json("/tmp/solution.json")
