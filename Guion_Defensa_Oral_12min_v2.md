# Guion de la defensa oral (12 minutos + 3 de preguntas)

**Proyecto final integrador · Big Data · Maestría en Inteligencia Artificial · Universidad de Palermo · Grupo 4**
Procesamiento distribuido y analítica de accidentes viales en EE. UU. (2016–2023)

Corresponde a `Presentacion_Defensa_Oral.pptx` (10 diapositivas). Todas las cifras salen de la ejecución del notebook `BIG-DATA-GRUPO-4-Colab.ipynb` incluida en el repositorio.

## Reparto de tiempos

| Expositor | Diapositivas | Tiempo | Contenido |
|---|---|---|---|
| Juan Diego Weber | 1 a 3 | 00:00 – 04:00 | Portada, dataset y entorno, consultas 3 y 4 |
| Diego Diamanti | 4 y 5 | 04:00 – 08:00 | Consultas 6, 8 y 10 |
| Melisa Maldonado | 6 a 10 | 08:00 – 12:00 | Machine learning, benchmarking, conclusiones y cierre |
| Todo el grupo | — | 12:00 – 15:00 | Preguntas del docente |

---

## Bloque 1 · Juan Diego Weber · 00:00 – 04:00

### Diapositiva 1 · Portada · 00:00 – 00:45

> "Buenas tardes. Somos el Grupo 4: Diego Diamanti, Melisa Maldonado y yo, Juan Diego Weber. Vamos a presentar nuestro proyecto final sobre procesamiento distribuido de accidentes viales en Estados Unidos entre 2016 y 2023.
>
> Trabajamos con más de 7,7 millones de registros y tuvimos dos objetivos. El primero, comparar Pandas en un solo nodo con Apache Spark en las cinco consultas que nos asignaron. El segundo, entrenar un modelo de clasificación con Spark MLlib."

### Diapositiva 2 · Dataset, almacenamiento y entorno · 00:45 – 02:15

> "El dataset tiene 7.728.394 registros y 46 atributos, y en CSV pesa unos 2,9 GB. Leer ese CSV completo en memoria es lento y arriesga quedarse sin RAM.
>
> Por eso lo convertimos a Parquet con compresión ZSTD y bajó a 491 MB, un 83% menos. Además, ordenamos físicamente los datos por latitud y longitud. Así, cada bloque del archivo agrupa zonas geográficas cercanas y guarda su mínimo y su máximo. En un filtro espacial, el lector puede descartar bloques enteros sin leerlos: eso es el *predicate pushdown*.
>
> Todo corrió en una notebook con Intel Core i5-1135G7, de 4 núcleos y 8 hilos, con 16 GB de RAM y Windows 11. Usamos Python 3.12, PySpark 3.5.0 y OpenJDK 17. El mismo notebook también funciona en Google Colab."

### Diapositiva 3 · Consultas 3 y 4 · 02:15 – 04:00

> "En la Consulta 3 filtramos los siniestros dentro de un radio de 25 km con la fórmula de Haversine. Encontramos 361.038 en Los Ángeles y 269.717 en Nueva York, y Pandas y Spark dieron exactamente el mismo resultado.
>
> La Consulta 4, común a todos los grupos, pide los cinco puntos más peligrosos y definir el criterio. Usamos dos:
>
> - **Enfoque A**, de gravedad: contamos solo los siniestros con severidad 3 o 4 en celdas de unos 2 km, a nivel nacional. El primer lugar es Downey, en Los Ángeles, con 1.619. Le siguen el George Washington Bridge, en Nueva York, con 1.550, y tres celdas del conector I-75/I-85 en Atlanta, con entre 1.301 y 1.432.
> - **Enfoque B**, de densidad: contamos todos los siniestros en celdas de unos 5 km en California. El máximo es el centro de Los Ángeles, con 50.254.
>
> Calcular la distancia de cada punto contra todos los demás sería un problema cuadrático que no entra en memoria. La grilla lo reduce a un agrupamiento lineal.
>
> Le paso la palabra a Diego."

---

## Bloque 2 · Diego Diamanti · 04:00 – 08:00

### Diapositiva 4 · Consultas 6 y 8 · 04:00 – 06:00

> "Gracias, Juan Diego. En la Consulta 6, la columna Street trae la misma autopista escrita de muchas formas, por ejemplo 'I-95 N' e 'I-95 S'. La normalizamos con una expresión regular que extrae el código I-número o US-número.
>
> Después medimos dos cosas:
>
> - **La tasa de gravedad**, que es el porcentaje de siniestros graves sobre el total, con un mínimo de 100 siniestros por autopista. Lideran la I-72, con 92,2%, y la I-57, con 81,1%.
> - **El volumen absoluto.** Lidera la I-95, con 62.202 siniestros graves y 95,7 minutos de despeje promedio.
>
> En la Consulta 8 buscamos días en los que un estado superó su media histórica en más de tres desvíos estándar, y encontramos 1.171. Si los ordenamos por volumen, domina California, con 2.823 siniestros el 29 de enero de 2021. Si los ordenamos por rareza, aparecen estados con poco tráfico: Kansas, con 23 desvíos, y Ohio, con 18. Ohio coincide con la tormenta invernal Elliott del 23 de diciembre de 2022, con viento de 26 mph y visibilidad de 1,3 millas. En cambio, Kansas no muestra valores climáticos extremos en el dataset, así que esa anomalía no la explican estas variables."

### Diapositiva 5 · Consulta 10 · 06:00 – 08:00

> "La Consulta 10 compara el tiempo medio de despeje según la luz y el tipo de día.
>
> - En un día hábil con luz solar, el despeje lleva 336 minutos, unas 5,6 horas.
> - En una noche de fin de semana sube a 720 minutos, unas 12 horas.
> - Si además separamos el crepúsculo astronómico, la noche cerrada de fin de semana llega a 773 minutos, casi 13 horas. Es 2,3 veces el valor del día hábil.
>
> Una explicación posible es que fuera del horario hábil hay menos grúas y cuadrillas disponibles. Pero es una hipótesis: el dataset no permite verificarla.
>
> Un detalle de rigor: en esta consulta, Pandas y Spark no incluyen la misma cantidad de registros, 6,97 contra 7,71 millones, porque filtran distinto los registros con datos faltantes. Por eso los minutos difieren entre 1% y 5%; mostramos los de Pandas.
>
> Ahora Melisa presenta el modelo y el benchmarking."

---

## Bloque 3 · Melisa Maldonado · 08:00 – 12:00

### Diapositiva 6 · Planteo del problema de machine learning · 08:00 – 09:15

> "Gracias, Diego. Para la Consigna 6 elegimos la opción de clasificación supervisada: predecir la severidad del siniestro, de 1 a 4, al momento del reporte. Eso serviría para priorizar el despacho de emergencias.
>
> Para evitar fuga de información, excluimos las variables que solo existen después del siniestro: la hora de fin, la duración y la distancia afectada.
>
> El pipeline de pyspark.ml tiene cinco pasos: elimina nulos en los predictores, indexa el estado y la condición día/noche con StringIndexer, los codifica con OneHotEncoder, arma un vector de 54 variables con VectorAssembler y entrena un árbol de decisión con profundidad 10. Esas 54 variables son 5 numéricas, 48 del estado y 1 de día o noche. Usamos maxBins = 60 porque hay 49 estados, y dividimos 80/20 con semilla 42."

### Diapositiva 7 · Resultados del modelo · 09:15 – 10:15

> "El modelo obtuvo 79,8% de exactitud y un F1 ponderado de 0,714, y tardó 284 segundos.
>
> Pero hay que leer esos números con cuidado. La severidad 2 es el 79,7% de todos los registros: si predijéramos siempre severidad 2, tendríamos casi la misma exactitud y un F1 de 0,707. El árbol apenas mejora esa referencia, así que en la práctica predice casi siempre la clase mayoritaria. Por eso nuestra hipótesis no queda confirmada.
>
> Los próximos pasos serían plantear el problema como severidad 3 o más contra el resto, ponderar las clases y reportar ROC-AUC y recall de las clases graves."

### Diapositiva 8 · Benchmarking · 10:15 – 11:15

> "En el benchmarking medimos el tiempo real con perf_counter y forzamos en Spark una acción terminal para que se ejecute el plan completo.
>
> - **Pandas fue más rápido en C3, C4 y C8.** Son filtros y agregaciones simples, y en ellos el costo fijo de Spark (arrancar la JVM, armar el plan y comunicarse con Python) pesa más que el cálculo. En C3, por ejemplo, Pandas tardó 2,5 segundos y Spark 42.
> - **Spark fue más rápido en C6 y C10**, que combinan expresiones regulares y agregaciones sobre los 7,7 millones de filas: 1,94 y 1,44 veces más rápido.
>
> Y si los datos no entraran en memoria, Pandas directamente no podría correr, mientras que Spark sí."

### Diapositivas 9 y 10 · Conclusiones y cierre · 11:15 – 12:00

> "Nos quedamos con cuatro lecciones. El formato de almacenamiento importa: Parquet redujo el tamaño un 83%. No hay una herramienta única: Pandas para filtros simples que entran en memoria, Spark para agregaciones pesadas y para escalar. Conviene mirar cada consulta desde dos perspectivas, volumen y tasa, para no confundir población con riesgo. Y con clases desbalanceadas, una exactitud alta puede no significar nada si no se compara con la clase mayoritaria.
>
> *(Pasar a la diapositiva 10)*
>
> El código está en el repositorio de GitHub que figura en pantalla. Muchas gracias; quedamos abiertos a sus preguntas."

---

## Bloque 4 · Preguntas probables del docente · 12:00 – 15:00

**1. ¿Por qué Pandas fue más rápido en la mayoría de las consultas?** · *Responde Melisa*

> "Porque el dataset en Parquet pesa 491 MB y entra completo en la memoria de un nodo. Spark tiene un costo fijo por consulta: arrancar la JVM, optimizar el plan con Catalyst y pasar datos entre Python y Java por Py4J. En consultas cortas ese costo domina. Cuando la consulta es pesada, como en C6 y C10, Spark ya gana en un solo nodo, y con datos que no entran en RAM sería la única opción."

**2. ¿Por qué una grilla en la Consulta 4 en lugar de Haversine contra todos los puntos?** · *Responde Juan Diego*

> "Comparar 7,7 millones de puntos contra todos implica unos 60 billones de pares, del orden de cientos de terabytes de memoria. Al redondear coordenadas a dos decimales, cada punto cae en una celda de unos 2 km y el problema pasa a ser un agrupamiento lineal, que en Pandas tardó 6,3 segundos."

**3. ¿Cómo evitaron la fuga de información en el modelo?** · *Responde Melisa*

> "Excluimos todo lo que se conoce recién después del siniestro: la hora de fin, la duración y la distancia afectada. El modelo solo usa ubicación, temperatura, humedad, visibilidad, estado y si era de día o de noche."

**4. Si el modelo apenas supera a la clase mayoritaria, ¿sirve?** · *Responde Melisa*

> "Tal como está, no para predecir casos graves. Lo valioso es haberlo detectado comparando contra la referencia correcta. Para que sirva, hay que reformularlo como binario (grave o no grave), balancear las clases y medir el recall de los casos graves, que son los que importan operativamente."

**5. ¿Por qué tanta diferencia entre el día hábil y la noche de fin de semana en la Consulta 10?** · *Responde Diego*

> "El dato es claro: casi 13 horas frente a 5,6. La causa no la podemos demostrar con este dataset. Nuestra hipótesis es la menor disponibilidad de grúas y cuadrillas fuera del horario hábil. Vale aclarar que el crepúsculo casi no cambia el tiempo (346 contra 336 minutos), así que no parece ser solo un tema de luz."
