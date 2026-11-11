# Índice posting+edges: notas para el diseño de la versión en disco

**Estado:** semilla del design doc de disco. Por ahora tiene una sola sección, la que dejó decidida el benchmark en memoria (v1.2, Fase B, 2026-11-11). El resto del diseño de disco no está escrito aquí.

Las cifras son de `bench-report.md`, sección "La celda desacoplada": dbpedia-openai 1M (1536d), S=236, P=256, 1 thread, todo en RAM. Son cuentas de trabajo y recall, que no dependen de dónde vivan los postings; los tiempos de I/O no están medidos.

## Refinar con el sidecar las estimaciones del scan

**Decisión (2026-11-11): no se construye para el modo memoria; se propone para el modo disco.**

### Por qué interesa en disco

En disco el posting es lo que se lee, y su tamaño lo manda el código de cada miembro. Con códigos de 256 bits (40 B con la cabecera) un posting pesa cinco veces menos que con el código completo de 1536d (200 B). El routing, en cambio, necesita los bits: con 256 bits para todo, las cargas a igual recall son 3.0x las del control.

La disposición que separa las dos cosas ya existe (`UserConfig.ScanCodeBits`): códigos cortos en el posting para el scan, y el código completo en el sidecar residente para ordenar los postings por sus endpoints. Es la "celda desacoplada" del informe.

### Lo medido sin refinar

Cargas de posting por query a igual recall, contra el control de código completo (rescoreLimit 3200 para sacar del medio el corte final, sin margen):

| celda | recall 0.90 | recall 0.95 | recall 0.99 |
|---|---|---|---|
| 256 bits para todo | 1.98x | 3.02x | 3.10x |
| scan a 256 bits, sidecar completo | 1.17x | 1.47x | 1.50x |

Techo de recall por rescoreLimit, subiendo solo ef:

| rescoreLimit | 64 | 200 | 400 | 800 | 1600 | 3200 |
|---|---|---|---|---|---|---|
| scan a 256 bits, sidecar completo | 0.788 | 0.911 | 0.952 | 0.974 | 0.987 | 0.994 |
| código completo (control) | 0.996 | 0.996 | — | — | — | — |

Darle al routing el código completo recupera tres cuartos de las cargas de más. Lo que queda son 1.5x cargas y un techo que obliga a rescorear miles de vectores exactos para llegar a 0.99. En disco cada uno de esos es una lectura al store.

### Las dos causas que quedan

Las dos vienen de usar la estimación del scan como distancia de los candidatos.

1. **La parada, hasta recall 0.97.** El orden en que se cargan los postings ya es el del control: sale de las estimaciones de los endpoints, que son del sidecar. Lo que cambia es dónde para cada query. El umbral de parada es la ef-ésima mejor estimación del scan, con el ruido de 256 bits, y se compara con estimaciones del sidecar. Las cargas se reparten peor entre queries: con la misma mediana de 9 cargas, el control saca recall 0.950 (media 10.1, p95 21) y esta celda 0.932 (media 11.4, p95 29). El corte final no lo explica: doblar el rescoreLimit de 1600 a 3200 mueve el recall 0.0006 como mucho hasta ef=80.
2. **El corte final, desde recall 0.98.** Los candidatos que pasan al rescore exacto se eligen por su estimación de scan. Pasar de rescoreLimit 1600 a 3200 suma 0.0022 de recall a ef=320 y 0.0044 a ef=1024.

Un efecto de la segunda: pasado el techo, subir ef baja el recall (de 0.9516 a 0.9397 con rescoreLimit 400). Más vectores escaneados compiten por los mismos puestos del rescore con estimaciones ruidosas.

### El arreglo

El mismo para las dos: re-estimar con el código completo del sidecar los mejores candidatos del scan antes de que entren en el umbral de parada y en el corte final.

- **Qué se refina.** Tras escanear un posting, los miembros cuya estimación de scan queda dentro de una banda sobre el peor candidato que hoy entraría en el umbral o en el pool del rescore. El resto se descarta con la estimación corta, como ahora.
- **Con qué.** El código completo del miembro en el sidecar, direccionado por slot. Es la misma lectura que ya se hace para cada endpoint de una arista: está residente, no es I/O.
- **Qué deja igual.** El formato del posting en disco (códigos cortos), el routing, las aristas y el rescore exacto final.
- **El knob nuevo.** El ancho de la banda, en unidades de la distancia. Demasiado estrecha deja fuera vecinos verdaderos con mala estimación corta; demasiado ancha refina de más.

### Qué se espera

- **Cargas:** las del control. Con el umbral y el pool sobre estimaciones de código completo, la búsqueda para donde para el control, y el rescoreLimit vuelve al orden de 64.
- **Bytes leídos por query:** hasta cinco veces menos que con el código completo en el posting, a igual número de cargas.
- **Coste nuevo:** una lectura al sidecar por candidato refinado. En RAM un endpoint puntuado cuesta entre 75 y 84 ns.
- **En memoria no compensa,** y por eso no se construye ahí: el scan es entre el 16 y el 19 % de la query y el código corto lo abarata de 54 a 37–50 ns por vector, así que la ganancia tiene un techo del 6 %, con un 14 % más de memoria por guardar dos códigos.

### Qué hay que medir antes de darla por buena

1. Recall a igual número de cargas contra el control de código completo, en ef de 10 a 1024. Sin refinar la diferencia va de -0.038 (8 cargas) a -0.002 (86 cargas); el objetivo es que quede dentro del ruido.
2. Techo de recall con rescoreLimit 64: tiene que alcanzar 0.99 subiendo solo ef, que es el gate del default.
3. Cuántos candidatos se refinan por query en función de la banda, que es el coste.
4. Que subir ef no baje el recall en ningún tramo.
5. El ancho del código de scan. Solo está medido 256 bits; 512 bits (72 B) es el otro candidato, con menos ruido que corregir y menos ahorro por carga.
