# CNC Guard — Nombres y datos del proyecto
## PC anfitrión (donde furrula Docker)

| | |
|---|---|
| IP | `192.168.75.211` |
| Carpeta del proyecto | `C:\Users\garcia.oier\cnc-guard` (clon del repo de GitHub) |
| Repo GitHub | https://github.com/AABD-Ethazi-2627/T5-erronka1 |

## Acceso desde el navegador (tu PC o compañeros)

| Servicio | URL | Usuario / contraseña |

| Editor web (carpeta compartida + terminal) | http://192.168.75.211:8444 | contraseña: `cncguard2026` |
| NiFi | https://192.168.75.211:8443/nifi | `admin` / `adminpassword123` |
| Kibana | http://192.168.75.211:5601 | — |
| Elasticsearch | http://192.168.75.211:9200 | — |
| Scoring API (fase 3) | http://192.168.75.211:8444/proxy/8000/docs | token: `admin` (en `.env`, `API_TOKEN`) |
| Kafka (desde Python en otro PC) | `192.168.75.211:29092` | — |

## Direcciones DENTRO de Docker (NiFi, Filebeat, API...)

| Servicio | Dirección |
|---|---|
| Kafka | `kafka:9092` |
| Elasticsearch | `http://elasticsearch:9200` |
| Scoring API | `http://scoring-api:8000/predict` |

> Dentro de Docker no se usa `localhost` ni la IP del PC: se usa el nombre del servicio.

## Contenedores

| Contenedor | Imagen |
|---|---|
| `cnc-kafka` | apache/kafka:3.9.0 |
| `cnc-filebeat` | elastic filebeat:9.1.4 |
| `cnc-nifi` | apache/nifi:2.4.0 |
| `cnc-elasticsearch` | elasticsearch:9.1.4 |
| `cnc-kibana` | kibana:9.1.4 |
| `cnc-scoring` | build `./scoring` (fase 3, falta `Dockerfile`) |
| `cnc-editor` | linuxserver/code-server |

## Datos del simulador (servidor del profe)

| | |
|---|---|
| Carpeta | `\\192.168.75.150\cnc-stream` (la IP ha cambiado alguna vez: antes `.222`) |
| Usuario / contraseña | `cncuser` / `cncuser` |
| Ficheros | `cnc_batch_AAAAMMDD_HHMMSS_NNNNN.csv`, uno cada 30 s, 10 lecturas cada uno |
| Dentro de Filebeat | `/mnt/cnc-stream` (volumen `cnc-stream-cifs`) |

## Kafka

| Topic | Particiones | Quién escribe → quién lee |
|---|---|---|
| `cnc-sensors-raw` | 3 (RF=1) | Filebeat → NiFi |

| Consumer group | Uso |
|---|---|
| `nifi-cnc-ingest` | NiFi fase 1 → índice `cnc-sensors` |
| `nifi-cnc-scoring` | NiFi fase 3 → API → índice `cnc-predictions` |

> Dos grupos distintos para que las dos fases reciban **todos** los mensajes.

## Elasticsearch — índices

| Índice | Contenido | Mapping |
|---|---|---|
| `cnc-sensors` | Lecturas de sensores (fase 1) | `elasticsearch/cnc-sensors-mapping.json` |
| `cnc-predictions` | Predicciones del modelo (fase 3) | — |

## Modelo (fase 2)

| | |
|---|---|
| Fichero | `models/cnc_failure_model.pkl` |
| Ruta en el contenedor de la API | `/models/cnc_failure_model.pkl` (`MODEL_PATH`) |
| Versión de scikit-learn | **1.5.0** (la misma que la API) |
| FEATURES (en este orden) | `air_temp_K, process_temp_K, rpm, torque_Nm, tool_wear_min` |

## Formato de una línea (14 campos)

```
timestamp,machine_id,machine_type,air_temp_K,process_temp_K,rpm,torque_Nm,tool_wear_min,machine_failure,failure_TWF,failure_HDF,failure_PWF,failure_OSF,failure_RNF
2026-04-29T10:57:50Z,CNC-039,L,299.0,308.6,1587,38.6,124,0,0,0,0,0,0
```

## Niveles de alerta (fase 3)

| Nivel | Probabilidad de fallo |
|---|---|
| OK | < 50 % |
| KONTUZ | 50–80 % |
| LARRIA | > 80 % |

## Reglas del cortafuegos (PC anfitrión)

| Regla | Puertos |
|---|---|
| CNC Guard | 8443, 5601, 9200, 29092 |
| CNC Guard - Editor | 8444 |
| CNC Guard - Ping | ICMP |

## Documentación

| Fichero | Contenido |
|---|---|
| `docs/nombres.md` | Esta hoja |
| `docs/galderak.md` | Respuestas a las preguntas G1–G12 de la erronka |
| `docs/filebeat.md` | Filebeat: configuración, comandos y problemas resueltos |
| `docs/kafka-comandos.md` | Chuleta de comandos de Kafka |
| `docs/AAE_ereduak.md` | Documento teórico de AAE (euskera) |