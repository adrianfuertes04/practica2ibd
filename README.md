# Madrid Sostenible 2030: Infraestructura de Datos

Este proyecto implementa una infraestructura de datos para la iniciativa "Madrid Sostenible 2030" del Ayuntamiento de Madrid. La solución está basada en un enfoque de Data Lake multi-zona que integra múltiples fuentes de información sobre la huella ecológica urbana, patrones de movilidad y calidad ambiental.

## Arquitectura

### Diagrama de la Infraestructura

La infraestructura implementada sigue un patrón de Data Lake con cuatro zonas funcionales:

1. **Zona Raw (Ingesta)**: Almacenamiento de datos en su formato original sin modificaciones.
2. **Zona Process (Procesamiento)**: Datos limpiados, transformados y enriquecidos.
3. **Zona Access (Acceso)**: Datos analíticos listos para consumo por diferentes perfiles de usuario.
4. **Zona Govern (Gobernanza)**: Metadatos, calidad de datos y políticas de seguridad.

El sistema está compuesto por los siguientes componentes:

- **MinIO**: Sistema de almacenamiento compatible con S3 que actúa como repositorio principal del Data Lake.
- **Trino**: Motor de consulta SQL distribuido para analizar datos en MinIO.
- **PostgreSQL**: Base de datos relacional para consultas SQL complejas.
- **Python**: Cliente para procesamiento de datos utilizando bibliotecas como Pandas.
- **Apache Superset**: Herramienta de visualización para usuarios no técnicos.

### Modelo de Datos

#### Datos de Entrada
- **Base de datos municipal**: Registros de consumo energético, infraestructuras y zonas verdes.
- **Archivos CSV de movilidad**: Datos de tráfico, uso de BiciMAD y ocupación de parkings.
- **Datos JSON de participación ciudadana**: Reportes de la app Avisa Madrid.

#### Modelos Analíticos
- **Análisis de congestión de tráfico**: Agregaciones por hora, ubicación y tipo de vehículo.
- **Análisis de rutas de BiciMAD**: Patrones de uso, estaciones populares y segmentación de usuarios.
- **Análisis de ocupación de parkings**: Variaciones temporales y correlación con ubicación.
- **Relación infraestructura-población**: Integración de datos demográficos e infraestructura de transporte.

### Procesos de Transformación

El flujo de datos sigue estos procesos de transformación:

1. **Ingesta (01_ingest_data.py)**: Carga de datos sin procesar desde las fuentes originales.
2. **Procesamiento (02_process_data.py)**: Limpieza, transformación y enriquecimiento de datos.
3. **Acceso (03_access_zone.py)**: Creación de vistas analíticas específicas para cada perfil de usuario.
4. **Gobernanza (04_govern_zone.py)**: Gestión de metadatos, calidad y seguridad.
5. **Consulta (05_query_data.py)**: Ejemplos de análisis para diferentes perfiles de usuario.

## Guía de Puesta en Marcha

### Requisitos Previos
- Docker y Docker Compose
- Git

### Pasos de Instalación

1. Clonar el repositorio:
