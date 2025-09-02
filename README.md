# Trabajo Práctico 1 - Extracción, Procesamiento y Almacenamiento de Datos

**Autor:** Nicolás Barra  
**Fecha:** 2025  
**API:** The News API

## Descripción

Este proyecto implementa un pipeline de datos completo, **idempotente y re-ejecutable**, para la extracción, procesamiento y almacenamiento de datos utilizando Python, Pandas y Delta Lake. El programa extrae datos de dos endpoints de The News API, aplica diversas transformaciones para limpiar, enriquecer y agregar la información, y finalmente los almacena en un Data Lake estructurado en capas (Bronze, Silver y Gold).

## Características Implementadas

### ✅ Requisitos Cumplidos

1.  **Extracción de datos de API**

    *   Endpoint 1: `/v1/news/top` (datos temporales)
    *   Endpoint 2: `/v1/news/sources` (datos estáticos/metadatos)

2.  **Conversión a DataFrames de Pandas**

    *   Procesamiento de datos JSON a DataFrames
    *   Agregado de metadatos de extracción

3.  **Procesamiento y Transformación de Datos (Más de 4 transformaciones)**

    *   **Eliminación de duplicados**: Asegura la unicidad de las noticias por `uuid`.
    *   **Renombrado de columnas**: Nombres de columnas más claros y consistentes (ej. `source` a `fuente_id`).
    *   **Creación de columna booleana**: `es_titular_corto` basada en la longitud del título.
    *   **Manejo de nulos**: Relleno de valores nulos en la descripción con 'Sin descripción'.
    *   **Extracción de dominio**: Creación de la columna `dominio_fuente` a partir de la URL.
    *   **Enriquecimiento con JOIN**: Unión de datos de noticias con información de fuentes (categoría, nombre de fuente) utilizando el dominio de la URL de la noticia y el dominio de la fuente.
    *   **Agregación con GROUP BY**: Conteo de noticias por `fuente_nombre` para generar métricas.
    *   **Manejo de fuentes desconocidas**: Relleno de valores nulos en 'fuente_nombre' con 'Fuente Desconocida' para asegurar la agregación.

4.  **Almacenamiento en Delta Lake Real**

    *   Formato Delta Lake nativo con archivos Parquet
    *   Metadatos de transacciones Delta Lake (`_delta_log`)

5.  **Estructura de Data Lake en Capas (Bronze, Silver, Gold)**

    *   **Bronze**: Datos crudos, inmutables, directamente de la API.
    *   **Silver**: Datos procesados, limpios y enriquecidos.
    *   **Gold**: Datos agregados y curados, listos para consumo.
    *   Organización jerárquica: `capa/sistema_origen/entidad/`

6.  **Estrategia de Particionamiento Optimizada**

    *   **Noticias (Bronze/Silver)**: Particionamiento por `fecha_particion` (diario), optimizado para consultas temporales y actualizaciones diarias.
    *   **Fuentes (Bronze)**: Particionamiento por `categories` (si aplica).

7.  **Extracción incremental y full**

    *   **Incremental:** Noticias top (se actualizan diariamente)
    *   **Full:** Fuentes (datos estáticos/metadatos)

8.  **Configuración Segura**

    *   Variables de entorno para datos sensibles
    *   Archivo de configuración separado
    *   Protección de credenciales

9.  **Tests Automatizados**

    *   Pruebas unitarias para extracción y procesamiento.
    *   Pruebas de integración para el pipeline completo.
    *   Uso de `pytest` y `pytest-mock` para simular dependencias externas.

## Estructura del Proyecto

```
entrega_1/
├── NicolasBarra_TP1.py    # Programa principal del pipeline
├── requirements.txt        # Dependencias del proyecto
├── README.md             # Este archivo
├── DECISIONES_TP1.md     # Justificación de decisiones
├── config.env            # Configuración con datos sensibles (no versionado)
├── config.env.example    # Ejemplo de configuración
├── .gitignore           # Archivos a ignorar en Git
├── tests/                # Directorio de pruebas
│   ├── conftest.py       # Configuraciones de pytest
│   ├── test_extraccion.py # Pruebas unitarias de extracción
│   ├── test_procesamiento.py # Pruebas unitarias de procesamiento
│   └── test_pipeline.py  # Pruebas de integración del pipeline
└── delta_lake/          # Data Lake (se crea al ejecutar)
    ├── bronze/          # Capa de datos crudos
    │   └── thenewsapi/  # Sistema origen
    │       ├── top_stories/  # Noticias con particionamiento por fecha
    │       │   └── fecha_particion=YYYY-MM-DD/
    │       │       └── *.parquet
    │       └── sources/      # Fuentes con particionamiento por categoría
    ├── silver/          # Capa de datos procesados
    │   └── top_stories_enriched/ # Noticias enriquecidas
    │       └── fecha_particion=YYYY-MM-DD/
    │           └── *.parquet
    ├── gold/            # Capa de datos agregados
    │   └── news_count_by_source/ # Conteo de noticias por fuente
    │       └── *.parquet
    └── logs/             # Metadatos de ejecución del pipeline
```

## Instalación y Configuración

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar variables de entorno

```bash
# Copiar el archivo de ejemplo
cp config.env.example config.env

# Editar config.env con tus datos reales
# Reemplaza 'tu_token_aqui' con tu token de API real
```

### 3. Configurar el archivo config.env

```env
# Configuración de The News API
API_BASE_URL=https://api.thenewsapi.com/v1
API_TOKEN=tu_token_real_aqui

# Configuración de extracción
DEFAULT_COUNTRY=us
DEFAULT_LANGUAGE=en
DEFAULT_LIMIT=100

# Configuración de delta lake
DATA_LAKE_BASE=delta_lake
```

### 4. Ejecutar el programa

```bash
python NicolasBarra_TP1.py
```

### 5. Ejecutar los Tests

```bash
pytest tests/
```

## Variables de Entorno

### Configuración de API

*   `API_BASE_URL`: URL base de la API
*   `API_TOKEN`: Token de autenticación

### Configuración de Extracción

*   `DEFAULT_COUNTRY`: País por defecto (us)
*   `DEFAULT_LANGUAGE`: Idioma por defecto (en)
*   `DEFAULT_LIMIT`: Límite de registros por defecto (100)

### Configuración de Delta Lake

*   `DATA_LAKE_BASE`: Directorio base del Data Lake

## Endpoints Utilizados

### 1. Top Stories (`/v1/news/top`)

*   **Tipo:** Datos temporales
*   **Actualización:** Diaria
*   **Particionamiento:** Por fecha (`fecha_particion`)
*   **Campos principales:** título, descripción, URL, fecha publicación, fuente, categorías

### 2. Sources (`/v1/news/sources`)

*   **Tipo:** Datos estáticos/metadatos
*   **Actualización:** Rara (metadatos de fuentes)
*   **Particionamiento:** Por categoría (si está disponible)
*   **Campos principales:** información de fuentes de noticias

## Decisiones de Diseño

### Selección de API

*   **The News API:** API gratuita y confiable
*   **Datos temporales:** Noticias que se actualizan constantemente
*   **Datos estáticos:** Metadatos de fuentes que cambian raramente

### Técnicas de Extracción

*   **Incremental para noticias:** Solo extrae datos nuevos
*   **Full para fuentes:** Extrae todos los datos (cambian poco)
*   **Alineación de Extracción de Fuentes:** La extracción de fuentes ahora incluye filtros por 'locale' e 'language' para asegurar la coherencia con las noticias extraídas.

### Estructura del Data Lake

*   **Capas Bronze, Silver, Gold:** Implementación de una arquitectura de Data Lake por capas para organizar los datos según su nivel de procesamiento y calidad.
    *   **Bronze:** Almacena los datos crudos, tal como se reciben de la fuente, sin transformaciones.
    *   **Silver:** Contiene los datos limpios, transformados y enriquecidos, listos para análisis más detallados.
    *   **Gold:** Guarda los datos agregados y curados, optimizados para el consumo por parte de usuarios de negocio o aplicaciones.

### Particionamiento

*   **Noticias:** Por fecha (`fecha_particion`) para optimizar consultas temporales y alinearse con la granularidad de actualización diaria.
*   **Fuentes:** Por categoría para facilitar filtros y análisis específicos.

### Almacenamiento

*   **Delta Lake Real:** Formato nativo con archivos Parquet y metadatos de transacciones (`_delta_log`).
*   **Modo Overwrite (Dinámico):** Utilizado en las capas Silver y Gold. Gracias al particionamiento, solo se sobreescriben las particiones correspondientes a los datos del día en ejecución, asegurando la idempotencia sin eliminar datos históricos de otras particiones.

### Seguridad

*   **Variables de entorno:** Protección de datos sensibles
*   **Archivo de configuración:** Separación de configuración
*   **Gitignore:** Prevención de commit de datos sensibles

### Tests Automatizados

*   **Pytest:** Framework de testing utilizado para la ejecución de pruebas.
*   **Mocks:** Uso de `unittest.mock` y `pytest-mock` para simular llamadas a la API y otras dependencias externas, asegurando la independencia y velocidad de las pruebas unitarias.

## Salida del Programa

El programa mostrará:

*   Progreso de extracción y procesamiento en tiempo real
*   Número de registros extraídos, procesados y agregados
*   Ubicación de los archivos guardados en cada capa del Data Lake
*   Logs detallados de la operación

## Compatibilidad

*   **Google Colab:** ✅ Compatible
*   **Python 3.8+:** ✅ Compatible
*   **Sistemas operativos:** Windows, macOS, Linux

## Notas Técnicas

*   El programa maneja errores de red y API
*   Crea automáticamente la estructura de directorios
*   Guarda metadatos de cada operación del pipeline
*   Utiliza logging para seguimiento de operaciones
*   Protege datos sensibles con variables de entorno
*   **Implementa Delta Lake real** con archivos Parquet y metadatos de transacciones