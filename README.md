# Trabajo Práctico 1 - Extracción y Almacenamiento de Datos

**Autor:** Nicolás Barra  
**Fecha:** 2025  
**API:** The News API

## Descripción

Este proyecto implementa un sistema completo de extracción y almacenamiento de datos utilizando Python, Pandas y **Delta Lake real**. El programa extrae datos de dos endpoints de The News API y los almacena en formato Delta Lake con particionamiento apropiado.

## Características Implementadas

### ✅ Requisitos Cumplidos

1. **Extracción de datos de API**

     - Endpoint 1: `/v1/news/top` (datos temporales)
     - Endpoint 2: `/v1/news/sources` (datos estáticos/metadatos)

2. **Conversión a DataFrames de Pandas**

     - Procesamiento de datos JSON a DataFrames
     - Agregado de metadatos de extracción

3. **Almacenamiento en Delta Lake Real**

     - Formato Delta Lake nativo con archivos Parquet
     - Particionamiento real por fecha y hora para noticias
     - Particionamiento por categoría para fuentes
     - Metadatos de transacciones Delta Lake

4. **Extracción incremental y full**

     - **Incremental:** Noticias top (se actualizan diariamente)
     - **Full:** Fuentes (datos estáticos/metadatos)

5. **Estructura de Delta Lake**

     - Creación automática de directorios
     - Organización jerárquica de datos
     - Logs de extracción

6. **Configuración Segura**

     - Variables de entorno para datos sensibles
     - Archivo de configuración separado
     - Protección de credenciales

## Estructura del Proyecto

```
entrega_1/
├── NicolasBarra_TP1.py    # Programa principal
├── requirements.txt        # Dependencias
├── README.md             # Este archivo
├── DECISIONES_TP1.md     # Justificación de decisiones
├── config.env            # Configuración con datos sensibles
├── config.env.example    # Ejemplo de configuración
├── .gitignore           # Archivos a ignorar en Git
└── delta_lake/          # Datos extraídos (se crea al ejecutar)
    ├── news/
    │   ├── top_stories/  # Noticias con particionamiento real por fecha/hora
    │   │   ├── _delta_log/  # Metadatos de transacciones Delta Lake
    │   │   └── published_date=YYYY-MM-DD/
    │   │       └── published_hour=HH/
    │   │           └── *.parquet  # Archivos Delta Lake
    │   └── sources/      # Fuentes con particionamiento por categoría
    └── logs/             # Metadatos de extracción
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

# Endpoints
ENDPOINT_TOP_STORIES=/news/top
ENDPOINT_SOURCES=/news/sources

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

## Variables de Entorno

### Configuración de API

-   `API_BASE_URL`: URL base de la API
-   `API_TOKEN`: Token de autenticación
-   `ENDPOINT_TOP_STORIES`: Endpoint para noticias top
-   `ENDPOINT_SOURCES`: Endpoint para fuentes

### Configuración de Extracción

-   `DEFAULT_COUNTRY`: País por defecto (us)
-   `DEFAULT_LANGUAGE`: Idioma por defecto (en)
-   `DEFAULT_LIMIT`: Límite de registros por defecto (100)

### Configuración de Delta Lake

-   `DATA_LAKE_BASE`: Directorio base del delta lake

## Endpoints Utilizados

### 1. Top Stories (`/v1/news/top`)

-   **Tipo:** Datos temporales
-   **Actualización:** Diaria
-   **Particionamiento:** Por fecha (`published_date`) y hora (`published_hour`)
-   **Campos principales:** título, descripción, URL, fecha publicación, fuente, categorías

### 2. Sources (`/v1/news/sources`)

-   **Tipo:** Datos estáticos/metadatos
-   **Actualización:** Rara (metadatos de fuentes)
-   **Particionamiento:** Por categoría (si está disponible)
-   **Campos principales:** información de fuentes de noticias

## Decisiones de Diseño

### Selección de API

-   **The News API:** API gratuita y confiable
-   **Datos temporales:** Noticias que se actualizan constantemente
-   **Datos estáticos:** Metadatos de fuentes que cambian raramente

### Técnicas de Extracción

-   **Incremental para noticias:** Solo extrae datos nuevos
-   **Full para fuentes:** Extrae todos los datos (cambian poco)

### Particionamiento

-   **Noticias:** Por fecha y hora para optimizar consultas temporales
-   **Fuentes:** Por categoría para facilitar filtros

### Almacenamiento

-   **Delta Lake Real:** Formato nativo con archivos Parquet
-   **Metadatos de transacciones:** \_delta_log con historial de cambios
-   **Particionamiento real:** Estructura de directorios nativa de Delta Lake

### Seguridad

-   **Variables de entorno:** Protección de datos sensibles
-   **Archivo de configuración:** Separación de configuración
-   **Gitignore:** Prevención de commit de datos sensibles

## Salida del Programa

El programa mostrará:

-   Progreso de extracción en tiempo real
-   Número de registros extraídos por endpoint
-   Ubicación de los archivos guardados
-   Logs detallados de la operación

## Compatibilidad

-   **Google Colab:** ✅ Compatible
-   **Python 3.8+:** ✅ Compatible
-   **Sistemas operativos:** Windows, macOS, Linux

## Notas Técnicas

-   El programa maneja errores de red y API
-   Crea automáticamente la estructura de directorios
-   Guarda metadatos de cada extracción
-   Utiliza logging para seguimiento de operaciones
-   Protege datos sensibles con variables de entorno
-   **Implementa Delta Lake real** con archivos Parquet y metadatos de transacciones
