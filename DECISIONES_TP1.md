# Decisiones de Diseño - Trabajo Práctico 1

**Autor:** Nicolás Barra  
**Fecha:** 2025

## 1. Selección de la Fuente de Datos

### API Elegida: The News API

-   **URL Base:** https://www.thenewsapi.com/
-   **Endpoint 1:** `/v1/news/top` (datos temporales)
-   **Endpoint 2:** `/v1/news/sources` (datos estáticos/metadatos)

### Justificación de la Selección:

1. **API Gratuita:** Permite acceso sin costo para el desarrollo
2. **Datos Temporales:** Las noticias se actualizan constantemente (mínimo diario)
3. **Datos Estáticos:** Las fuentes proporcionan metadatos que cambian raramente
4. **Documentación Clara:** API bien documentada y fácil de usar
5. **Formato JSON:** Compatible con Pandas para conversión a DataFrames

## 2. Técnicas de Extracción Implementadas

### Extracción Incremental (Noticias Top)

-   **Justificación:** Las noticias se actualizan constantemente
-   **Implementación:** Extrae datos nuevos cada ejecución
-   **Particionamiento:** Por fecha (`published_date`) y hora (`published_hour`)
-   **Ventajas:** Optimiza consultas temporales y reduce redundancia

### Extracción Full (Fuentes)

-   **Justificación:** Los metadatos de fuentes cambian raramente
-   **Implementación:** Extrae todos los datos en cada ejecución
-   **Particionamiento:** Por categoría (si está disponible)
-   **Ventajas:** Garantiza datos completos y actualizados

## 3. Almacenamiento en Delta Lake Real

### Formato de Almacenamiento

-   **Formato:** Delta Lake nativo con archivos Parquet
-   **Estructura:** Directorios organizados jerárquicamente con particionamiento real
-   **Metadatos:** Archivos JSON con información de extracción + \_delta_log

### Particionamiento Real

-   **Noticias:** Por fecha y hora para optimizar consultas temporales
-   **Fuentes:** Por categoría para facilitar filtros
-   **Beneficios:** Mejora rendimiento de consultas y organización
-   **Implementación:** Estructura de directorios nativa de Delta Lake

### Características Delta Lake

-   **Archivos Parquet:** Formato columnar eficiente
-   **Metadatos de transacciones:** \_delta_log con historial de cambios
-   **ACID Transactions:** Consistencia de datos
-   **Time Travel:** Capacidad de versionado de datos

## 4. Estructura del Delta Lake

```
delta_lake/
├── news/
│   ├── top_stories/     # Noticias con particionamiento temporal real
│   │   ├── _delta_log/  # Metadatos de transacciones Delta Lake
│   │   └── published_date=YYYY-MM-DD/
│   │       └── published_hour=HH/
│   │           └── *.parquet  # Archivos Delta Lake
│   └── sources/         # Fuentes con particionamiento por categoría
└── logs/               # Metadatos de extracción
```

### Justificación de la Estructura:

1. **Separación Clara:** Noticias y fuentes en directorios distintos
2. **Logs Centralizados:** Metadatos de todas las extracciones
3. **Escalabilidad:** Fácil agregar nuevos tipos de datos
4. **Mantenimiento:** Estructura clara y organizada
5. **Delta Lake Nativo:** Estructura estándar de Delta Lake

## 5. Decisiones Técnicas

### Manejo de Errores

-   **Try-Catch:** Captura errores de red y API
-   **Logging:** Registro detallado de operaciones
-   **DataFrames Vacíos:** Manejo graceful de errores

### Metadatos de Extracción

-   **Información Guardada:** Fecha, registros, tipo de extracción
-   **Formato JSON:** Fácil de leer y procesar
-   **Traza Completa:** Seguimiento de cada extracción

### Configuración de Logging

-   **Nivel INFO:** Información detallada del proceso
-   **Formato Estándar:** Timestamp, nivel, mensaje
-   **Debugging:** Facilita identificación de problemas

### Limpieza de Datos para Delta Lake

-   **Conversión de tipos:** Manejo de valores nulos y tipos problemáticos
-   **Particionamiento:** Conversión de columnas de particionamiento a string
-   **Compatibilidad:** Asegurar compatibilidad con formato Delta Lake

## 6. Compatibilidad y Portabilidad

### Google Colab

-   **Dependencias Mínimas:** requests, pandas, deltalake, python-dotenv
-   **Delta Lake Real:** Implementación nativa con librería deltalake
-   **Ejecución Directa:** Funciona sin configuración adicional

### Sistemas Operativos

-   **Cross-Platform:** Windows, macOS, Linux
-   **Python 3.8+:** Compatibilidad amplia
-   **Dependencias Estándar:** Librerías comunes

## 7. Optimizaciones Implementadas

### Rendimiento

-   **Sesión HTTP:** Reutilización de conexiones
-   **Particionamiento Real:** Optimización de consultas con Delta Lake
-   **Manejo de Memoria:** DataFrames eficientes
-   **Archivos Parquet:** Formato columnar optimizado

### Mantenibilidad

-   **Código Modular:** Clases y métodos bien definidos
-   **Documentación:** Docstrings completos
-   **Configuración:** Parámetros flexibles

## 8. Consideraciones Futuras

### Escalabilidad

-   **Múltiples APIs:** Fácil agregar nuevas fuentes
-   **Nuevos Tipos de Datos:** Estructura extensible
-   **Procesamiento:** Preparado para limpieza de datos

### Mejoras Posibles

-   **Time Travel:** Aprovechar capacidades de versionado de Delta Lake
-   **Scheduling:** Automatización de extracciones
-   **Monitoreo:** Métricas de rendimiento
-   **Optimización:** Z-Ordering y estadísticas de columnas

## Conclusión

El diseño implementado cumple con todos los requisitos del trabajo práctico:

-   ✅ Extracción de datos de API
-   ✅ Conversión a DataFrames
-   ✅ **Almacenamiento en Delta Lake real** con archivos Parquet
-   ✅ **Particionamiento real** con estructura nativa de Delta Lake
-   ✅ Extracción incremental y full
-   ✅ Estructura de data lake
-   ✅ Documentación completa
-   ✅ Código mantenible y escalable
-   ✅ **Implementación profesional** con Delta Lake nativo
