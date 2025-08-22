"""
Trabajo Práctico 1 - Extracción, Almacenamiento y Procesamiento de Datos
Autor: Nicolás Barra
Fecha: 2025

Este programa implementa un pipeline de datos completo que:
1. Extrae datos de noticias y fuentes desde The News API.
2. Almacena los datos crudos en formato Delta Lake.
3. Procesa, limpia y enriquece los datos de noticias.
4. Genera una tabla agregada con estadísticas.
5. Guarda los resultados procesados y agregados en Delta Lake.
6. Sigue buenas prácticas de código, como modularidad, documentación y legibilidad.
"""

import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Any
from urllib.parse import urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from deltalake import write_deltalake, DeltaTable

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Carga de Variables de Entorno ---
load_dotenv("config.env")


class PipelineDeNoticias:
    """
    Clase que encapsula un pipeline de ETL para datos de noticias.

    Orquesta la extracción, procesamiento y carga de datos, siguiendo
    buenas prácticas de modularidad y documentación.
    """

    # --- Constantes de Configuración ---
    DATA_LAKE_BASE = os.getenv("DATA_LAKE_BASE", "delta_lake")
    API_BASE_URL = os.getenv("API_BASE_URL", "https://api.thenewsapi.com/v1")
    API_TOKEN = os.getenv("API_TOKEN")

    # Rutas de las tablas
    # Capa Bronze: Datos crudos del origen
    RUTA_BRONZE_NOTICIAS = f"{DATA_LAKE_BASE}/bronze/thenewsapi/top_stories"
    RUTA_BRONZE_FUENTES = f"{DATA_LAKE_BASE}/bronze/thenewsapi/sources"
    # Capa Silver: Datos procesados y enriquecidos
    RUTA_SILVER_NOTICIAS_ENRIQUECIDAS = f"{DATA_LAKE_BASE}/silver/top_stories_enriched"
    # Capa Gold: Datos agregados y curados
    RUTA_GOLD_CONTEO_POR_FUENTE = f"{DATA_LAKE_BASE}/gold/news_count_by_source"
    # Logs
    RUTA_LOGS = f"{DATA_LAKE_BASE}/logs"

    def __init__(self, api_key: str = None):
        """
        Inicializa el pipeline de noticias.

        Args:
            api_key (str, optional): La clave de API para The News API.
                                     Si no se provee, se toma de las variables de entorno.
        """
        self.api_key = api_key or self.API_TOKEN
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        self._crear_directorios_data_lake()

    def _crear_directorios_data_lake(self):
        """Crea la estructura de directorios necesaria para el Data Lake."""
        directorios = [
            self.RUTA_BRONZE_NOTICIAS,
            self.RUTA_BRONZE_FUENTES,
            self.RUTA_SILVER_NOTICIAS_ENRIQUECIDAS,
            self.RUTA_GOLD_CONTEO_POR_FUENTE,
            self.RUTA_LOGS,
        ]
        for directorio in directorios:
            os.makedirs(directorio, exist_ok=True)
            logger.info(f"Directorio creado/verificado: {directorio}")

    def extraer_noticias_principales(self, pais: str = None, idioma: str = None, limite: int = None) -> pd.DataFrame:
        """
        Extrae las noticias principales desde el endpoint /news/top de la API.
        """
        try:
            pais = pais or os.getenv("DEFAULT_COUNTRY", "us")
            idioma = idioma or os.getenv("DEFAULT_LANGUAGE", "en")
            limite = limite or int(os.getenv("DEFAULT_LIMIT", "3"))
            logger.info(f"Extrayendo noticias principales para el país: {pais}")

            endpoint = "/news/top"
            url = f"{self.API_BASE_URL}{endpoint}"
            params = {"api_token": self.api_key, "locale": pais, "language": idioma, "limit": limite}

            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "data" not in data:
                logger.error("La respuesta de la API no contiene la clave 'data'")
                return pd.DataFrame()

            df = pd.DataFrame(data["data"])
            df["fecha_extraccion"] = datetime.now()
            df["endpoint_origen"] = "top_stories"
            df["pais_consulta"] = pais
            df["idioma_consulta"] = idioma

            date_col = None
            if "published_on" in df.columns:
                date_col = "published_on"
            elif "published_at" in df.columns:
                date_col = "published_at"

            if date_col:
                df["fecha_publicacion"] = pd.to_datetime(df[date_col])
                df["fecha_particion"] = df["fecha_publicacion"].dt.date
                # No se particiona por hora, solo por día

            logger.info(f"Se extrajeron {len(df)} noticias principales")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red extrayendo noticias: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error inesperado extrayendo noticias: {e}")
            return pd.DataFrame()

    def extraer_fuentes(self) -> pd.DataFrame:
        """
        Extrae todas las fuentes de noticias disponibles desde el endpoint /news/sources.
        """
        try:
            logger.info("Extrayendo fuentes de noticias...")
            endpoint = "/news/sources"
            url = f"{self.API_BASE_URL}{endpoint}"
            params = {"api_token": self.api_key}

            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "data" not in data:
                logger.error("La respuesta de la API no contiene la clave 'data'")
                return pd.DataFrame()

            df = pd.DataFrame(data["data"])
            df["fecha_extraccion"] = datetime.now()
            df["endpoint_origen"] = "sources"
            logger.info(f"Se extrajeron {len(df)} fuentes")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red extrayendo fuentes: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error inesperado extrayendo fuentes: {e}")
            return pd.DataFrame()

    def guardar_en_delta_lake(self, df: pd.DataFrame, ruta_tabla: str, modo: str = 'append', particionado_por: List[str] = None):
        """
        Guarda un DataFrame en una tabla Delta Lake.
        """
        try:
            if df.empty:
                logger.warning(f"El DataFrame para '{ruta_tabla}' está vacío, no se guardará.")
                return

            df_limpio = df.copy()
            for col in df_limpio.columns:
                if df_limpio[col].dtype == 'object':
                    df_limpio[col] = df_limpio[col].astype(str).fillna('')
                elif pd.api.types.is_datetime64_any_dtype(df_limpio[col]):
                    df_limpio[col] = df_limpio[col].astype(str).fillna('')

            write_deltalake(ruta_tabla, df_limpio, mode=modo, partition_by=particionado_por)
            logger.info(f"Datos guardados en Delta Lake: {ruta_tabla} (Modo: {modo})")
            self._guardar_metadatos_pipeline(os.path.basename(ruta_tabla), len(df_limpio), ruta_tabla, modo)

        except Exception as e:
            logger.error(f"Error guardando en Delta Lake para '{ruta_tabla}': {e}")

    def procesar_y_enriquecer_datos(self, ruta_noticias_crudas: str, ruta_fuentes_crudas: str, ruta_tabla_procesada: str) -> pd.DataFrame:
        """
        Carga los datos crudos, los procesa, enriquece y guarda el resultado.
        """
        try:
            logger.info(f"Iniciando procesamiento y enriquecimiento desde: {ruta_noticias_crudas}")
            df_noticias = DeltaTable(ruta_noticias_crudas).to_pandas()

            if df_noticias.empty:
                logger.warning("La tabla de noticias crudas está vacía. No hay nada que procesar.")
                return pd.DataFrame()

            logger.info(f"Leídos {len(df_noticias)} registros de noticias crudas.")

            filas_iniciales = len(df_noticias)
            df_noticias = df_noticias.drop_duplicates(subset=['uuid'])
            logger.info(f"Transformación 1: Eliminados {filas_iniciales - len(df_noticias)} duplicados.")

            df_noticias = df_noticias.rename(columns={'source': 'fuente_id'})
            logger.info("Transformación 2: Columnas renombradas.")

            df_noticias['es_titular_corto'] = df_noticias['title'].str.len() < 50
            logger.info("Transformación 3: Creada columna 'es_titular_corto'.")

            df_noticias['description'] = df_noticias['description'].fillna('Sin descripción')
            logger.info("Transformación 4: Nulos rellenados en 'description'.")

            df_noticias['dominio_fuente'] = df_noticias['url'].apply(lambda x: urlparse(x).netloc if pd.notna(x) else None)
            logger.info("Transformación 5: Creada columna 'dominio_fuente' desde la URL.")

            logger.info("Iniciando join con datos de fuentes...")
            df_fuentes = DeltaTable(ruta_fuentes_crudas).to_pandas()
            df_fuentes = df_fuentes.rename(columns={'source_id': 'fuente_id', 'domain': 'fuente_nombre'})
            
            columnas_a_unir = ['fuente_id', 'fuente_nombre']
            if 'categories' in df_fuentes.columns:
                columnas_a_unir.append('categories')

            df_enriquecido = pd.merge(df_noticias, df_fuentes[columnas_a_unir], on='fuente_id', how='left')
            logger.info(f"Transformación 6: Join completado. {len(df_enriquecido)} registros enriquecidos.")

            df_enriquecido['es_titular_corto'] = df_enriquecido['es_titular_corto'].astype(bool)
            if 'fecha_particion' in df_enriquecido.columns:
                df_enriquecido['fecha_particion'] = pd.to_datetime(df_enriquecido['fecha_particion'])

            self.guardar_en_delta_lake(df_enriquecido, ruta_tabla_procesada, modo='overwrite', particionado_por=['fecha_particion'])
            return df_enriquecido

        except Exception as e:
            logger.error(f"Error durante el procesamiento de datos: {e}", exc_info=True)
            return pd.DataFrame()

    def agregar_datos(self, df_procesado: pd.DataFrame, ruta_tabla_agregada: str):
        """
        Genera una tabla agregada a partir de los datos procesados.
        """
        try:
            if df_procesado.empty:
                logger.warning("DataFrame procesado vacío, no se puede agregar.")
                return

            logger.info("Iniciando agregación de datos por fuente...")
            df_agregado = df_procesado.groupby('fuente_nombre').agg(cantidad_noticias=('uuid', 'count')).reset_index()
            logger.info(f"Agregación completada. {len(df_agregado)} fuentes agregadas.")

            self.guardar_en_delta_lake(df_agregado, ruta_tabla_agregada, modo='overwrite')

        except Exception as e:
            logger.error(f"Error durante la agregación de datos: {e}")

    def _guardar_metadatos_pipeline(self, nombre_tabla: str, num_registros: int, ruta_archivo: str, operacion: str):
        """
        Guarda metadatos de una operación del pipeline en un archivo JSON.
        """
        metadatos = {
            "nombre_tabla": nombre_tabla,
            "timestamp_operacion": datetime.now().isoformat(),
            "cantidad_registros": num_registros,
            "ruta_archivo": ruta_archivo,
            "operacion": operacion,
            "formato": "delta_lake",
        }
        ruta_metadatos = f"{self.RUTA_LOGS}/{nombre_tabla}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(ruta_metadatos, "w") as f:
            json.dump(metadatos, f, indent=4)
        logger.info(f"Metadatos de operación guardados: {ruta_metadatos}")

    def ejecutar_pipeline(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo de extracción, procesamiento y agregación.
        """
        logger.info("Iniciando pipeline de datos completo")

        logger.info("=== Fase 1: Extracción de Datos ===")
        df_noticias = self.extraer_noticias_principales()
        if not df_noticias.empty:
            self.guardar_en_delta_lake(df_noticias, self.RUTA_BRONZE_NOTICIAS, modo='overwrite', particionado_por=["fecha_particion"])

        df_fuentes = self.extraer_fuentes()
        if not df_fuentes.empty:
            self.guardar_en_delta_lake(df_fuentes, self.RUTA_BRONZE_FUENTES, modo='overwrite', particionado_por=["categories"] if "categories" in df_fuentes.columns else None)

        logger.info("=== Fase 2: Procesamiento y Enriquecimiento de Datos ===")
        df_procesado = self.procesar_y_enriquecer_datos(self.RUTA_BRONZE_NOTICIAS, self.RUTA_BRONZE_FUENTES, self.RUTA_SILVER_NOTICIAS_ENRIQUECIDAS)

        logger.info("=== Fase 3: Agregación de Datos ===")
        self.agregar_datos(df_procesado, self.RUTA_GOLD_CONTEO_POR_FUENTE)

        logger.info("Pipeline de datos completado")
        return {
            "noticias_extraidas": len(df_noticias),
            "fuentes_extraidas": len(df_fuentes),
            "noticias_procesadas": len(df_procesado),
            "fuentes_agregadas": len(df_procesado.groupby('fuente_id')) if not df_procesado.empty else 0
        }

def main():
    """
    Función principal para ejecutar el pipeline de noticias.
    """
    print("=" * 60)
    print("TRABAJO PRÁCTICO 1 - PIPELINE DE EXTRACCIÓN Y PROCESAMIENTO")
    print("Autor: Nicolás Barra")
    print("=" * 60)

    pipeline = PipelineDeNoticias()
    resultados = pipeline.ejecutar_pipeline()

    print("\n" + "=" * 60)
    print("RESULTADOS DEL PIPELINE")
    print("=" * 60)
    print(f"Noticias crudas extraídas: {resultados['noticias_extraidas']}")
    print(f"Fuentes extraídas: {resultados['fuentes_extraidas']}")
    print(f"Noticias procesadas y enriquecidas: {resultados['noticias_procesadas']}")
    print(f"Fuentes únicas en datos procesados: {resultados['fuentes_agregadas']}")
    print("\nUbicaciones en Delta Lake:")
    print(f"- Datos Crudos (Bronze): {pipeline.RUTA_BRONZE_NOTICIAS}")
    print(f"- Datos Procesados (Silver): {pipeline.RUTA_SILVER_NOTICIAS_ENRIQUECIDAS}")
    print(f"- Datos Agregados (Gold): {pipeline.RUTA_GOLD_CONTEO_POR_FUENTE}")
    print(f"- Logs: {pipeline.RUTA_LOGS}")
    print("=" * 60)

if __name__ == "__main__":
    main()
