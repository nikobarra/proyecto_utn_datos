"""
Trabajo Práctico 1 - Extracción y Almacenamiento de Datos
Autor: Nicolás Barra
Fecha: 2025

Este programa implementa:
1. Extracción de datos de The News API (2 endpoints)
2. Conversión a DataFrames de Pandas
3. Almacenamiento en formato Delta Lake con particionamiento
4. Extracción incremental para noticias y full para fuentes
"""

import requests
import pandas as pd
import os
from datetime import datetime
import json
from typing import List
import logging
from dotenv import load_dotenv
from deltalake import write_deltalake

# Cargar variables de entorno
load_dotenv("config.env")

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class NewsDataExtractor:
    """
    Clase para extraer y almacenar datos de The News API
    """

    def __init__(self, api_key: str = None):
        """
        Inicializa el extractor de datos

        Args:
            api_key: Clave de API (opcional para endpoints públicos)
        """
        # Cargar configuración desde variables de entorno
        self.base_url = os.getenv("API_BASE_URL", "https://api.thenewsapi.com/v1")
        self.api_key = api_key or os.getenv("API_TOKEN")
        self.session = requests.Session()

        # Configurar headers
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

        # Crear directorios base para Delta Lake
        self.data_lake_base = os.getenv("DATA_LAKE_BASE", "delta_lake")
        self.create_data_lake_directories()

    def create_data_lake_directories(self):
        """
        Crea la estructura de directorios para el data lake
        """
        directories = [
            f"{self.data_lake_base}/news/top_stories",
            f"{self.data_lake_base}/news/sources",
            f"{self.data_lake_base}/logs",
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Directorio creado/verificado: {directory}")

    def extract_top_stories(
        self, country: str = None, language: str = None, limit: int = None
    ) -> pd.DataFrame:
        """
        Extrae noticias top (datos temporales)

        Args:
            country: País de las noticias
            language: Idioma
            limit: Número máximo de noticias

        Returns:
            DataFrame con las noticias top
        """
        try:
            # Usar valores por defecto desde variables de entorno
            country = country or os.getenv("DEFAULT_COUNTRY", "us")
            language = language or os.getenv("DEFAULT_LANGUAGE", "en")
            limit = limit or int(os.getenv("DEFAULT_LIMIT", "100"))

            logger.info(f"Extrayendo noticias top para país: {country}")

            endpoint = os.getenv("ENDPOINT_TOP_STORIES", "/news/top")
            url = f"{self.base_url}{endpoint}"
            params = {"country": country, "language": language, "limit": limit}

            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if "data" not in data:
                logger.error("Respuesta de API no contiene 'data'")
                return pd.DataFrame()

            # Convertir a DataFrame
            df = pd.DataFrame(data["data"])

            # Agregar metadatos de extracción
            df["extraction_date"] = datetime.now()
            df["source_endpoint"] = "top_stories"
            df["country"] = country
            df["language"] = language

            # Convertir published_at a datetime
            if "published_at" in df.columns:
                df["published_at"] = pd.to_datetime(df["published_at"])
                df["published_date"] = df["published_at"].dt.date
                df["published_hour"] = df["published_at"].dt.hour

            logger.info(f"Extraídas {len(df)} noticias top")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error en la extracción de noticias top: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error inesperado en extracción de noticias top: {e}")
            return pd.DataFrame()

    def extract_sources(self) -> pd.DataFrame:
        """
        Extrae información de fuentes (datos estáticos/metadatos)

        Returns:
            DataFrame con información de fuentes
        """
        try:
            logger.info("Extrayendo información de fuentes")

            endpoint = os.getenv("ENDPOINT_SOURCES", "/news/sources")
            url = f"{self.base_url}{endpoint}"

            response = self.session.get(url)
            response.raise_for_status()

            data = response.json()

            if "data" not in data:
                logger.error("Respuesta de API no contiene 'data'")
                return pd.DataFrame()

            # Convertir a DataFrame
            df = pd.DataFrame(data["data"])

            # Agregar metadatos de extracción
            df["extraction_date"] = datetime.now()
            df["source_endpoint"] = "sources"

            logger.info(f"Extraídas {len(df)} fuentes")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error en la extracción de fuentes: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error inesperado en extracción de fuentes: {e}")
            return pd.DataFrame()

    def save_to_delta_lake(
        self, df: pd.DataFrame, table_name: str, partition_by: List[str] = None
    ):
        """
        Guarda DataFrame en formato Delta Lake real

        Args:
            df: DataFrame a guardar
            table_name: Nombre de la tabla
            partition_by: Columnas para particionamiento
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame vacío para {table_name}, no se guarda")
                return

            # Crear directorio de destino
            delta_path = f"{self.data_lake_base}/news/{table_name}"
            os.makedirs(delta_path, exist_ok=True)

            # Limpiar y preparar datos para Delta Lake
            df_clean = df.copy()

            # Limpiar todas las columnas de valores nulos y tipos problemáticos
            for col in df_clean.columns:
                if df_clean[col].dtype == "object":
                    # Convertir objetos a string y llenar nulos
                    df_clean[col] = df_clean[col].astype(str).fillna("")
                elif df_clean[col].dtype == "datetime64[ns]":
                    # Convertir datetime a string
                    df_clean[col] = df_clean[col].astype(str).fillna("")
                else:
                    # Para otros tipos, llenar nulos con valores apropiados
                    df_clean[col] = df_clean[col].fillna(0)

            # Configurar particionamiento
            if partition_by:
                available_columns = [
                    col for col in partition_by if col in df_clean.columns
                ]
                if available_columns:
                    logger.info(f"Particionando por: {available_columns}")
                    # Guardar en formato Delta Lake con particionamiento
                    write_deltalake(
                        delta_path,
                        df_clean,
                        partition_by=available_columns,
                        mode="append",
                    )
                else:
                    logger.warning(
                        f"Columnas de particionamiento no encontradas: {partition_by}"
                    )
                    # Guardar sin particionamiento
                    write_deltalake(delta_path, df_clean, mode="append")
            else:
                # Guardar sin particionamiento
                write_deltalake(delta_path, df_clean, mode="append")

            logger.info(f"Datos guardados en Delta Lake: {delta_path}")
            logger.info(f"Registros guardados: {len(df_clean)}")

            # Guardar metadatos de la extracción
            self.save_extraction_metadata(table_name, len(df_clean), delta_path)

        except Exception as e:
            logger.error(f"Error guardando en Delta Lake para {table_name}: {e}")

    def save_extraction_metadata(
        self, table_name: str, record_count: int, file_path: str
    ):
        """
        Guarda metadatos de la extracción

        Args:
            table_name: Nombre de la tabla
            record_count: Número de registros
            file_path: Ruta del archivo guardado
        """
        metadata = {
            "table_name": table_name,
            "extraction_date": datetime.now().isoformat(),
            "record_count": record_count,
            "file_path": file_path,
            "extraction_type": "incremental" if table_name == "top_stories" else "full",
            "format": "delta_lake",
        }

        metadata_path = f"{self.data_lake_base}/logs/{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Metadatos guardados: {metadata_path}")

    def run_extraction_pipeline(self):
        """
        Ejecuta el pipeline completo de extracción
        """
        logger.info("Iniciando pipeline de extracción de datos")

        # 1. Extraer noticias top (datos temporales - incremental)
        logger.info("=== Extrayendo noticias top ===")
        top_stories_df = self.extract_top_stories()

        if not top_stories_df.empty:
            # Guardar con particionamiento por fecha y hora
            self.save_to_delta_lake(
                df=top_stories_df,
                table_name="top_stories",
                partition_by=["published_date", "published_hour"],
            )

        # 2. Extraer fuentes (datos estáticos - full)
        logger.info("=== Extrayendo fuentes ===")
        sources_df = self.extract_sources()

        if not sources_df.empty:
            # Guardar con particionamiento opcional por categoría
            self.save_to_delta_lake(
                df=sources_df,
                table_name="sources",
                partition_by=["category"] if "category" in sources_df.columns else None,
            )

        logger.info("Pipeline de extracción completado")

        return {
            "top_stories_count": len(top_stories_df),
            "sources_count": len(sources_df),
        }


def main():
    """
    Función principal del programa
    """
    print("=" * 60)
    print("TRABAJO PRÁCTICO 1 - EXTRACCIÓN Y ALMACENAMIENTO DE DATOS")
    print("Autor: Nicolás Barra")
    print("API: The News API")
    print("Formato: Delta Lake Real")
    print("=" * 60)

    # Inicializar extractor (usa variables de entorno automáticamente)
    extractor = NewsDataExtractor()

    # Ejecutar pipeline
    results = extractor.run_extraction_pipeline()

    # Mostrar resultados
    print("\n" + "=" * 60)
    print("RESULTADOS DE LA EXTRACCIÓN")
    print("=" * 60)
    print(f"Noticias top extraídas: {results['top_stories_count']}")
    print(f"Fuentes extraídas: {results['sources_count']}")
    print(
        f"Total de registros: {results['top_stories_count'] + results['sources_count']}"
    )
    print("\nDatos guardados en formato Delta Lake en:")
    print(f"- Noticias: {extractor.data_lake_base}/news/top_stories/")
    print(f"- Fuentes: {extractor.data_lake_base}/news/sources/")
    print(f"- Logs: {extractor.data_lake_base}/logs/")
    print("=" * 60)


if __name__ == "__main__":
    main()
