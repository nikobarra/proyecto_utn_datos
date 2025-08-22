"""
Pruebas para el módulo de procesamiento de datos del pipeline.
"""

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import os
from deltalake import DeltaTable
from datetime import datetime

# Importar la clase a probar desde el script principal
from NicolasBarra_TP1 import PipelineDeNoticias

@pytest.fixture
def pipeline(tmp_path) -> PipelineDeNoticias:
    """Fixture que provee una instancia del pipeline con un data lake temporal."""
    pipeline_instance = PipelineDeNoticias()
    pipeline_instance.DATA_LAKE_BASE = str(tmp_path)
    pipeline_instance._crear_directorios_data_lake()
    return pipeline_instance

@pytest.fixture
def datos_de_prueba():
    """Fixture que provee datos de prueba para noticias y fuentes con esquema completo."""
    # Esquema completo de noticias crudas (20 columnas)
    noticias_crudas = pd.DataFrame({
        'uuid': ['a', 'b', 'b', 'c'],
        'title': ['Titular corto', 'Este es un titular mucho más largo de cincuenta caracteres', 'Titular duplicado', 'Otro titular'],
        'description': ['desc a', None, 'desc b', 'desc c'],
        'keywords': ['k1,k2', 'k3', 'k3', 'k4'],
        'snippet': ['snip a', 'snip b', 'snip b', 'snip c'],
        'url': ['http://example.com/a', 'https://news.example.org/b', 'https://news.example.org/b', 'http://another.com/c'],
        'image_url': ['http://example.com/a.jpg', 'https://news.example.org/b.jpg', 'https://news.example.org/b.jpg', 'http://another.com/c.jpg'],
        'language': ['es', 'en', 'en', 'es'],
        'published_at': pd.to_datetime(['2025-08-22T10:00:00', '2025-08-22T11:00:00', '2025-08-22T11:00:00', '2025-08-22T12:00:00']),
        'source': ['fuente1', 'fuente2', 'fuente2', 'fuente1'],
        'categories': [['tech'], ['sports'], ['sports'], ['general']],
        'relevance_score': [1.0, 2.0, 2.0, 3.0],
        'locale': ['us', 'us', 'us', 'us'],
        'fecha_extraccion': [datetime.now()] * 4, # Añadido por la función de extracción
        'endpoint_origen': ['top_stories'] * 4, # Añadido por la función de extracción
        'pais_consulta': ['us'] * 4, # Añadido por la función de extracción
        'idioma_consulta': ['en'] * 4, # Añadido por la función de extracción
        'fecha_publicacion': pd.to_datetime(['2025-08-22T10:00:00', '2025-08-22T11:00:00', '2025-08-22T11:00:00', '2025-08-22T12:00:00']),
        'fecha_particion': [pd.to_datetime('2025-08-22').date()] * 4,
        'hora_particion': [10, 11, 11, 12]
    })

    # Esquema completo de fuentes crudas (7 columnas)
    fuentes_crudas = pd.DataFrame({
        'source_id': ['fuente1', 'fuente2'],
        'domain': ['Fuente Uno', 'Fuente Dos'],
        'language': ['es', 'en'],
        'locale': ['us', 'us'],
        'categories': ['general', 'sports'],
        'fecha_extraccion': [datetime.now()] * 2, # Añadido por la función de extracción
        'endpoint_origen': ['sources'] * 2 # Añadido por la función de extracción
    })
    return noticias_crudas, fuentes_crudas


def test_procesar_y_enriquecer_datos(pipeline, datos_de_prueba):
    """Prueba la lógica de procesamiento y enriquecimiento de datos."""
    # 1. Preparación (Arrange)
    noticias_crudas, fuentes_crudas = datos_de_prueba
    
    pipeline.guardar_en_delta_lake(noticias_crudas, pipeline.RUTA_BRONZE_NOTICIAS, modo='overwrite')
    pipeline.guardar_en_delta_lake(fuentes_crudas, pipeline.RUTA_BRONZE_FUENTES, modo='overwrite')

    # 2. Acción (Act)
    df_procesado = pipeline.procesar_y_enriquecer_datos(
        pipeline.RUTA_BRONZE_NOTICIAS,
        pipeline.RUTA_BRONZE_FUENTES,
        pipeline.RUTA_SILVER_NOTICIAS_ENRIQUECIDAS
    )

    # 3. Aserción (Assert)
    assert not df_procesado.empty
    assert len(df_procesado) == 3
    assert df_procesado['uuid'].is_unique
    assert 'fuente_id' in df_procesado.columns
    assert df_procesado.loc[df_procesado['uuid'] == 'a', 'es_titular_corto'].iloc[0] == True
    assert df_procesado.loc[df_procesado['uuid'] == 'b', 'es_titular_corto'].iloc[0] == False
    assert df_procesado.loc[df_procesado['uuid'] == 'b', 'description'].iloc[0] == 'Sin descripción'
    assert df_procesado.loc[df_procesado['uuid'] == 'a', 'dominio_fuente'].iloc[0] == 'example.com'
    assert 'fuente_nombre' in df_procesado.columns
    assert df_procesado.loc[df_procesado['uuid'] == 'b', 'fuente_nombre'].iloc[0] == 'Fuente Dos'
    assert df_procesado['es_titular_corto'].dtype == bool

def test_agregar_datos(pipeline, datos_de_prueba):
    """Prueba la lógica de agregación de datos."""
    # 1. Preparación
    noticias_crudas, fuentes_crudas = datos_de_prueba
    pipeline.guardar_en_delta_lake(noticias_crudas, pipeline.RUTA_BRONZE_NOTICIAS, modo='overwrite')
    pipeline.guardar_en_delta_lake(fuentes_crudas, pipeline.RUTA_BRONZE_FUENTES, modo='overwrite')
    df_procesado = pipeline.procesar_y_enriquecer_datos(
        pipeline.RUTA_BRONZE_NOTICIAS,
        pipeline.RUTA_BRONZE_FUENTES,
        pipeline.RUTA_SILVER_NOTICIAS_ENRIQUECIDAS
    )

    # 2. Acción
    pipeline.agregar_datos(df_procesado, pipeline.RUTA_GOLD_CONTEO_POR_FUENTE)

    # 3. Aserción
    df_agregado_leido = DeltaTable(pipeline.RUTA_GOLD_CONTEO_POR_FUENTE).to_pandas()

    df_esperado = pd.DataFrame({
        'fuente_nombre': ['Fuente Dos', 'Fuente Uno'],
        'cantidad_noticias': [1, 2]
    })

    df_agregado_leido = df_agregado_leido.sort_values(by='fuente_nombre').reset_index(drop=True)
    df_esperado = df_esperado.sort_values(by='fuente_nombre').reset_index(drop=True)

    assert_frame_equal(df_agregado_leido, df_esperado, check_like=True)