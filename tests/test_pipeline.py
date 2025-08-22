"""
Prueba de integración para el pipeline completo.
"""

import pytest
from unittest.mock import MagicMock
import pandas as pd
from datetime import datetime

from NicolasBarra_TP1 import PipelineDeNoticias
from deltalake import DeltaTable

@pytest.fixture
def pipeline_integracion(tmp_path, mocker):
    """
    Fixture para la prueba de integración.

    - Provee una instancia del pipeline con un data lake temporal.
    - Parchea las llamadas a la API para no depender de la red.
    """
    pipeline_instance = PipelineDeNoticias()
    pipeline_instance.DATA_LAKE_BASE = str(tmp_path)
    pipeline_instance._crear_directorios_data_lake()

    # Datos simulados que devolverá la API
    noticias_api_data = {
        'data': [
            {'uuid': '1', 'title': 'Noticia 1', 'source': 'fuente1', 'published_on': '2025-08-22T10:00:00',
             'description': 'desc 1', 'keywords': 'k1', 'snippet': 'snip 1', 'url': 'http://example.com/1',
             'image_url': 'http://example.com/1.jpg', 'language': 'es', 'categories': ['tech'],
             'relevance_score': 1.0, 'locale': 'us'},
            {'uuid': '2', 'title': 'Noticia 2', 'source': 'fuente2', 'published_on': '2025-08-22T11:00:00',
             'description': 'desc 2', 'keywords': 'k2', 'snippet': 'snip 2', 'url': 'http://example.org/2',
             'image_url': 'http://example.org/2.jpg', 'language': 'en', 'categories': ['sports'],
             'relevance_score': 2.0, 'locale': 'us'}
        ]
    }
    fuentes_api_data = {
        'data': [
            {'source_id': 'fuente1', 'domain': 'Fuente Uno', 'language': 'es', 'locale': 'us', 'categories': ['cat1']},
            {'source_id': 'fuente2', 'domain': 'Fuente Dos', 'language': 'en', 'locale': 'us', 'categories': ['cat2']}
        ]
    }

    mock_noticias_response = MagicMock()
    mock_noticias_response.raise_for_status.return_value = None
    mock_noticias_response.json.return_value = noticias_api_data

    mock_fuentes_response = MagicMock()
    mock_fuentes_response.raise_for_status.return_value = None
    mock_fuentes_response.json.return_value = fuentes_api_data

    # Mockear el método get de la sesión para que devuelva las respuestas simuladas
    mocker.patch.object(pipeline_instance.session, 'get', side_effect=[mock_noticias_response, mock_fuentes_response])

    return pipeline_instance

def test_ejecutar_pipeline_completo(pipeline_integracion):
    """
    Prueba que el pipeline completo se ejecute sin errores y genere los artefactos esperados.
    """
    # 1. Preparación (Arrange)
    # El fixture `pipeline_integracion` ya ha preparado todo.

    # 2. Acción (Act)
    resultados = pipeline_integracion.ejecutar_pipeline()

    # 3. Aserción (Assert)
    assert resultados['noticias_extraidas'] == 2
    assert resultados['fuentes_extraidas'] == 2
    assert resultados['noticias_procesadas'] == 2
    assert resultados['fuentes_agregadas'] == 2

    # Verificar que las tablas en Delta Lake fueron creadas y contienen datos
    df_noticias_enriquecidas = DeltaTable(pipeline_integracion.RUTA_SILVER_NOTICIAS_ENRIQUECIDAS).to_pandas()
    assert len(df_noticias_enriquecidas) == 2
    assert 'fuente_nombre' in df_noticias_enriquecidas.columns
    assert df_noticias_enriquecidas['fuente_nombre'].iloc[0] == 'Fuente Uno'

    df_agregado = DeltaTable(pipeline_integracion.RUTA_GOLD_CONTEO_POR_FUENTE).to_pandas()
    assert len(df_agregado) == 2
