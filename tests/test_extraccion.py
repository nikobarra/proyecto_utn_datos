"""
Pruebas para el módulo de extracción de datos del pipeline.
"""

import pytest
import requests
from unittest.mock import MagicMock

from NicolasBarra_TP1 import PipelineDeNoticias

@pytest.fixture
def pipeline() -> PipelineDeNoticias:
    """Fixture que provee una instancia del pipeline."""
    return PipelineDeNoticias()

def test_extraer_noticias_principales_exito(pipeline, mocker):
    """Prueba la extracción exitosa de noticias principales."""
    # 1. Preparación (Arrange)
    api_response_data = {
        'data': [
            {'uuid': '1', 'title': 'Noticia 1', 'published_on': '2025-08-22T10:00:00'},
            {'uuid': '2', 'title': 'Noticia 2', 'published_on': '2025-08-22T11:00:00'}
        ]
    }
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = api_response_data
    mocker.patch.object(pipeline.session, 'get', return_value=mock_response)

    # 2. Acción (Act)
    df = pipeline.extraer_noticias_principales()

    # 3. Aserción (Assert)
    assert not df.empty
    assert len(df) == 2
    assert 'uuid' in df.columns
    assert 'fecha_particion' in df.columns # Verify partition column is created

def test_extraer_noticias_principales_fallo_api(pipeline, mocker):
    """Prueba el manejo de un error de la API al extraer noticias."""
    # 1. Preparación
    mocker.patch.object(pipeline.session, 'get', side_effect=requests.exceptions.RequestException("Error de API"))

    # 2. Acción
    df = pipeline.extraer_noticias_principales()

    # 3. Aserción
    assert df.empty

def test_extraer_fuentes_exito(pipeline, mocker):
    """Prueba la extracción exitosa de fuentes."""
    # 1. Preparación
    api_response_data = {
        'data': [
            {'source_id': 'fuente1', 'domain': 'Fuente 1'},
            {'source_id': 'fuente2', 'domain': 'Fuente 2'}
        ]
    }
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = api_response_data
    mocker.patch.object(pipeline.session, 'get', return_value=mock_response)

    # 2. Acción
    df = pipeline.extraer_fuentes()

    # 3. Aserción
    assert not df.empty
    assert len(df) == 2
    assert 'source_id' in df.columns

def test_extraer_fuentes_sin_datos(pipeline, mocker):
    """Prueba el manejo de una respuesta exitosa pero sin la clave 'data'."""
    # 1. Preparación
    api_response_data = {'metadata': 'sin datos'}
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = api_response_data
    mocker.patch.object(pipeline.session, 'get', return_value=mock_response)

    # 2. Acción
    df = pipeline.extraer_fuentes()

    # 3. Aserción
    assert df.empty
