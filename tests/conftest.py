"""
Fichero de configuración de Pytest.

Este fichero permite realizar configuraciones especiales para las pruebas.
Agrega el directorio raíz del proyecto al path de Python para que los módulos
(como NicolasBarra_TP1.py) puedan ser importados desde los tests.
"""

import sys
import os

# Añadir el directorio raíz del proyecto al sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
