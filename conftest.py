# Raiz do projeto no sys.path — permite que qualquer subpacote (streaming,
# producer, dashboard) seja importado nos testes independente de como o
# pytest é invocado.
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
