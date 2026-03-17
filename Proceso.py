from __future__ import annotations

import argparse
import asyncio
import urllib.parse
from pathlib import Path
import sqlalchemy

from src.core.config import load_settings
from src.services.logger_service import build_logger
from src.services.worker_service import EtlWorkerService


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Worker Service ETL en Python (fase de extraccion)."
	)
	parser.add_argument(
		"--config",
		default="config/settings.json",
		help="Ruta al archivo JSON de configuracion.",
	)
	parser.add_argument(
		"--skip-schema",
		action="store_true",
		help="No aplicar script SQL antes de ejecutar el worker.",
	)
	return parser.parse_args()



def main() -> None:
	args = parse_args()
	base_dir = Path(__file__).resolve().parent

	config_path = Path(args.config)
	if not config_path.is_absolute():
		config_path = (base_dir / config_path).resolve()

	settings = load_settings(base_dir=base_dir, config_path=config_path)

	logger = build_logger(
		name="EtlWorkerService",
		level=settings.worker.log_level,
		log_file=base_dir / "staging" / "etl_worker.log",
	)

	logger.info("Configuracion cargada desde %s", settings.config_path)
	logger.info("Base de datos objetivo: %s", settings.database.database)

	worker = EtlWorkerService(settings=settings, logger=logger)
	result = asyncio.run(worker.run(run_schema=not args.skip_schema))

	print("=== RESUMEN WORKER ETL ===")
	print(f"BatchId: {result.batch_id}")
	print(f"Datasets extraidos: {result.total_datasets}")
	print(f"Filas extraidas: {result.total_rows}")
	print(f"Errores: {result.errors}")

	# --- Carga analítica ---
	print("\n=== INICIO CARGA ANALÍTICA ===")
	import pyodbc
	from src.services.analitica_loader import poblar_dimensiones, poblar_hechos, validar_carga, reporte_ventas_por_cliente


	# Cadena de conexión corregida para SQL Server local
	STAGING_CONN_STR = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=LAPTOP-L44SRLPQ\\SQLEXPRESS;DATABASE=ProcesoETLDB;Trusted_Connection=yes;'
	ANALITICA_CONN_STR = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=LAPTOP-L44SRLPQ\\SQLEXPRESS;DATABASE=AnaliticaVentas;Trusted_Connection=yes;'

	conn_staging = pyodbc.connect(STAGING_CONN_STR)
	conn_analitica = pyodbc.connect(ANALITICA_CONN_STR)

	# Crear engines SQLAlchemy para pandas
	STAGING_ENGINE = sqlalchemy.create_engine(
		'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(STAGING_CONN_STR)
	)
	ANALITICA_ENGINE = sqlalchemy.create_engine(
		'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(ANALITICA_CONN_STR)
	)

	poblar_dimensiones(STAGING_ENGINE, conn_analitica)
	poblar_hechos(STAGING_ENGINE, conn_analitica)
	validar_carga(conn_analitica)
	reporte_ventas_por_cliente(conn_analitica)
	print("\n=== FIN CARGA ANALÍTICA ===")


if __name__ == "__main__":
	main()
