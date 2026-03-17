import pandas as pd
import logging
import math

logger = logging.getLogger("CargaAnalitica")
logging.basicConfig(level=logging.INFO)

def poblar_dimensiones(engine_staging, conn_analitica):

    # DIM CLIENTES
    clientes = pd.read_sql("""

        SELECT DISTINCT
        JSON_VALUE(PayloadJson,'$.CustomerID') AS ClienteID,
        JSON_VALUE(PayloadJson,'$.FirstName') AS Nombre,
        JSON_VALUE(PayloadJson,'$.LastName') AS Apellido,
        JSON_VALUE(PayloadJson,'$.Email') AS Email,
        JSON_VALUE(PayloadJson,'$.City') AS Ciudad,
        JSON_VALUE(PayloadJson,'$.Country') AS Pais

        FROM dbo.StgCsvExtract
        WHERE EntityName='customers'

    """, engine_staging)

    logger.info(f"Clientes encontrados: {len(clientes)}")

    for _, row in clientes.iterrows():

        try:

            if pd.isna(row.ClienteID):
                continue

            cliente_id = int(row.ClienteID)

            conn_analitica.execute("""

            IF NOT EXISTS(
                SELECT 1 FROM dbo.DimClientes WHERE ClienteID=?
            )

            INSERT INTO dbo.DimClientes
            (ClienteID,Nombre,Apellido,Email,Ciudad,Pais)

            VALUES (?,?,?,?,?,?)

            """,

            cliente_id,
            cliente_id,
            row.Nombre,
            row.Apellido,
            row.Email,
            row.Ciudad,
            row.Pais
            )

        except Exception as e:
            print("Error cliente:", e)

    conn_analitica.commit()


    # DIM PRODUCTOS
    productos = pd.read_sql("""

        SELECT DISTINCT
        JSON_VALUE(PayloadJson,'$.ProductID') AS ProductoID,
        JSON_VALUE(PayloadJson,'$.ProductName') AS Nombre,
        JSON_VALUE(PayloadJson,'$.Category') AS Categoria,
        JSON_VALUE(PayloadJson,'$.Price') AS Precio

        FROM dbo.StgCsvExtract
        WHERE EntityName='products'

    """, engine_staging)

    logger.info(f"Productos encontrados: {len(productos)}")

    for _, row in productos.iterrows():

        try:

            if pd.isna(row.ProductoID):
                continue

            producto_id = int(row.ProductoID)
            precio = float(row.Precio) if not pd.isna(row.Precio) else 0

            conn_analitica.execute("""

            IF NOT EXISTS(
                SELECT 1 FROM dbo.DimProductos WHERE ProductoID=?
            )

            INSERT INTO dbo.DimProductos
            (ProductoID,Nombre,Categoria,Precio)

            VALUES (?,?,?,?)

            """,

            producto_id,
            producto_id,
            row.Nombre,
            row.Categoria,
            precio
            )

        except Exception as e:
            print("Error producto:", e)

    conn_analitica.commit()


    # DIM FECHAS
    fechas = pd.read_sql("""

        SELECT DISTINCT
        JSON_VALUE(PayloadJson,'$.OrderDate') AS Fecha

        FROM dbo.StgCsvExtract
        WHERE EntityName='orders'

    """, engine_staging)

    fechas["Fecha"] = pd.to_datetime(fechas["Fecha"], errors="coerce")

    fechas["Año"] = fechas["Fecha"].dt.year
    fechas["Mes"] = fechas["Fecha"].dt.month
    fechas["Dia"] = fechas["Fecha"].dt.day
    fechas["Trimestre"] = fechas["Fecha"].dt.quarter

    for _, row in fechas.iterrows():

        try:

            if pd.isna(row.Fecha):
                continue

            conn_analitica.execute("""

            IF NOT EXISTS(
                SELECT 1 FROM dbo.DimFechas WHERE Fecha=?
            )

            INSERT INTO dbo.DimFechas
            (Fecha,Año,Mes,Dia,Trimestre)

            VALUES (?,?,?,?,?)

            """,

            row.Fecha,
            row.Fecha,
            int(row.Año),
            int(row.Mes),
            int(row.Dia),
            int(row.Trimestre)
            )

        except Exception as e:
            print("Error fecha:", e)

    conn_analitica.commit()

def poblar_hechos(engine_staging, conn_analitica):

    # FACT DETALLES PEDIDO
    detalles = pd.read_sql("""

        SELECT
        JSON_VALUE(PayloadJson,'$.OrderID') AS PedidoID,
        JSON_VALUE(PayloadJson,'$.ProductID') AS ProductoID,
        JSON_VALUE(PayloadJson,'$.Quantity') AS Cantidad,
        JSON_VALUE(PayloadJson,'$.TotalPrice') AS Total

        FROM dbo.StgCsvExtract
        WHERE EntityName='order_details'

    """, engine_staging)

    logger.info(f"Detalles encontrados: {len(detalles)}")

    for _, row in detalles.iterrows():

        try:

            if pd.isna(row.PedidoID) or pd.isna(row.ProductoID):
                continue

            pedido_id = int(row.PedidoID)
            producto_id = int(row.ProductoID)

            cantidad = int(row.Cantidad) if not pd.isna(row.Cantidad) else 0
            total = float(row.Total) if not pd.isna(row.Total) else 0

            precio_unitario = total / cantidad if cantidad > 0 else 0

            if math.isnan(precio_unitario):
                precio_unitario = 0

            conn_analitica.execute("""

            INSERT INTO dbo.FactDetallesPedido
            (PedidoID,ProductoID,Cantidad,PrecioUnitario,Total)

            VALUES (?,?,?,?,?)

            """,

            pedido_id,
            producto_id,
            cantidad,
            precio_unitario,
            total
            )

        except Exception as e:

            print("Error detalle:", e)

    conn_analitica.commit()


    # FACT PEDIDOS (AGREGADA)
    pedidos = pd.read_sql("""

        SELECT
        JSON_VALUE(PayloadJson,'$.OrderID') AS PedidoID,
        JSON_VALUE(PayloadJson,'$.CustomerID') AS ClienteID,
        JSON_VALUE(PayloadJson,'$.OrderDate') AS Fecha

        FROM dbo.StgCsvExtract
        WHERE EntityName='orders'

    """, engine_staging)

    logger.info(f"Pedidos encontrados: {len(pedidos)}")

    for _, row in pedidos.iterrows():

        try:

            if pd.isna(row.PedidoID):
                continue

            pedido_id = int(row.PedidoID)
            cliente_id = int(row.ClienteID) if not pd.isna(row.ClienteID) else None
            fecha = row.Fecha

            conn_analitica.execute("""

            INSERT INTO dbo.FactPedidos
            (PedidoID,ClienteID,Fecha)

            VALUES (?,?,?)

            """,

            pedido_id,
            cliente_id,
            fecha
            )

        except Exception as e:

            print("Error pedido:", e)

    conn_analitica.commit()

    ventas = pd.read_sql("""

        SELECT
        JSON_VALUE(PayloadJson,'$.OrderDate') AS Fecha,
        JSON_VALUE(PayloadJson,'$.CustomerID') AS ClienteID,
        JSON_VALUE(PayloadJson,'$.ProductID') AS ProductoID,
        JSON_VALUE(PayloadJson,'$.Quantity') AS Cantidad,
        JSON_VALUE(PayloadJson,'$.TotalPrice') AS Total

        FROM dbo.StgCsvExtract
        WHERE EntityName='order_details'

    """, engine_staging)

    logger.info(f"Ventas encontradas: {len(ventas)}")

    for _, row in ventas.iterrows():

        try:

            if pd.isna(row.ClienteID) or pd.isna(row.ProductoID):
                continue

            fecha = row.Fecha
            cliente_id = int(row.ClienteID)
            producto_id = int(row.ProductoID)

            cantidad = int(row.Cantidad) if not pd.isna(row.Cantidad) else 0
            total = float(row.Total) if not pd.isna(row.Total) else 0

            conn_analitica.execute("""

            INSERT INTO dbo.FactVentas
            (Fecha,ClienteID,ProductoID,Cantidad,Total)

            VALUES (?,?,?,?,?)

            """,

            fecha,
            cliente_id,
            producto_id,
            cantidad,
            total
            )

        except Exception as e:

            print("Error venta:", e)

    conn_analitica.commit()

    facturas = pd.read_sql("""

        SELECT
        JSON_VALUE(PayloadJson,'$.OrderID') AS PedidoID,
        JSON_VALUE(PayloadJson,'$.OrderDate') AS Fecha,
        JSON_VALUE(PayloadJson,'$.TotalPrice') AS Total

        FROM dbo.StgCsvExtract
        WHERE EntityName='orders'

    """, engine_staging)

    logger.info(f"Facturas encontradas: {len(facturas)}")

    for _, row in facturas.iterrows():

        try:

            if pd.isna(row.PedidoID):
                continue

            pedido_id = int(row.PedidoID)
            fecha = row.Fecha
            total = float(row.Total) if not pd.isna(row.Total) else 0

            conn_analitica.execute("""

            INSERT INTO dbo.FactFacturacion
            (PedidoID,Fecha,Total)

            VALUES (?,?,?)

            """,
            pedido_id,
            fecha,
            total
            )

        except Exception as e:

            print("Error factura:", e)

    conn_analitica.commit()


def validar_carga(conn_analitica):

    tablas = [
        "DimClientes",
        "DimProductos",
        "DimFechas",
        "FactPedidos",
        "FactVentas",
        "FactDetallesPedido",
        "FactFacturacion"
    ]

    for tabla in tablas:

        count = conn_analitica.execute(
            f"SELECT COUNT(*) FROM dbo.{tabla}"
        ).fetchone()[0]

        print(f"{tabla}: {count} registros")


def reporte_ventas_por_cliente(conn_analitica):

    df = pd.read_sql("""

        SELECT
        dc.Nombre,
        dc.Apellido,
        SUM(fv.Total) AS TotalVentas

        FROM dbo.FactVentas fv

        JOIN dbo.DimClientes dc
        ON fv.ClienteID = dc.ClienteID

        GROUP BY dc.Nombre,dc.Apellido

        ORDER BY TotalVentas DESC

    """, conn_analitica)

    print("\n=== Ventas por Cliente ===\n")

    print(df)

def get_sqlalchemy_engine_from_pyodbc_conn(conn):

   # conn_str = conn.getinfo(pyodbc.SQL_DRIVER_NAME)

    raise NotImplementedError('Pasa la cadena de conexión original a sqlalchemy.create_engine')

