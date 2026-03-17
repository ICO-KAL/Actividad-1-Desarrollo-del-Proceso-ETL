-- 1. Conteo de registros por tabla
SELECT 'DimClientes' AS Tabla, COUNT(*) AS Registros FROM dbo.DimClientes
UNION ALL
SELECT 'DimProductos', COUNT(*) FROM dbo.DimProductos
UNION ALL
SELECT 'DimFechas', COUNT(*) FROM dbo.DimFechas
UNION ALL
SELECT 'DimEstadosPedido', COUNT(*) FROM dbo.DimEstadosPedido
UNION ALL
SELECT 'FactPedidos', COUNT(*) FROM dbo.FactPedidos
UNION ALL
SELECT 'FactDetallesPedido', COUNT(*) FROM dbo.FactDetallesPedido
UNION ALL
SELECT 'FactVentas', COUNT(*) FROM dbo.FactVentas;

-- 2. Top 10 clientes por ventas
SELECT TOP 10 dc.Nombre, dc.Apellido, SUM(fv.Total) AS TotalVentas
FROM dbo.FactVentas fv
JOIN dbo.DimClientes dc ON fv.ClienteID = dc.ClienteID
GROUP BY dc.ClienteID, dc.Nombre, dc.Apellido
ORDER BY TotalVentas DESC;

-- 3. Ventas por producto
SELECT dp.Nombre AS Producto, SUM(fv.Cantidad) AS TotalVendidas, SUM(fv.Total) AS TotalIngresos
FROM dbo.FactVentas fv
JOIN dbo.DimProductos dp ON fv.ProductoID = dp.ProductoID
GROUP BY dp.ProductoID, dp.Nombre
ORDER BY TotalIngresos DESC;

-- 4. Ventas por mes y año
SELECT df.Año, df.Mes, SUM(fv.Total) AS TotalVentas
FROM dbo.FactVentas fv
JOIN dbo.DimFechas df ON fv.Fecha = df.Fecha
GROUP BY df.Año, df.Mes
ORDER BY df.Año, df.Mes;

-- 5. Pedidos por estado
SELECT de.Estado, COUNT(fp.PedidoID) AS TotalPedidos
FROM dbo.FactPedidos fp
JOIN dbo.DimEstadosPedido de ON fp.EstadoID = de.EstadoID
GROUP BY de.Estado
ORDER BY TotalPedidos DESC;

-- 6. Detalles de pedidos (primeros 20)
SELECT TOP 20 * FROM dbo.FactDetallesPedido;

-- 7. Clientes sin ventas
SELECT c.ClienteID, c.Nombre, c.Apellido
FROM dbo.DimClientes c
LEFT JOIN dbo.FactVentas fv ON c.ClienteID = fv.ClienteID
WHERE fv.ClienteID IS NULL;

