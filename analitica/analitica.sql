
-- Modelo estrella para Data Warehouse analítico
-- Basado en el modelo operativo de ProcesoETL.sql

CREATE TABLE dbo.DimClientes (
	ClienteID INT PRIMARY KEY,
	Nombre NVARCHAR(100),
	Apellido NVARCHAR(100),
	Email NVARCHAR(200),
	Ciudad NVARCHAR(120),
	Pais NVARCHAR(120)
);

CREATE TABLE dbo.DimProductos (
	ProductoID INT PRIMARY KEY,
	Nombre NVARCHAR(200),
	Categoria NVARCHAR(100),
	Precio DECIMAL(18,2)
);

CREATE TABLE dbo.DimFechas (
	Fecha DATE PRIMARY KEY,
	Año INT,
	Mes INT,
	Dia INT,
	Trimestre INT
);

CREATE TABLE dbo.DimEstadosPedido (
	EstadoID INT PRIMARY KEY,
	Estado NVARCHAR(20)
);

CREATE TABLE dbo.FactVentas (
	VentaID BIGINT IDENTITY(1,1) PRIMARY KEY,
	Fecha DATE NOT NULL,
	ClienteID INT NOT NULL,
	ProductoID INT NOT NULL,
	Cantidad INT NOT NULL,
	Total DECIMAL(18,2) NOT NULL,
	CONSTRAINT FK_FactVentas_Cliente FOREIGN KEY (ClienteID) REFERENCES dbo.DimClientes(ClienteID),
	CONSTRAINT FK_FactVentas_Producto FOREIGN KEY (ProductoID) REFERENCES dbo.DimProductos(ProductoID),
	CONSTRAINT FK_FactVentas_Fecha FOREIGN KEY (Fecha) REFERENCES dbo.DimFechas(Fecha)
);

CREATE TABLE dbo.FactPedidos (
	PedidoID BIGINT IDENTITY(1,1) PRIMARY KEY,
	ClienteID INT NOT NULL,
	Fecha DATE NOT NULL,
	EstadoID INT NOT NULL,
	Total DECIMAL(18,2) NOT NULL,
	CONSTRAINT FK_FactPedidos_Cliente FOREIGN KEY (ClienteID) REFERENCES dbo.DimClientes(ClienteID),
	CONSTRAINT FK_FactPedidos_Fecha FOREIGN KEY (Fecha) REFERENCES dbo.DimFechas(Fecha),
	CONSTRAINT FK_FactPedidos_Estado FOREIGN KEY (EstadoID) REFERENCES dbo.DimEstadosPedido(EstadoID)
);

CREATE TABLE dbo.FactDetallesPedido (
	DetalleID BIGINT IDENTITY(1,1) PRIMARY KEY,
	PedidoID BIGINT NOT NULL,
	ProductoID INT NOT NULL,
	Cantidad INT NOT NULL,
	PrecioUnitario DECIMAL(18,2) NOT NULL,
	Total DECIMAL(18,2) NOT NULL,
	CONSTRAINT FK_FactDetallesPedido_Pedido FOREIGN KEY (PedidoID) REFERENCES dbo.FactPedidos(PedidoID),
	CONSTRAINT FK_FactDetallesPedido_Producto FOREIGN KEY (ProductoID) REFERENCES dbo.DimProductos(ProductoID)
);

CREATE TABLE dbo.FactFacturacion (
	FacturaID BIGINT IDENTITY(1,1) PRIMARY KEY,
	PedidoID BIGINT NOT NULL,
	Fecha DATE NOT NULL,
	Total DECIMAL(18,2) NOT NULL,
	CONSTRAINT FK_FactFacturacion_Pedido FOREIGN KEY (PedidoID) REFERENCES dbo.FactPedidos(PedidoID),
	CONSTRAINT FK_FactFacturacion_Fecha FOREIGN KEY (Fecha) REFERENCES dbo.DimFechas(Fecha)
);

-- Índices para acelerar consultas analíticas
CREATE INDEX IX_FactVentas_Fecha ON dbo.FactVentas(Fecha);
CREATE INDEX IX_FactVentas_Cliente ON dbo.FactVentas(ClienteID);
CREATE INDEX IX_FactVentas_Producto ON dbo.FactVentas(ProductoID);
CREATE INDEX IX_FactPedidos_Cliente ON dbo.FactPedidos(ClienteID);
CREATE INDEX IX_FactPedidos_Estado ON dbo.FactPedidos(EstadoID);
CREATE INDEX IX_FactDetallesPedido_Pedido ON dbo.FactDetallesPedido(PedidoID);
CREATE INDEX IX_FactDetallesPedido_Producto ON dbo.FactDetallesPedido(ProductoID);
CREATE INDEX IX_FactFacturacion_Pedido ON dbo.FactFacturacion(PedidoID);
