/*
	Base de datos operativa (SQL Server)
	- customers.csv
	- products.csv
	- orders.csv
	- order_details.csv
*/

SET NOCOUNT ON;
GO

IF DB_ID(N'ProcesoETLDB') IS NULL
BEGIN
	CREATE DATABASE ProcesoETLDB;
END;
GO

USE ProcesoETLDB;
GO

IF OBJECT_ID(N'dbo.DataSources', N'U') IS NULL
BEGIN
	CREATE TABLE dbo.DataSources (
		SourceID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_DataSources PRIMARY KEY,
		SourceType VARCHAR(50) NOT NULL,
		Description VARCHAR(200) NULL,
		CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_DataSources_CreatedAt DEFAULT (SYSDATETIME())
	);
END;
GO

IF OBJECT_ID(N'dbo.Customers', N'U') IS NULL
BEGIN
	CREATE TABLE dbo.Customers (
		CustomerID INT NOT NULL CONSTRAINT PK_Customers PRIMARY KEY,
		FirstName VARCHAR(100) NOT NULL,
		LastName VARCHAR(100) NOT NULL,
		Email VARCHAR(200) NULL,
		Phone VARCHAR(60) NULL,
		City VARCHAR(120) NULL,
		Country VARCHAR(120) NULL,
		CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_Customers_CreatedAt DEFAULT (SYSDATETIME())
	);
END;
GO

IF OBJECT_ID(N'dbo.Products', N'U') IS NULL
BEGIN
	CREATE TABLE dbo.Products (
		ProductID INT NOT NULL CONSTRAINT PK_Products PRIMARY KEY,
		ProductName VARCHAR(200) NOT NULL,
		Category VARCHAR(100) NULL,
		Price DECIMAL(18,2) NOT NULL CONSTRAINT CK_Products_Price CHECK (Price >= 0),
		Stock INT NOT NULL CONSTRAINT CK_Products_Stock CHECK (Stock >= 0),
		CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_Products_CreatedAt DEFAULT (SYSDATETIME())
	);
END;
GO

IF OBJECT_ID(N'dbo.Orders', N'U') IS NULL
BEGIN
	CREATE TABLE dbo.Orders (
		OrderID INT NOT NULL CONSTRAINT PK_Orders PRIMARY KEY,
		CustomerID INT NOT NULL,
		OrderDate DATE NOT NULL,
		Status VARCHAR(20) NOT NULL CONSTRAINT CK_Orders_Status CHECK (Status IN ('Pending', 'Shipped', 'Delivered', 'Cancelled')),
		CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_Orders_CreatedAt DEFAULT (SYSDATETIME()),

		CONSTRAINT FK_Orders_Customers
			FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
	);
END;
GO

IF OBJECT_ID(N'dbo.OrderDetails', N'U') IS NULL
BEGIN
	CREATE TABLE dbo.OrderDetails (
		OrderDetailID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OrderDetails PRIMARY KEY,
		OrderID INT NOT NULL,
		ProductID INT NOT NULL,
		Quantity INT NOT NULL CONSTRAINT CK_OrderDetails_Quantity CHECK (Quantity > 0),
		TotalPrice DECIMAL(18,2) NOT NULL CONSTRAINT CK_OrderDetails_TotalPrice CHECK (TotalPrice >= 0),
		CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_OrderDetails_CreatedAt DEFAULT (SYSDATETIME()),

		CONSTRAINT FK_OrderDetails_Orders
			FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID),
		CONSTRAINT FK_OrderDetails_Products
			FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
	);
END;
GO

IF OBJECT_ID(N'dbo.Invoices', N'U') IS NULL
BEGIN
	CREATE TABLE dbo.Invoices (
		InvoiceID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Invoices PRIMARY KEY,
		OrderID INT NOT NULL,
		InvoiceDate DATE NOT NULL,
		TotalAmount DECIMAL(18,2) NOT NULL CONSTRAINT CK_Invoices_TotalAmount CHECK (TotalAmount >= 0),
		CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_Invoices_CreatedAt DEFAULT (SYSDATETIME()),

		CONSTRAINT FK_Invoices_Orders
			FOREIGN KEY (OrderID) REFERENCES dbo.Orders(OrderID)
	);
END;
GO

IF OBJECT_ID(N'dbo.StgCsvExtract', N'U') IS NOT NULL
BEGIN
	DROP TABLE dbo.StgCsvExtract;
END;
GO

CREATE TABLE dbo.StgCsvExtract (
	StagingID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_StgCsvExtract PRIMARY KEY,
	BatchId UNIQUEIDENTIFIER NOT NULL,
	EntityName VARCHAR(120) NOT NULL,
	SourceDetail NVARCHAR(MAX) NULL,
	PayloadJson NVARCHAR(MAX) NOT NULL,
	ExtractedAt DATETIME2 NOT NULL CONSTRAINT DF_StgCsvExtract_ExtractedAt DEFAULT (SYSDATETIME())
);
GO

IF OBJECT_ID(N'dbo.StgDbExtract', N'U') IS NOT NULL
BEGIN
	DROP TABLE dbo.StgDbExtract;
END;
GO

CREATE TABLE dbo.StgDbExtract (
	StagingID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_StgDbExtract PRIMARY KEY,
	BatchId UNIQUEIDENTIFIER NOT NULL,
	EntityName VARCHAR(120) NOT NULL,
	SourceDetail NVARCHAR(MAX) NULL,
	PayloadJson NVARCHAR(MAX) NOT NULL,
	ExtractedAt DATETIME2 NOT NULL CONSTRAINT DF_StgDbExtract_ExtractedAt DEFAULT (SYSDATETIME())
);
GO

IF OBJECT_ID(N'dbo.StgApiExtract', N'U') IS NOT NULL
BEGIN
	DROP TABLE dbo.StgApiExtract;
END;
GO

CREATE TABLE dbo.StgApiExtract (
	StagingID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_StgApiExtract PRIMARY KEY,
	BatchId UNIQUEIDENTIFIER NOT NULL,
	EntityName VARCHAR(120) NOT NULL,
	SourceDetail NVARCHAR(MAX) NULL,
	PayloadJson NVARCHAR(MAX) NOT NULL,
	ExtractedAt DATETIME2 NOT NULL CONSTRAINT DF_StgApiExtract_ExtractedAt DEFAULT (SYSDATETIME())
);
GO

IF OBJECT_ID(N'dbo.ExtractionAudit', N'U') IS NOT NULL
BEGIN
	DROP TABLE dbo.ExtractionAudit;
END;
GO

CREATE TABLE dbo.ExtractionAudit (
	AuditID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ExtractionAudit PRIMARY KEY,
	BatchId UNIQUEIDENTIFIER NOT NULL,
	Component VARCHAR(120) NOT NULL,
	SourceType VARCHAR(50) NOT NULL,
	EntityName VARCHAR(120) NOT NULL,
	RowsExtracted INT NOT NULL CONSTRAINT DF_ExtractionAudit_RowsExtracted DEFAULT (0),
	DurationMs INT NOT NULL CONSTRAINT DF_ExtractionAudit_DurationMs DEFAULT (0),
	Status VARCHAR(20) NOT NULL,
	ErrorMessage NVARCHAR(1000) NULL,
	CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_ExtractionAudit_CreatedAt DEFAULT (SYSDATETIME())
);
GO

/*
  se cuencia para ver los query en si 
*/

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_Orders_CustomerID'
	  AND object_id = OBJECT_ID(N'dbo.Orders')
)
BEGIN
	CREATE INDEX IX_Orders_CustomerID ON dbo.Orders(CustomerID);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_Orders_OrderDate'
	  AND object_id = OBJECT_ID(N'dbo.Orders')
)
BEGIN
	CREATE INDEX IX_Orders_OrderDate ON dbo.Orders(OrderDate);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_OrderDetails_OrderID'
	  AND object_id = OBJECT_ID(N'dbo.OrderDetails')
)
BEGIN
	CREATE INDEX IX_OrderDetails_OrderID ON dbo.OrderDetails(OrderID);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_OrderDetails_ProductID'
	  AND object_id = OBJECT_ID(N'dbo.OrderDetails')
)
BEGIN
	CREATE INDEX IX_OrderDetails_ProductID ON dbo.OrderDetails(ProductID);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'UX_Invoices_OrderID'
	  AND object_id = OBJECT_ID(N'dbo.Invoices')
)
BEGIN
	CREATE UNIQUE INDEX UX_Invoices_OrderID ON dbo.Invoices(OrderID);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_StgCsvExtract_BatchId'
	  AND object_id = OBJECT_ID(N'dbo.StgCsvExtract')
)
BEGIN
	CREATE INDEX IX_StgCsvExtract_BatchId ON dbo.StgCsvExtract(BatchId);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_StgDbExtract_BatchId'
	  AND object_id = OBJECT_ID(N'dbo.StgDbExtract')
)
BEGIN
	CREATE INDEX IX_StgDbExtract_BatchId ON dbo.StgDbExtract(BatchId);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_StgApiExtract_BatchId'
	  AND object_id = OBJECT_ID(N'dbo.StgApiExtract')
)
BEGIN
	CREATE INDEX IX_StgApiExtract_BatchId ON dbo.StgApiExtract(BatchId);
END;
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.indexes
	WHERE name = N'IX_ExtractionAudit_BatchId'
	  AND object_id = OBJECT_ID(N'dbo.ExtractionAudit')
)
BEGIN
	CREATE INDEX IX_ExtractionAudit_BatchId ON dbo.ExtractionAudit(BatchId);
END;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.DataSources WHERE SourceType = 'CSV')
BEGIN
	INSERT INTO dbo.DataSources (SourceType, Description)
	VALUES ('CSV', 'Datos importados desde archivos CSV de la carpeta ProcesoEtl/csv');
END;

IF NOT EXISTS (SELECT 1 FROM dbo.DataSources WHERE SourceType = 'API')
BEGIN
	INSERT INTO dbo.DataSources (SourceType, Description)
	VALUES ('API', 'Datos importados desde servicios externos');
END;

IF NOT EXISTS (SELECT 1 FROM dbo.DataSources WHERE SourceType = 'SQL')
BEGIN
	INSERT INTO dbo.DataSources (SourceType, Description)
	VALUES ('SQL', 'Datos importados desde otras bases de datos SQL Server');
END;
GO

SELECT 'DataSources' AS TableName, COUNT(*) AS RowsCount FROM dbo.DataSources
UNION ALL
SELECT 'Customers', COUNT(*) FROM dbo.Customers
UNION ALL
SELECT 'Products', COUNT(*) FROM dbo.Products
UNION ALL
SELECT 'Orders', COUNT(*) FROM dbo.Orders
UNION ALL
SELECT 'OrderDetails', COUNT(*) FROM dbo.OrderDetails
UNION ALL
SELECT 'Invoices', COUNT(*) FROM dbo.Invoices
UNION ALL
SELECT 'StgCsvExtract', COUNT(*) FROM dbo.StgCsvExtract
UNION ALL
SELECT 'StgDbExtract', COUNT(*) FROM dbo.StgDbExtract
UNION ALL
SELECT 'StgApiExtract', COUNT(*) FROM dbo.StgApiExtract
UNION ALL
SELECT 'ExtractionAudit', COUNT(*) FROM dbo.ExtractionAudit;
GO
