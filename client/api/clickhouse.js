import { toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

export const handleConnectClickHouse = async (
  host,
  port,
  database,
  user,
  password,
  setStatus
) => {
  try {
    const response = await fetch(
      "http://localhost:8080/api/connect-clickhouse",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ host, port, database, user, password }),
      }
    );
    const result = await response.text();
    setStatus(result);
    toast.success(result);
  } catch (error) {
    setStatus("Error: " + error.message);
    toast.error(error.message);
  }
};

export const handleLoadClickHouseSchema = async (
  host,
  port,
  database,
  user,
  password,
  tableName,

  setColumns
) => {
  try {
    const params = new URLSearchParams({
      host,
      port,
      database,
      user,
      password,
      tableName,
    });
    const response = await fetch(
      `http://localhost:8080/api/clickhouse-schema?${params.toString()}`,
      {
        method: "GET",
      }
    );
    const result = await response.json();
    toast.success("Schema loaded successfully");
    if (Array.isArray(result)) {
      setColumns(result);
      toast.success(result);
    } else {
      setColumns([]);
      setStatus("Error: Unexpected response format");
    }
  } catch (error) {
    setStatus("Error: " + error.message);
    toast.error(error.message);
  }
};

export const handleLoadClickHouseTables = async (
  host,
  port,
  database,
  user,
  password,
  setTables
) => {
  try {
    const params = new URLSearchParams({
      host,
      port,
      database,
      user,
      password,
    });
    const response = await fetch(
      `http://localhost:8080/api/clickhouse-tables?${params.toString()}`,
      {
        method: "GET",
      }
    );
    const result = await response.json();
    setTables(result);
    toast.success("Tables loaded successfully");
  } catch (error) {
    toast.error("Error: " + error.message);
  }
};

export const handleExportClickHouseToFlatFile = async (
  tableName,
  filePath,
  selectedColumns
) => {
  console.log("Export params: ", { tableName, filePath, selectedColumns });

  if (!tableName || !filePath || selectedColumns.length === 0) {
    toast.error(
      "Error: Please provide table name, file name, and select at least one column."
    );
    return;
  }

  try {
    const response = await fetch(
      "http://localhost:8080/api/export-clickhouse-to-flatfile",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tableName: tableName.trim(),
          fileName: filePath.trim(),
          columns: selectedColumns,
        }),
      }
    );

    const result = await response.text();
    toast.success(result);
  } catch (error) {
    toast.error("Error: " + error.message);
  }
};

export const handleTableSelection = async (
  host,
  port,
  database,
  user,
  password,
  table,
  setTableName,
  setColumns
) => {
  setTableName(table);
  try {
    const params = new URLSearchParams({
      host,
      port,
      database,
      user,
      password,
      tableName: table,
    });
    const response = await fetch(
      `http://localhost:8080/api/clickhouse-schema?${params.toString()}`,
      {
        method: "GET",
      }
    );
    const result = await response.json();
    setColumns(result);
    toast.success("Schema loaded successfully");
  } catch (error) {
    toast.error("Error: " + error.message);
  }
};

export const handlePreviewData = async (
  tableName,
  selectedColumns,
  setPreviewData
) => {
  if (!tableName || selectedColumns.length === 0) {
    toast.error("Error: Please select a table and at least one column.");
    return;
  }

  try {
    const response = await fetch("http://localhost:8080/api/preview-data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        tableName: tableName.trim(),
        columns: selectedColumns,
      }),
    });

    const result = await response.json();
    setPreviewData(result);
    toast.success("Preview data loaded successfully.");
  } catch (error) {
    toast.error("Error: " + error.message);
  }
};

export const handleIngestFlatFile = async (
  filePath,
  selectedColumns,
  tableName
) => {
  if (!filePath) {
    toast.error("Error: Please select a file.");
    return;
  }

  if (selectedColumns.length === 0) {
    toast.error("Error: Please select at least one column.");
    return;
  }

  try {
    const formData = new FormData();
    formData.append("file", filePath);
    formData.append("target", tableName);

    const response = await fetch("http://localhost:8080/api/ingest-data", {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(errorText);
    }

    const result = await response.json();

    if (result.error) {
      toast.error("Error: " + result.error);
    } else {
      toast.success(
        `Ingestion completed successfully. Total records: ${result.totalRecords}`
      );
    }
  } catch (error) {
    toast.error("Error: " + error.message);
  }
};
