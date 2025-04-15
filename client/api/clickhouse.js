export const connectToClickHouse = async (
  host,
  port,
  database,
  user,
  password
) => {
  const response = await fetch("http://localhost:8080/api/connect-clickhouse", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ host, port, database, user, password }),
  });

  if (!response.ok) {
    throw new Error(`Failed to connect to ClickHouse: ${response.statusText}`);
  }

  return response.text();
};

export const getClickHouseSchema = async (
  host,
  port,
  database,
  user,
  password,
  tableName
) => {
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

  if (!response.ok) {
    throw new Error(
      `Failed to fetch ClickHouse schema: ${response.statusText}`
    );
  }

  return response.json();
};

export const exportClickHouseToFlatFile = async (tableName, fileName) => {
  const response = await fetch(
    "http://localhost:8080/api/export-clickhouse-to-flatfile",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tableName, fileName }),
    }
  );

  if (!response.ok) {
    throw new Error(`Failed to export data: ${response.statusText}`);
  }

  return response.text();
};
