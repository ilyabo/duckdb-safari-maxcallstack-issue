import type { NextPage } from "next";
import { useEffect, useState } from "react";

import * as duckdb from "@duckdb/duckdb-wasm";

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

const N = 100;
const PARALLELISM = 10;
const QUERY = `
SELECT
    l.id,
    l.name,
    l.lat,
    l.lon,
    tin.total_in,
    tout.total_out,
    tout.total_int,
    GREATEST(tin.total_in, tout.total_out) AS total_max
FROM 
    locations_12 l 
LEFT JOIN (
  SELECT
      origin,
      SUM(CASE WHEN origin <> dest THEN count ELSE 0 END) AS total_out,
      SUM(CASE WHEN origin = dest THEN count ELSE 0 END) AS total_int
  FROM
      flows_12 f
  WHERE 
    (TRUE AND (origin IN ('72006') OR dest IN ('72006')) AND TRUE)
  GROUP BY
      origin
) tout ON l.id = tout.origin

LEFT JOIN (
  SELECT
      dest,
      SUM(CASE WHEN origin <> dest THEN count ELSE 0 END) AS total_in
  FROM
      flows_12 f
  WHERE 
    (TRUE AND (origin IN ('72006') OR dest IN ('72006')) AND TRUE)
  GROUP BY
      dest
) tin ON l.id = tin.dest

WHERE
    l.lat BETWEEN 40.22971369851149 AND 42.03780879324967
    AND l.lon BETWEEN 14.978306190131324 AND 17.392598457924283
ORDER BY total_max ASC
`;

const Home: NextPage = () => {
  // const { conn, db } = useDuckConn();

  const [status, setStatus] = useState<string>("");
  useEffect(() => {
    (async () => {
      const location = globalThis.document?.location;
      if (!location) return;
      const ROOT_URL = `${location.protocol}//${location.hostname}:${location.port}`;
      const FLOWS_URL = `${ROOT_URL}/data/flows.parquet`;
      const LOCATIONS_URL = `${ROOT_URL}/data/locations.parquet`;

      setStatus("Loading…");
      // Select a bundle based on browser checks
      const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker!}");`], {
          type: "text/javascript",
        })
      );

      // Instantiate the asynchronus version of DuckDB-wasm
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger();
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      URL.revokeObjectURL(worker_url);

      await db.open({
        path: ":memory:",
        query: {
          castBigIntToDouble: true,
        },
      });
      const conn = await db.connect();

      await conn.query(`CREATE TABLE flows_12 AS SELECT * FROM '${FLOWS_URL}'`);
      await conn.query(
        `CREATE TABLE locations_12 AS SELECT * FROM '${LOCATIONS_URL}'`
      );

      setStatus((stage) => `${stage}\nData loaded`);

      for (let i = 0; i < N; i++) {
        await Promise.all(
          range(PARALLELISM).map((i) => {
            return conn.query(QUERY);
          })
        );
        setStatus((stage) => `${stage}\nBatch ${i + 1} completed`);
      }
    })();
  }, []);

  return <pre>{status}</pre>;
};

function range(n: number) {
  const rv = [];
  for (let i = 0; i < n; i++) rv.push(i);
  return rv;
}

export default Home;
