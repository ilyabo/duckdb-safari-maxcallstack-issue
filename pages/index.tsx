import type { NextPage } from "next";
import { useEffect, useState } from "react";

import * as duckdb from "@duckdb/duckdb-wasm";

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

const N = 1000;
const QUERY = `
SELECT GREATEST(tin.total_in, tout.total_out) AS total_max FROM locations l 

LEFT JOIN (
  SELECT origin, SUM(count) AS total_out FROM flows f 
  WHERE origin <> dest GROUP BY origin
) tout ON l.id = tout.origin

LEFT JOIN (
  SELECT dest, SUM(count) AS total_in FROM flows f 
  WHERE origin <> dest GROUP BY dest
) tin ON l.id = tin.dest

WHERE l.lat BETWEEN 40.22 AND 42.03 AND l.lon BETWEEN 14.97 AND 17.39
ORDER BY total_max ASC
`;

const Home: NextPage = () => {
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

      const conn = await db.connect();

      await conn.query(`CREATE TABLE flows AS SELECT * FROM '${FLOWS_URL}'`);
      await conn.query(
        `CREATE TABLE locations AS SELECT * FROM '${LOCATIONS_URL}'`
      );

      setStatus("");

      try {
        for (let i = 0; i < N; i++) {
          await conn.query(QUERY);
          setStatus((prev) => `${prev} ${i + 1}`);
        }
      } catch (err) {
        setStatus((prev) => `${prev} ERROR!!!\n${err}`);
      }
    })();
  }, []);

  return <div style={{ fontFamily: "sans-serif" }}>{status}</div>;
};

export default Home;
