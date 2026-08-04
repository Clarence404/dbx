export type InfluxDbVersion = "1" | "2" | "3";

export type InfluxDb3Transport = "flightsql" | "http";

export interface InfluxDbExternalConfig {
  version?: InfluxDbVersion;
  /** InfluxDB 2.x organization. */
  org?: string;
  /** InfluxDB 3.x default database (informational — the driver reads `database` off the connection). */
  database?: string;
  /** InfluxDB 3.x transport selector. Defaults to Flight SQL when unset. */
  transport?: InfluxDb3Transport;
}
