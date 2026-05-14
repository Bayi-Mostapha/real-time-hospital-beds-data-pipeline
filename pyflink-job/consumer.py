import json
import sys
import logging
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Test ClickHouse connection BEFORE starting Flink — fail loud and early
# -----------------------------------------------------------------------------
def test_clickhouse():
    import clickhouse_connect
    print("[STARTUP] Testing ClickHouse connection...", flush=True)
    try:
        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            database="hospital",
            username="user",
            password="password",
        )
        result = client.command("SELECT 1")
        print(f"[STARTUP] ClickHouse OK — ping returned: {result}", flush=True)

        tables = client.command("SHOW TABLES FROM hospital")
        print(f"[STARTUP] Tables in hospital db: {tables}", flush=True)

        client.insert(
            "vitals_raw",
            [("TEST_BED", 0, 0.0, 0.0, 0, 0.0, 0, "2000-01-01 00:00:00")],
            column_names=["bed_id","heart_rate","oxygen_sat","body_temp","blood_pressure_sys","glucose","respiration_rate","recorded_at"],
        )
        count = client.command("SELECT count() FROM hospital.vitals_raw WHERE bed_id = 'TEST_BED'")
        print(f"[STARTUP] Test insert OK — count of TEST_BED rows: {count}", flush=True)

        client.command("ALTER TABLE hospital.vitals_raw DELETE WHERE bed_id = 'TEST_BED'")
        print("[STARTUP] Test row cleaned up", flush=True)

    except Exception as e:
        print(f"[STARTUP] CLICKHOUSE FAILED: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

test_clickhouse()

# -----------------------------------------------------------------------------
# Parse Kafka JSON  ← UNCHANGED
# -----------------------------------------------------------------------------
class ParseVital(MapFunction):
    def map(self, raw: str):
        try:
            d = json.loads(raw)
            return {
                "bed_id":             str(d["bed_id"]),
                "heart_rate":         int(d["heart_rate"]),
                "oxygen_sat":         float(d["oxygen_sat"]),
                "body_temp":          float(d["body_temp"]),
                "blood_pressure_sys": int(d["blood_pressure_sys"]),
                "glucose":            float(d["glucose"]),
                "respiration_rate":   int(d["respiration_rate"]),
                "recorded_at":        str(d.get("timestamp", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))[:19],
            }
        except Exception as e:
            print(f"[PARSE ERROR] {e} | raw={raw}", flush=True)
            return None

# -----------------------------------------------------------------------------
# Raw ClickHouse sink  ← UNCHANGED
# -----------------------------------------------------------------------------
class WriteToClickHouse(MapFunction):

    def __init__(self):
        self._client = None

    def _get_client(self):
        if self._client is None:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                database="hospital",
                username="user",
                password="password",
            )
            print("[CH] Client created", flush=True)
        return self._client

    def map(self, record):
        if record is None:
            return "skip"

        print(f"[CH] Attempting insert: {record['bed_id']}", flush=True)
        try:
            self._get_client().insert(
                "vitals_raw",
                [(
                    record["bed_id"],
                    record["heart_rate"],
                    record["oxygen_sat"],
                    record["body_temp"],
                    record["blood_pressure_sys"],
                    record["glucose"],
                    record["respiration_rate"],
                    record["recorded_at"],
                )],
                column_names=[
                    "bed_id", "heart_rate", "oxygen_sat", "body_temp",
                    "blood_pressure_sys", "glucose", "respiration_rate", "recorded_at",
                ],
            )
            print(f"[CH] INSERT OK: {record['bed_id']}", flush=True)
        except Exception as e:
            print(f"[CH ERROR] {type(e).__name__}: {e}", flush=True)
            import traceback
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()

        return f"ok:{record['bed_id']}"

# -----------------------------------------------------------------------------
# 5-minute tumbling window — compute averages per bed_id
# -----------------------------------------------------------------------------
class VitalsWindowAgg(ProcessWindowFunction):
    """Called once per (bed_id, window) when the 5-min window closes."""

    def process(self, bed_id, context, elements):
        records = list(elements)
        n = len(records)
        if n == 0:
            return

        w_start = datetime.fromtimestamp(
            context.window().start / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        w_end = datetime.fromtimestamp(
            context.window().end / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

        yield {
            "bed_id":         bed_id,
            "avg_heart_rate": sum(r["heart_rate"]         for r in records) / n,
            "avg_oxygen_sat": sum(r["oxygen_sat"]         for r in records) / n,
            "avg_body_temp":  sum(r["body_temp"]          for r in records) / n,
            "avg_bp_sys":     sum(r["blood_pressure_sys"] for r in records) / n,
            "avg_glucose":    sum(r["glucose"]            for r in records) / n,
            "avg_resp_rate":  sum(r["respiration_rate"]   for r in records) / n,
            "count":          n,
            "window_start":   w_start,
            "window_end":     w_end,
        }


class WriteAggToClickHouse(MapFunction):
    """Writes one aggregated row per closed window to vitals_agg."""

    def __init__(self):
        self._client = None

    def _get_client(self):
        if self._client is None:
            import clickhouse_connect
            self._client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                database="hospital",
                username="user",
                password="password",
            )
            print("[AGG-CH] Client created", flush=True)
        return self._client

    def map(self, record):
        print(f"[AGG] Window closed: {record['bed_id']} "
              f"n={record['count']} "
              f"hr={record['avg_heart_rate']:.1f} "
              f"{record['window_start']} → {record['window_end']}", flush=True)
        try:
            self._get_client().insert(
                "vitals_agg",
                [(
                    record["bed_id"],
                    record["avg_heart_rate"],
                    record["avg_oxygen_sat"],
                    record["avg_body_temp"],
                    record["avg_bp_sys"],
                    record["avg_glucose"],
                    record["avg_resp_rate"],
                    record["count"],
                    record["window_start"],
                    record["window_end"],
                )],
                column_names=[
                    "bed_id", "avg_heart_rate", "avg_oxygen_sat", "avg_body_temp",
                    "avg_bp_sys", "avg_glucose", "avg_resp_rate",
                    "record_count", "window_start", "window_end",
                ],
            )
            print(f"[AGG-CH] INSERT OK: {record['bed_id']} window={record['window_start']}", flush=True)
        except Exception as e:
            print(f"[AGG-CH ERROR] {type(e).__name__}: {e}", flush=True)
            import traceback
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()

        return f"agg_ok:{record['bed_id']}"

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
print("[MAIN] Starting PyFlink job...", flush=True)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("hospital_vitals")
    .set_group_id("flink-ch-group")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .set_property("allow.auto.create.topics", "true")
    .build()
)

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

# Parse once, reuse for both branches
parsed = (
    stream
    .map(ParseVital())
    .filter(lambda x: x is not None)
)

# --- Branch 1: raw insert (unchanged) ---
parsed.map(WriteToClickHouse()).print()

# --- Branch 2: 5-min window aggregation per bed ---
(
    parsed
    .key_by(lambda r: r["bed_id"])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
    .process(VitalsWindowAgg())
    .map(WriteAggToClickHouse())
    .print()
)

print("[MAIN] Submitting job...", flush=True)
env.execute("hospital_vitals → ClickHouse")