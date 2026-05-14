CREATE DATABASE IF NOT EXISTS hospital;

CREATE TABLE IF NOT EXISTS hospital.vitals_raw (
    bed_id              String,
    heart_rate          Int32,
    oxygen_sat          Float64,
    body_temp           Float64,
    blood_pressure_sys  Int32,
    glucose             Float64,
    respiration_rate    Int32,
    recorded_at         String          -- keep as String, producer sends ISO timestamp
) ENGINE = MergeTree()
ORDER BY (recorded_at, bed_id);

CREATE TABLE IF NOT EXISTS hospital.vitals_agg (
    bed_id          String,
    avg_heart_rate  Float64,
    avg_oxygen_sat  Float64,
    avg_body_temp   Float64,
    avg_bp_sys      Float64,
    avg_glucose     Float64,
    avg_resp_rate   Float64,
    record_count    UInt32,
    window_start    String,
    window_end      String
) ENGINE = ReplacingMergeTree()
ORDER BY (window_start, bed_id);