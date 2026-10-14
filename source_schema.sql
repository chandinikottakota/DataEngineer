-- TEMPLATE ONLY: compare with existing RDS tables before executing.
-- Do not drop/recreate existing tables to match this template.
CREATE TABLE IF NOT EXISTS patients (
  patient_id BIGINT PRIMARY KEY, patient_name VARCHAR(200),
  gender VARCHAR(30), date_of_birth DATE, phone VARCHAR(30), city VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS doctors (
  doctor_id BIGINT PRIMARY KEY, doctor_name VARCHAR(200), specialty VARCHAR(150)
);
CREATE TABLE IF NOT EXISTS appointments (
  appointment_id BIGINT PRIMARY KEY,
  patient_id BIGINT NOT NULL REFERENCES patients(patient_id),
  doctor_id BIGINT NOT NULL REFERENCES doctors(doctor_id),
  appointment_date TIMESTAMP, appointment_status VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS treatments (
  treatment_id BIGINT PRIMARY KEY,
  appointment_id BIGINT NOT NULL REFERENCES appointments(appointment_id),
  treatment_name VARCHAR(200), treatment_cost NUMERIC(12,2)
);
CREATE TABLE IF NOT EXISTS billing (
  billing_id BIGINT PRIMARY KEY,
  treatment_id BIGINT NOT NULL REFERENCES treatments(treatment_id),
  billing_date DATE, amount NUMERIC(12,2), payment_status VARCHAR(50)
);
-- Update names/types to match your actual source schema.
