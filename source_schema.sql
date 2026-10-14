
CREATE TABLE patients (
    patient_id INTEGER PRIMARY KEY,
    patient_name VARCHAR(100),
    gender VARCHAR(20),
    date_of_birth DATE
);

CREATE TABLE doctors (
    doctor_id INTEGER PRIMARY KEY,
    doctor_name VARCHAR(100),
    specialization VARCHAR(100)
);

CREATE TABLE appointments (
    appointment_id INTEGER PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    doctor_id INTEGER REFERENCES doctors(doctor_id),
    appointment_date TIMESTAMP,
    status VARCHAR(30)
);

CREATE TABLE treatments (
    treatment_id INTEGER PRIMARY KEY,
    appointment_id INTEGER REFERENCES appointments(appointment_id),
    treatment_name VARCHAR(100),
    treatment_cost NUMERIC(10,2)
);

CREATE TABLE billing (
    billing_id INTEGER PRIMARY KEY,
    treatment_id INTEGER REFERENCES treatments(treatment_id),
    amount NUMERIC(10,2),
    payment_status VARCHAR(30)
);

