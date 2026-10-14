-- Expected output for each orphan/duplicate query: zero rows.
SELECT patient_id, COUNT(*) FROM patients GROUP BY patient_id HAVING COUNT(*) > 1;
SELECT doctor_id, COUNT(*) FROM doctors GROUP BY doctor_id HAVING COUNT(*) > 1;
SELECT a.appointment_id, a.patient_id FROM appointments a
LEFT JOIN patients p ON p.patient_id=a.patient_id WHERE p.patient_id IS NULL;
SELECT a.appointment_id, a.doctor_id FROM appointments a
LEFT JOIN doctors d ON d.doctor_id=a.doctor_id WHERE d.doctor_id IS NULL;
SELECT t.treatment_id, t.appointment_id FROM treatments t
LEFT JOIN appointments a ON a.appointment_id=t.appointment_id
WHERE a.appointment_id IS NULL;
SELECT b.billing_id, b.treatment_id FROM billing b
LEFT JOIN treatments t ON t.treatment_id=b.treatment_id
WHERE t.treatment_id IS NULL;
SELECT 'patients' AS table_name, COUNT(*) AS row_count FROM patients
UNION ALL SELECT 'doctors', COUNT(*) FROM doctors
UNION ALL SELECT 'appointments', COUNT(*) FROM appointments
UNION ALL SELECT 'treatments', COUNT(*) FROM treatments
UNION ALL SELECT 'billing', COUNT(*) FROM billing;
-- Negative values may represent refunds in some systems; confirm business rules.
SELECT treatment_id, treatment_cost FROM treatments WHERE treatment_cost < 0;
SELECT billing_id, amount FROM billing WHERE amount < 0;
