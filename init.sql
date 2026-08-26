DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'amn_datawarehouse') THEN
      CREATE DATABASE amn_datawarehouse;
   END IF;
END
$$;

\c amn_datawarehouse;