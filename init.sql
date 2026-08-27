DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datos') THEN
      CREATE DATABASE datos;
   END IF;
END
$$;

\c datos;