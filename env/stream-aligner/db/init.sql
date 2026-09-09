-- Logical databases for the three independently deployable services.
-- In production these would typically live in separate clusters; the compose
-- setup keeps one Postgres container for convenience while each service only
-- ever touches its own database.
CREATE DATABASE stream_a;
CREATE DATABASE stream_b;
CREATE DATABASE aligner;
