This directory holds TPC-DS query `.sql` files (e.g. `q1.sql` .. `q99.sql`).

- Generate them from the vendored kit: `TPCDS_DIALECT=spark TPCDS_SCALE=1 ./tpcds/gen-queries.sh`
- Or place your own `.sql` files here.

By default, generated queries are ignored by git (`.gitignore`) to avoid licensing / policy issues.
