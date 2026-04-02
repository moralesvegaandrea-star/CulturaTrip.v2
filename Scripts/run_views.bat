docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/04_new_tables.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/05_new_views.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/06_index.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/07_new_changes.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/08_alter_tables.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/09_ML_views.sql