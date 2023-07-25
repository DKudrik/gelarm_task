1. Для запуска проекта необходимо ввести команду в командной строке
   _sudo docker-compose up -d --build_
  
2. Далеее для создания необходимых таблиц в БД нужно ввести команду 
   _sudo docker exec -it gelarm psql -h 127.0.0.1 -U gelarm -d gelarm_db -f gelarm_tables.ddl_