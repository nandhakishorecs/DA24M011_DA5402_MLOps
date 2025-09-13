[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/uECeo1no)

Author: **Nandhakishore C S** \
Roll Number: **DA24M011** \
Details to execute the given web scrapping module using Apache Airflow: 

**Update**: 
Use the file **DA24M011_DAG1_DAG2_Combined.py** file for execution. This single python file will create two DAGS. 

1. Go to directory where the docker-compose.yaml file is situated for airflow. 
2. Open the **docker-compose.yaml** file and map the port number of postgres service, 5432:54320 
3. Update the databse configurations: \
    **'database'**: 'airflow', \
    **'username'**: 'airflow', \
    **'password'**: 'airflow; 
4. Open CLI in the same directory as 1 and do **$ docker compose up** - Airflow should be up and running. 
5. Go to [localhost](http://localhost:8080) -> Admin -> Connections -> new connection and configure the connection to postgres by updating the fields: \
    **Connection ID**: tutorial_pg_conn \
    **Connection Type**: Postgres \
    **Host**: host.docker.internal \
    **Databse**: airflow \
    **Login**: airflow \
    **Port**: 54320 
6. Use a databse viewer (DBeaver in my case) and establish connection by using the same parameters as above. 
7. Go to your gmail account and get your 2 Factor Authentiation key and paste it in DAG 2. Add the recepient and sender email IDs. 
8. Go to /usr/dags/ directory and place the DAG files there. 
9. Go to [localhost](http://localhost:8080) and run both DAGs. The output will be shown in DBeaver (databse entries) and the new entries will be emailed to the recepient email ID from the send email ID. 