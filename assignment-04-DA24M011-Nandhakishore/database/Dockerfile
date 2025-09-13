FROM postgres:15
ENV POSTGRES_USER=news_admin
ENV POSTGRES_PASSWORD=securepassword
ENV POSTGRES_DB=news_db
COPY init.sql /docker-entrypoint-initdb.d/
