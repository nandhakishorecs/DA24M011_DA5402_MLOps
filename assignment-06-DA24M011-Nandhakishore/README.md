[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/232lQ-Vi)


**Submitted by**: Nandhakishore C S <br>
**Roll Number**: DA24M011 

### Setup 
- Make sure that the system has docker installed. 
- Createa a docker image using the docker file given in this repository with a custom name
```console
$ docker build -t prometheus-exporter .
```
- Run the docker image with port mapping
```console
$ docker run -d --name prometheus-exporter -p 18000:18000 prometheus-exporter
```
- Once the docker is started, the program will automatically look for metrics in the iostat command and also from /prec/meminfo file. 

- To enable prometheus to get these metrics as queries, add the following jon under the scrape_configs section in the prometheus.yml file. 
```yaml 
scrape_configs:
  - job_name: "mlops_a6_exporter"
    scrape_interval: 5s 
    static_configs: 
      - targets: ["localhost:18000"]
```
- Once the changes are done in prometheus.yml file, restart the prometheus server
