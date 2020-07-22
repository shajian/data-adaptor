# Descriptions
This Project contains the following task:
1. Company browsed frequency statistics (Statistic)
2. data-writing into Elasticsearch & Mongodb (Write)
3. Elasticsearch & Mongodb data accessing API (API)
## 1. Statistic
Statisticing company browsed frequency is used to optimized company score, which can
help to rank the companies returned by Elasticsearch searching. This task should be 
firstly launched and completed. After this, the task can be executed at a regular date
to update company frequencies. However, frequencies are not needed to be updated in time.

## 2. Write
Writing data from RMDB to Elasticsearch & Mongodb. Data stored in Elasticsearch just
contains parts of fields that are necessary for searching. For retrieving other fields
for a company document, it is the role Mongodb acts as.

## 3. API
Use spring-boot to provide a plenty of useful RESTful APIs. 
 
 
# Build
Library conflicting often happens. You can add `<exclusions>` under a `dependency` node 
in file `pom.xml`. Use
```$xslt
mvn dependency:tree
```
to get conflict information.

We use `maven-assembly-plugin` to customize the building process. The descriptor file
is located at `src/main/assembly/assembly.xml`.

## 1. Statistic
## 2. Write
## 3. API
run the application directly by 
```
mvn spring-boot:runy
```
build the jar file
```
mvn clean package
```
(If want to skip unit test, add option `-Dmaven.skip.test=true` or `-DskipTests=true`)

Sometimes, we want to rebuild this project itself after some source code modification, without any dependency changes,
then do
~~~```
mvn compile
```~~~
and substitute the single new jar file.

Now add scala module in this project, and to package this project, please use
```
mvn clean scala:compile compile package
```
in the above command, add `scala:compile` before `compile` when packaging, if not do this, maven will not
packages any scala module.

This command may generate two jar files `data-adapter-x.x.jar` 
and `data-adapter-x.x.jar.original`. The latter is original jar
file and the former is additionally use `com.qianzhan.qichamao.app.Application`
as the program entry.

run the application outside
```
java -jar target/data-adapter-x.x.jar
```

# run
Some scripts are provided under `bin` for running programs. You can modify `JAVA_OPTS`
for optimize jvm performance.
