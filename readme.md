
# Build
## 1
Library conflicting often happens. You can add `<exclusions>` under a `dependency` node 
in file `pom.xml`. Use
```$xslt
mvn dependency:tree
```
to get conflict information.

## 2
The following command is used to pack source file(no compilation) into a jar:
```
mvn clean source:jar
```

## 3
If only try to pack all source code(compiled, i.e. class file) into a jar:
```
mvn clean scala:compile compiler:compile jar:jar
```
This method will generate a relatively small jar file, and save time for generation from ~48s to ~27s.

## 4
Build the jar file. As default it will pack source code and all dependencies into one jar, which may be a slow process.
```
mvn clean package
```
(If want to skip unit test, add option `-Dmaven.skip.test=true` or `-DskipTests=true`)

This command may generate two jar files `data-adapter-x.x.jar`
and `data-adapter-x.x.jar.original`.
We use `maven-assembly-plugin` to customize the building process. The descriptor file
is located at `src/main/assembly/assembly.xml`. This is used to zip all jar files.


## 5
Because we mix scala and java together, following closely `mvn` command, add `scala:compile` when scala code can not
be compiled, e.g.
```
mvn clean scala:compile compile package
```
in the above command, add `scala:compile` before `compile` when packaging, if not do this, maven will not
packages any scala module.

## 6
run the web application directly(start quickly) by
```
mvn spring-boot:run
```

## 7
run arbitrary application outside
```
java -jar target/data-adapter-x.x.jar
```
or you can use scripts in `\bin` directory:
```
.\start.bat
```
You can modify `JAVA_OPTS` for optimize jvm performance.

