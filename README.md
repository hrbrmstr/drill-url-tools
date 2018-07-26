# drill-url-tools

A set of Apache Drill UDFs for working with URLs

It uses the [`galimatias`](http://galimatias.mola.io/) Java library for the parsing.

## UDFs

The following UDFs are included:

- `url_parse(url-string)`: Given a URL/URI string input, a set of fields (`url`, `scheme`, `username`, `password`, `host`, `port`, path`, `query` and `fragment`) will be returned in a map.

## Building

Retrieve the dependencies and build the UDF:

```
make deps
make udf
```

To automatically install it locally, ensure `DRILL_HOME` is set (the `Makefile` has a default of `/usr/local/drill`) and:

```
make install
```

Assuming you're running in standalone mode, you can then do:

```
make restart
```

You can manually copy:

- `target/drill-url-tools-1.0.jar`
- `target/drill-url-tools-1.0-sources.jar`
- `deps/galimatias-0.2.0.jar`
- `deps/icu4j-53.1.jar`

(after a successful build) to your `$DRILL_HOME/jars/3rdparty` directory and manually restart Drill as well.

## Example

Using the following query:

```
SELECT
  a.url AS url,
  a.rec.scheme AS scheme,
  a.rec.username AS username,
  a.rec.password AS password,
  a.rec.host AS host,
  a.rec.port AS port,
  a.rec.path AS path,
  a.rec.query AS query,
  a.rec.fragment AS fragment
FROM
  (SELECT url, url_parse(url) AS rec
  FROM
    (SELECT 'https://www.test.url/something/a.cgi?first=no#frag' AS url
     FROM (VALUES((1))))) a;
```

Here's the output:

```
$ drill-conf
apache drill 1.14.0-SNAPSHOT
"this isn't your grandfather's sql"
0: jdbc:drill:> !set outputFormat vertical
0: jdbc:drill:> SELECT
. . . . . . . >   a.url AS url,
. . . . . . . >   a.rec AS rec
. . . . . . . > FROM
. . . . . . . >   (SELECT url, url_parse(url) AS rec
. . . . . . . >   FROM
. . . . . . . >     (SELECT 'https://www.test.url/something/a.cgi?first=no#frag' AS url
. . . . . . . >      FROM (VALUES((1))))) a;
url       https://www.test.url/something/a.cgi?first=no#frag
scheme    https
username
password  null
host      www.test.url
port      443
path      /something/a.cgi
query     first=no
fragment  frag

1 row selected (0.126 seconds)
```