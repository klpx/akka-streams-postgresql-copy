# Postgres COPY in/out Akka Streams Adapters

[![Build Status](https://travis-ci.org/klpx/akka-streams-postgresql-copy.svg?branch=master)](https://travis-ci.org/klpx/akka-streams-postgresql-copy) [![Coverage Status](https://coveralls.io/repos/github/klpx/akka-stream-postgresql-copy/badge.svg?branch=master)](https://coveralls.io/github/klpx/akka-stream-postgresql-copy?branch=master)

## Requirements
Scala 2.12 and 2.11 is supported. Tested on PostgreSQL 9.4 but you can pull this repository and run tests with your Postgres using Docker.

## Installation
`libraryDependencies ++= "ru.arigativa" %% "akka-streams-postgresql-copy" % "0.7.0"`

## Usage

### Source

`PgCopyStreamConverters.source` creates a `Source` of `Seq[String]`. Where each element is a column value (and can be `null`).

```scala
import ru.arigativa.akka.streams.{PgCopyStreamConverters, PgCopySinkSettings}
import ru.arigativa.akka.streams.ConnectionProvider._ // Implicits for ConnectionProvider

val conn: BaseConnection
PgCopyStreamConverters.source("""
        COPY (SELECT id, name, age FROM people) TO STDOUT
    """, PgCopySourceSettings(conn))
    .runWith(Sink.foreach(println))
/*
List(1, Alex, 26)
List(2, Lisa, 22)
List(3, With
	 special chars\, 10)
List(4, null, -1)
*/
```

### Sink

`PgCopyStreamConverters.sink` creates a `Sink` of `Product`'s (Tuple for example). Each tuple converts to String using `toString` method. `Option[T]` and `null` are handled properly.

For complex type for now you should convert values to string manually.

You also should provide connection. `sink()` expects `ConnectionProvider` for able you to control connection acquiring/release. `ConnectionProvider` companion-object provide implicit conversion for `org.postgresql.core.PGConnection` (after sink is complete it does not close connection) and for getter `() => org.postgresql.core.BaseConnection` (after sink is complete it does close connection)

```scala
import ru.arigativa.akka.streams.{PgCopyStreamConverters, PgCopySinkSettings}
import ru.arigativa.akka.streams.ConnectionProvider._ // Implicits for ConnectionProvider

val conn: BaseConnection
val peoples = Seq(
    (1L, "Peter", Some("{tag1,tag2}"))
    (2L, "Jope", None)
)
Source.fromIterator(() => peoples.iterator)
  .runWith(PgCopyStreamConverters.sink(
    "COPY people (id, name, tags) FROM STDIN",
    PgCopySinkSettings(conn)
  ))
```


#### Initial buffer
`PgCopySinkSettings` has parameter `initialBufferSize`. If it more than 0 then COPY command won't started and connection to DB won't opened until initial buffer of that size is filled up ,


### ConnectionProvider

You can manually write ConnectionProvider for your library. Example for Slick 3.1.1:
```scala
import org.postgresql.PGConnection
import slick.jdbc.JdbcBackend.DatabaseDef

implicit def slickDatabaseDef2ConnectionProvider(db: DatabaseDef): ConnectionProvider = new ConnectionProvider {
    private val session = db.createSession()

    def acquire(): Try[PGConnection] = Try(session.conn.asInstanceOf[PGConnection])
    def release(): Unit = session.close()
  }
```

