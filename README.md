# Postgres COPY in/out Akka Streams Adapters

[![Build Status](https://travis-ci.org/klpx/akka-stream-postgresql-copy.svg?branch=master)](https://travis-ci.org/klpx/akka-stream-postgresql-copy) [![Coverage Status](https://coveralls.io/repos/github/klpx/akka-stream-postgresql-copy/badge.svg?branch=master)](https://coveralls.io/github/klpx/akka-stream-postgresql-copy?branch=master)

## Requirements
Scala 2.12 and 2.11 is supported. Tested on PostgreSQL 9.4

## Installation
`libraryDependencies ++= "ru.arigativa" %% "akka-streams-postgresql-copy" % "0.1.0"`

## Usage

### Source

_in progress_

### Sink

`PgCopyStreamConverters.sink` creates a `Sink` of `Product`'s (Tuple for example). Each tuple converts to String using `toString` method. `Option[T]` and `null` are handled properly.

For complex type for now you should convert values to string manually.

```scala
import ru.arigativa.akka.streams.PgCopyStreamConverters

def connection: PGConnection
val peoples = Seq(
    (1L, "Peter", Some("{tag1,tag2}"))
    (2L, "Jope", None)
)
Source.fromIterator(() => peoples.iterator)
  .runWith(PgCopyStreamConverters.sink(
    "COPY people (id, name, age) FROM STDIN", connection
  ))
```


