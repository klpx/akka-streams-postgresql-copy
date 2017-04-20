package ru.arigativa.akka.streams

/**
  * @param connectionProvider Object that provides connection and releases it when Sink is complete
  * @param initialBufferSize COPY won't start until initial buffer is filled
  */
case class PgCopySinkSettings(
                               connectionProvider: ConnectionProvider,
                               initialBufferSize: Long = 0
                             )
/**
  * @param connectionProvider Object that provides connection and releases it when Sink is complete
  */
case class PgCopySourceSettings(
                               connectionProvider: ConnectionProvider
                             )
