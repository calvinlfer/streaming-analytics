import ch.qos.logback.core.*
import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender(name="CONSOLE", clazz=ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%date{ISO8601} [%level] %logger %X{akkaSource} message=%msg\n"
    }
}

logger(name="akka", level=WARN)
logger(name="com.outworkers.phantom", level=INFO)

root(level=INFO, appenderNames=["CONSOLE"])