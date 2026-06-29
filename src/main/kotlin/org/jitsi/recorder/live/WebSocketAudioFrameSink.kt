package org.jitsi.recorder.live

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.websocket.Frame
import io.ktor.websocket.send
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.CancellationException
import org.jitsi.utils.logging2.Logger
import java.time.Instant
import java.util.Base64
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class WebSocketAudioFrameSink(
    private val url: String,
    private val authToken: String?,
    private val failOnError: Boolean,
    maxQueueSize: Int,
    parentLogger: Logger
) : AudioFrameSink {
    private val logger: Logger = parentLogger.createChildLogger(this.javaClass.name)

    private val startedTracks = mutableSetOf<String>()
    private val queue = LinkedBlockingQueue<String>(maxQueueSize)

    private val client = HttpClient(CIO) {
        install(WebSockets)
    }

    private val workerThread: Thread

    private val sessionEndSent = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)
    private val droppedFrameCount = AtomicLong(0)
    private val sentFrameCount = AtomicLong(0)
    private val meetingIdForClose = AtomicReference<String?>(null)

    private val lastDroppedLogTime = AtomicLong(0)

    init {
        workerThread = Thread {
            runBlocking {
                while (!closed.get()) {
                    try {
                        try {
                            connectAndDrain()
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            if (closed.get()) break
                            logger.warn("WebSocket connection error: ${t.message}")
                            delay(1000)
                        }
                    } catch (t: Throwable) {
                        if (closed.get()) break
                        logger.warn("WebSocket setup error: ${t.message}")
                        delay(1000)
                    }
                }
            }
        }
        workerThread.isDaemon = true
        workerThread.start()
    }

    private suspend fun connectAndDrain() {
        client.webSocket(urlString = this@WebSocketAudioFrameSink.url, request = {
            if (!authToken.isNullOrBlank()) {
                header(HttpHeaders.Authorization, "Bearer $authToken")
            }
            header("X-Jitsi-Multitrack-Fork", "springloop-media-router")
        }) {
            while (!closed.get()) {
                val msg = try {
                    queue.poll(100, TimeUnit.MILLISECONDS)
                } catch (_: InterruptedException) {
                    null
                } ?: continue
                try {
                    send(Frame.Text(msg))
                    sentFrameCount.incrementAndGet()
                } catch (t: Throwable) {
                    val reoffered = tryReoffer(msg)
                    if (!reoffered && failOnError) {
                        throw IllegalStateException("Failed to send message and re-offer to queue", t)
                    }
                    throw t
                }
            }
        }
    }

    private fun escapeJson(s: String?): String {
        if (s == null) return ""
        val sb = StringBuilder()
        for (ch in s) {
            when (ch) {
                '\\' -> sb.append("\\\\")
                '"' -> sb.append("\\\"")
                '\b' -> sb.append("\\b")
                '\u000C' -> sb.append("\\f")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> {
                    if (ch.code < 0x20) {
                        sb.append(String.format("\\u%04x", ch.code))
                    } else {
                        sb.append(ch)
                    }
                }
            }
        }
        return sb.toString()
    }

    private fun tryOffer(json: String) {
        if (queue.offer(json)) return
        if (failOnError) throw IllegalStateException("Queue is full, failing on error")
        val dropped = queue.poll()
        if (dropped != null) {
            val count = droppedFrameCount.incrementAndGet()
            val now = Instant.now().toEpochMilli()
            val lastLog = lastDroppedLogTime.get()
            if (count % 100L == 0L || now - lastLog >= 5000) {
                lastDroppedLogTime.set(now)
                logger.warn("Dropped $count frames so far while offering frames to the queue")
            }
        }
        queue.offer(json)
    }

    private fun tryReoffer(json: String): Boolean {
        if (queue.offer(json)) return true
        if (failOnError) return false
        val dropped = queue.poll()
        if (dropped != null) {
            val count = droppedFrameCount.incrementAndGet()
            val now = Instant.now().toEpochMilli()
            val lastLog = lastDroppedLogTime.get()
            if (count % 100L == 0L || now - lastLog >= 5000) {
                lastDroppedLogTime.set(now)
                logger.warn("Dropped $count frames so far while re-offering frames to the queue")
            }
        }
        return queue.offer(json)
    }

    override fun onSessionStart(meetingId: String) {
        meetingIdForClose.set(meetingId)
        try {
            val startedAt = Instant.now().toString()
            val json = "{" +
                "\"type\":\"session.start\"," +
                "\"meetingId\":\"${escapeJson(meetingId)}\"," +
                "\"roomId\":\"${escapeJson(meetingId)}\"," +
                "\"startedAt\":\"${escapeJson(startedAt)}\"" +
                "}"
            tryOffer(json)
        } catch (t: Throwable) {
            logger.error("Error handling onSessionStart", t)
            if (failOnError) throw t
        }
    }

    override fun onTrackStart(meetingId: String, trackName: String, endpointId: String?) {
        synchronized(startedTracks) {
            if (!startedTracks.add(trackName)) return
        }
        try {
            val startedAt = Instant.now().toString()
            val participantId = endpointId ?: trackName
            val endpoint = endpointId ?: trackName
            val json = "{" +
                "\"type\":\"track.start\"," +
                "\"meetingId\":\"${escapeJson(meetingId)}\"," +
                "\"roomId\":\"${escapeJson(meetingId)}\"," +
                "\"participantId\":\"${escapeJson(participantId)}\"," +
                "\"endpointId\":\"${escapeJson(endpoint)}\"," +
                "\"trackName\":\"${escapeJson(trackName)}\"," +
                "\"startedAt\":\"${escapeJson(startedAt)}\"" +
                "}"
            tryOffer(json)
        } catch (t: Throwable) {
            logger.error("Error handling onTrackStart", t)
            if (failOnError) throw t
        }
    }

    override fun onOpusFrame(meetingId: String, trackName: String, endpointId: String?, timestampMs: Long, opusPayload: ByteArray) {
        try {
            val payloadBase64 = Base64.getEncoder().encodeToString(opusPayload)
            val participantId = endpointId ?: trackName
            val endpoint = endpointId ?: trackName
            val json = "{" +
                "\"type\":\"audio.opus\"," +
                "\"meetingId\":\"${escapeJson(meetingId)}\"," +
                "\"roomId\":\"${escapeJson(meetingId)}\"," +
                "\"participantId\":\"${escapeJson(participantId)}\"," +
                "\"endpointId\":\"${escapeJson(endpoint)}\"," +
                "\"trackName\":\"${escapeJson(trackName)}\"," +
                "\"timestampMs\":$timestampMs," +
                "\"payloadEncoding\":\"base64\"," +
                "\"payload\":\"${escapeJson(payloadBase64)}\"" +
                "}"
            tryOffer(json)
        } catch (t: Throwable) {
            logger.error("Error handling onOpusFrame", t)
            if (failOnError) throw t
        }
    }

    override fun onTrackEnd(meetingId: String, trackName: String, endpointId: String?) {
        synchronized(startedTracks) {
            startedTracks.remove(trackName)
        }
        try {
            val endedAt = Instant.now().toString()
            val participantId = endpointId ?: trackName
            val endpoint = endpointId ?: trackName
            val json = "{" +
                "\"type\":\"track.end\"," +
                "\"meetingId\":\"${escapeJson(meetingId)}\"," +
                "\"roomId\":\"${escapeJson(meetingId)}\"," +
                "\"participantId\":\"${escapeJson(participantId)}\"," +
                "\"endpointId\":\"${escapeJson(endpoint)}\"," +
                "\"trackName\":\"${escapeJson(trackName)}\"," +
                "\"endedAt\":\"${escapeJson(endedAt)}\"" +
                "}"
            tryOffer(json)
        } catch (t: Throwable) {
            logger.error("Error handling onTrackEnd", t)
            if (failOnError) throw t
        }
    }

    override fun onSessionEnd(meetingId: String) {
        if (sessionEndSent.compareAndSet(false, true)) {
            try {
                val endedAt = Instant.now().toString()
                val json = "{" +
                    "\"type\":\"session.end\"," +
                    "\"meetingId\":\"${escapeJson(meetingId)}\"," +
                    "\"roomId\":\"${escapeJson(meetingId)}\"," +
                    "\"endedAt\":\"${escapeJson(endedAt)}\"" +
                    "}"
                tryOffer(json)
            } catch (t: Throwable) {
                logger.error("Error handling onSessionEnd", t)
                if (failOnError) throw t
            }
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return

        try {
            val meetingId = meetingIdForClose.get()
            if (meetingId != null && sessionEndSent.compareAndSet(false, true)) {
                val endedAt = Instant.now().toString()
                val json = "{" +
                    "\"type\":\"session.end\"," +
                    "\"meetingId\":\"${escapeJson(meetingId)}\"," +
                    "\"roomId\":\"${escapeJson(meetingId)}\"," +
                    "\"endedAt\":\"${escapeJson(endedAt)}\"" +
                    "}"
                try {
                    tryOffer(json)
                } catch (t: Throwable) {
                    logger.warn("Failed to enqueue session-end during close: ${t.message}")
                }
            }
        } catch (t: Throwable) {
            logger.warn("Error while sending session-end on close: ${t.message}")
        }

        try {
            workerThread.interrupt()
            workerThread.join(5000)
        } catch (_: Throwable) {
        }

        try {
            client.close()
        } catch (t: Throwable) {
            logger.warn("Error closing http client: ${t.message}")
        }

        logger.info("WebSocketAudioFrameSink closed. sent=${sentFrameCount.get()} dropped=${droppedFrameCount.get()}")
    }
}
