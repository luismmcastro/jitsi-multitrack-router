package org.jitsi.recorder.live

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import io.ktor.websocket.send
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.jitsi.utils.logging2.Logger
import java.util.concurrent.ConcurrentHashMap

class DeepgramAudioFrameSink(
    private val apiKey: String,
    private val model: String,
    private val language: String,
    private val interimResults: Boolean,
    private val endpointing: Int,
    private val sessionStartTimeMs: Long,
    parentLogger: Logger
) : AudioFrameSink {
    data class TrackSession(val job: Job, val channel: SendChannel<ByteArray>)

    private val logger: Logger = parentLogger.createChildLogger(this.javaClass.name)
    private val client = HttpClient(CIO) {
        install(WebSockets)
    }

    private val tracks = ConcurrentHashMap<String, TrackSession>()

    private val json = Json { ignoreUnknownKeys = true }

    override fun onSessionStart(meetingId: String) {
    }

    override fun onTrackStart(meetingId: String, trackName: String, endpointId: String?) {
        if (tracks.containsKey(trackName)) return

        val channel = Channel<ByteArray>(Channel.UNLIMITED)
        val scope = CoroutineScope(Job() + Dispatchers.IO)

        val job = scope.launch {
            try {
                val url = "wss://api.deepgram.com/v1/listen?encoding=opus&sample_rate=48000&channels=1&model=$model&language=$language&interim_results=$interimResults&endpointing=$endpointing"
                client.webSocket(urlString = url, request = {
                    header(HttpHeaders.Authorization, "Token $apiKey")
                }) {
                    val outgoing = launch {
                        try {
                            for (data in channel) {
                                try {
                                    send(Frame.Binary(true, data))
                                } catch (t: Throwable) {
                                    logger.warn("Failed to send audio frame for track=$trackName: ${t.message}")
                                }
                            }
                            try {
                                send(Frame.Text("{\"type\":\"CloseStream\"}"))
                            } catch (t: Throwable) {
                                logger.warn("Failed to send CloseStream for track=$trackName: ${t.message}")
                            }
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            logger.warn("Outgoing audio coroutine error for track=$trackName: ${t.message}")
                        }
                    }

                    val incomingReader = launch {
                        try {
                            for (frame in incoming) {
                                if (frame is Frame.Text) {
                                    val text = frame.readText()
                                    try {
                                        val elem = json.parseToJsonElement(text)
                                        val speechFinal = try {
                                            elem.jsonObject["speech_final"]?.jsonPrimitive?.content?.toBoolean() ?: false
                                        } catch (_: Throwable) {
                                            false
                                        }
                                        if (!speechFinal) continue
                                        var transcript: String? = null
                                        try {
                                            val channelObj = elem.jsonObject["channel"]?.jsonObject
                                            val alternatives = channelObj?.get("alternatives")?.jsonArray
                                            val firstAlt = alternatives?.getOrNull(0)?.jsonObject
                                            transcript = firstAlt?.get("transcript")?.jsonPrimitive?.content
                                        } catch (_: Throwable) {
                                        }
                                        if (!transcript.isNullOrBlank()) {
                                            val participant = endpointId ?: trackName
                                            val relativeMs = System.currentTimeMillis() - sessionStartTimeMs
                                            logger.info("[Transcription] meetingId=$meetingId participant=$participant t=${relativeMs}ms: $transcript")
                                        }
                                    } catch (t: Throwable) {
                                        logger.warn("Failed to parse Deepgram message for track=$trackName: ${t.message}")
                                    }
                                }
                            }
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            logger.warn("Incoming reader error for track=$trackName: ${t.message}")
                        }
                    }

                    try {
                        outgoing.join()
                    } finally {
                        incomingReader.cancel()
                    }
                }
            } catch (t: Throwable) {
                if (t is CancellationException) return@launch
                logger.warn("Deepgram websocket error for track=$trackName: ${t.message}")
            } finally {
                try {
                    channel.close()
                } catch (_: Throwable) {
                }
            }
        }

        tracks[trackName] = TrackSession(job, channel)
    }

    override fun onOpusFrame(meetingId: String, trackName: String, endpointId: String?, timestampMs: Long, opusPayload: ByteArray) {
        val session = tracks[trackName]
        if (session == null) {
            logger.warn("Received opus frame for unknown track=$trackName")
            return
        }
        val result = session.channel.trySend(opusPayload)
        if (result.isFailure) {
            logger.warn("Dropping audio frame for track=$trackName: ${result.exceptionOrNull()?.message}")
        }
    }

    override fun onTrackEnd(meetingId: String, trackName: String, endpointId: String?) {
        val session = tracks.remove(trackName) ?: return
        try {
            session.channel.close()
        } catch (t: Throwable) {
            logger.warn("Error closing channel for track=$trackName: ${t.message}")
        }
        try {
            runBlocking {
                session.job.cancelAndJoin()
            }
        } catch (t: Throwable) {
            logger.warn("Error waiting for job to finish for track=$trackName: ${t.message}")
            try {
                session.job.cancel()
            } catch (_: Throwable) {}
        }
    }

    override fun onSessionEnd(meetingId: String) {
        val keys = ArrayList(tracks.keys)
        for (k in keys) {
            onTrackEnd(meetingId, k, null)
        }
    }

    override fun close() {
        val entries = ArrayList(tracks.entries)
        for ((k, v) in entries) {
            try {
                v.channel.close()
            } catch (_: Throwable) {
            }
            try {
                runBlocking { v.job.cancelAndJoin() }
            } catch (t: Throwable) {
                try { v.job.cancel() } catch (_: Throwable) {}
            }
            tracks.remove(k)
        }
        try {
            client.close()
        } catch (t: Throwable) {
            logger.warn("Error closing http client: ${t.message}")
        }
    }
}
