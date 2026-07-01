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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.jitsi.utils.logging2.Logger
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
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
    data class TrackSession(
        val job: Job,
        val channel: SendChannel<ByteArray>
    )

    private val logger: Logger = parentLogger.createChildLogger(this.javaClass.name)

    private val client = HttpClient(CIO) {
        install(WebSockets)
    }

    private val tracks = ConcurrentHashMap<String, TrackSession>()

    private val json = Json {
        ignoreUnknownKeys = true
    }

    override fun onSessionStart(meetingId: String) {
        logger.info("DeepgramAudioFrameSink session started meetingId=$meetingId")
    }

    override fun onTrackStart(meetingId: String, trackName: String, endpointId: String?) {
        if (tracks.containsKey(trackName)) return

        val channel = Channel<ByteArray>(Channel.UNLIMITED)
        val scope = CoroutineScope(Job() + Dispatchers.IO)

        val job = scope.launch {
            val transcriptBuffer = StringBuilder()

            fun flushTranscript(reason: String) {
                val finalText = transcriptBuffer.toString().trim()
                if (finalText.isBlank()) return

                val participant = endpointId ?: trackName
                val relativeMs = System.currentTimeMillis() - sessionStartTimeMs

                logger.info(
                    "[Transcription] meetingId=$meetingId participant=$participant track=$trackName reason=$reason t=${relativeMs}ms: $finalText"
                )

                transcriptBuffer.clear()
            }

            try {
                /**
                 * UtteranceEnd requires interim_results=true.
                 * We still only log when speech_final or UtteranceEnd happens.
                 */
                val enableUtteranceEnd = true
                val effectiveInterimResults = if (enableUtteranceEnd) true else interimResults

                val url = buildDeepgramUrl(
                    model = model,
                    language = language,
                    interimResults = effectiveInterimResults,
                    endpointing = endpointing,
                    enableUtteranceEnd = enableUtteranceEnd
                )

                logger.info("Opening Deepgram websocket for track=$trackName endpointId=$endpointId")

                client.webSocket(
                    urlString = url,
                    request = {
                        header(HttpHeaders.Authorization, "Token $apiKey")
                    }
                ) {
                    val keepAlive = launch {
                        try {
                            while (true) {
                                delay(3000)
                                send(Frame.Text("""{"type":"KeepAlive"}"""))
                            }
                        } catch (_: CancellationException) {
                            throw CancellationException()
                        } catch (t: Throwable) {
                            logger.warn("Deepgram KeepAlive error for track=$trackName: ${t.message}")
                        }
                    }

                    val outgoing = launch {
                        try {
                            for (data in channel) {
                                try {
                                    send(Frame.Binary(true, data))
                                } catch (t: Throwable) {
                                    logger.warn("Failed to send audio frame for track=$trackName: ${t.message}")
                                }
                            }

                            /**
                             * CloseStream tells Deepgram the stream is done.
                             * It should flush remaining audio and close the response stream.
                             */
                            try {
                                send(Frame.Text("""{"type":"CloseStream"}"""))
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
                                if (frame !is Frame.Text) continue

                                val text = frame.readText()

                                try {
                                    val elem = json.parseToJsonElement(text)
                                    val obj = elem.jsonObject

                                    val type = obj["type"]?.jsonPrimitive?.content

                                    /**
                                     * Deepgram UtteranceEnd event.
                                     * This means a silence gap was detected after finalized words.
                                     */
                                    if (type == "UtteranceEnd") {
                                        flushTranscript("utterance_end")
                                        continue
                                    }

                                    /**
                                     * Usually transcription messages are type=Results.
                                     * Some messages may not have type, so do not hard-require it.
                                     */
                                    val isFinal =
                                        obj["is_final"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: false

                                    val speechFinal =
                                        obj["speech_final"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: false

                                    val transcript = try {
                                        val channelObj = obj["channel"]?.jsonObject
                                        val alternatives = channelObj?.get("alternatives")?.jsonArray
                                        val firstAlt = alternatives?.getOrNull(0)?.jsonObject
                                        firstAlt?.get("transcript")?.jsonPrimitive?.content
                                    } catch (_: Throwable) {
                                        null
                                    }

                                    /**
                                     * Important:
                                     * - We buffer finalized text.
                                     * - We do NOT log here.
                                     * - We only log when speechFinal or UtteranceEnd happens.
                                     */
                                    if ((isFinal || speechFinal) && !transcript.isNullOrBlank()) {
                                        transcriptBuffer.append(transcript).append(" ")
                                    }

                                    /**
                                     * speech_final=true means Deepgram detected an endpoint/pause.
                                     * This is the main "user stopped speaking" signal.
                                     */
                                    if (speechFinal) {
                                        flushTranscript("speech_final")
                                    }
                                } catch (t: Throwable) {
                                    logger.warn("Failed to parse Deepgram message for track=$trackName: ${t.message}")
                                }
                            }
                        } catch (t: Throwable) {
                            if (t is CancellationException) throw t
                            logger.warn("Incoming reader error for track=$trackName: ${t.message}")
                        } finally {
                            /**
                             * In case the socket closes while we still have buffered text.
                             */
                            flushTranscript("socket_closed")
                        }
                    }

                    try {
                        outgoing.join()

                        /**
                         * Give Deepgram time to send the final messages after CloseStream.
                         */
                        val completed = withTimeoutOrNull(8000) {
                            incomingReader.join()
                            true
                        } ?: false

                        if (!completed) {
                            logger.warn("Timeout waiting for Deepgram final messages for track=$trackName")
                            incomingReader.cancel()
                        }
                    } finally {
                        keepAlive.cancel()
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

    override fun onOpusFrame(
        meetingId: String,
        trackName: String,
        endpointId: String?,
        timestampMs: Long,
        opusPayload: ByteArray
    ) {
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

        logger.info("Ending Deepgram track=$trackName endpointId=$endpointId")

        try {
            session.channel.close()
        } catch (t: Throwable) {
            logger.warn("Error closing channel for track=$trackName: ${t.message}")
        }

        try {
            runBlocking {
                val finished = withTimeoutOrNull(10000) {
                    session.job.join()
                    true
                } ?: false

                if (!finished) {
                    logger.warn("Timeout waiting for Deepgram job to finish for track=$trackName, cancelling")
                    session.job.cancelAndJoin()
                }
            }
        } catch (t: Throwable) {
            logger.warn("Error waiting for Deepgram job to finish for track=$trackName: ${t.message}")

            try {
                session.job.cancel()
            } catch (_: Throwable) {
            }
        }
    }

    override fun onSessionEnd(meetingId: String) {
        logger.info("DeepgramAudioFrameSink session ended meetingId=$meetingId")

        val keys = ArrayList(tracks.keys)

        for (trackName in keys) {
            onTrackEnd(meetingId, trackName, null)
        }
    }

    override fun close() {
        val entries = ArrayList(tracks.entries)

        for ((trackName, session) in entries) {
            try {
                session.channel.close()
            } catch (_: Throwable) {
            }

            try {
                runBlocking {
                    val finished = withTimeoutOrNull(10000) {
                        session.job.join()
                        true
                    } ?: false

                    if (!finished) {
                        session.job.cancelAndJoin()
                    }
                }
            } catch (_: Throwable) {
                try {
                    session.job.cancel()
                } catch (_: Throwable) {
                }
            }

            tracks.remove(trackName)
        }

        try {
            client.close()
        } catch (t: Throwable) {
            logger.warn("Error closing http client: ${t.message}")
        }
    }

    private fun buildDeepgramUrl(
        model: String,
        language: String,
        interimResults: Boolean,
        endpointing: Int,
        enableUtteranceEnd: Boolean
    ): String {
        val params = linkedMapOf(
            "encoding" to "opus",
            "sample_rate" to "48000",
            "channels" to "1",
            "model" to model,
            "language" to language,
            "interim_results" to interimResults.toString(),
            "endpointing" to endpointing.toString(),
            "punctuate" to "true",
            "smart_format" to "true"
        )

        if (enableUtteranceEnd) {
            params["utterance_end_ms"] = "1000"
        }

        val query = params.entries.joinToString("&") { (key, value) ->
            "${urlEncode(key)}=${urlEncode(value)}"
        }

        return "wss://api.deepgram.com/v1/listen?$query"
    }

    private fun urlEncode(value: String): String {
        return URLEncoder.encode(value, StandardCharsets.UTF_8.toString())
    }
}