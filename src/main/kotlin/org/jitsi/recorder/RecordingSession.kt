package org.jitsi.recorder

import org.jitsi.mediajson.Event
import org.jitsi.mediajson.PingEvent
import org.jitsi.mediajson.PongEvent
import org.jitsi.mediajson.SessionEndEvent
import org.jitsi.mediajson.TranscriptionResultEvent
import org.jitsi.recorder.live.AudioFrameSink
import org.jitsi.recorder.live.NoopAudioFrameSink
import org.jitsi.recorder.live.DeepgramAudioFrameSink
import org.jitsi.recorder.live.SessionIntelligenceClient
import org.jitsi.utils.logging2.createLogger
import kotlinx.coroutines.runBlocking
import java.io.File
import org.jitsi.recorder.RecorderMetrics.Companion.instance as metrics

class RecordingSession(private val meetingId: String) {
    private val logger = createLogger().apply { addContext("meetingId", meetingId) }
    private val directory = selectDirectory(meetingId)

    init {
        metrics.sessionsStarted.inc()
        metrics.currentSessions.inc()
    }

    private val sessionStartTimeMs = System.currentTimeMillis()

    private val sessionIntelligenceClient: SessionIntelligenceClient? =
        Config.sessionIntelligenceBaseUrl?.let { baseUrl ->
            SessionIntelligenceClient(baseUrl = baseUrl, parentLogger = logger)
        }

    private val audioFrameSink: AudioFrameSink = if (Config.deepgramEnabled) {
        DeepgramAudioFrameSink(
            apiKey = Config.deepgramApiKey,
            model = Config.deepgramModel,
            language = Config.deepgramLanguage,
            interimResults = Config.deepgramInterimResults,
            endpointing = Config.deepgramEndpointing,
            sessionStartTimeMs = sessionStartTimeMs,
            parentLogger = logger,
            sessionIntelligenceClient = sessionIntelligenceClient
        )
    } else {
        NoopAudioFrameSink()
    }

    private val mediaJsonRecorder = if (Config.recordingFormat == RecordingFormat.MKA) {
        MediaJsonMkaRecorder(
            directory = directory,
            parentLogger = logger,
            meetingId = meetingId,
            audioFrameSink = audioFrameSink,
            recordingEnabled = Config.recordingEnabled
        )
    } else {
        MediaJsonJsonRecorder(directory)
    }

    init {
        audioFrameSink.onSessionStart(meetingId)
        if (!Config.recordingEnabled && !Config.deepgramEnabled) {
            logger.warn("Neither recording nor Deepgram transcription is enabled. Events will be received but not processed.")
        }
    }

    fun processText(text: String): String? {
        return try {
            when (val event = Event.parse(text)) {
                is PingEvent -> PongEvent(event.id).toJson()
                is PongEvent, is SessionEndEvent, is TranscriptionResultEvent -> null
                else -> {
                    mediaJsonRecorder.addEvent(event)
                    null
                }
            }
        } catch (e: Throwable) {
            logger.error("Error", e)
            null
        }
    }

    fun stop() {
        logger.info("Stopping")
        try {
            mediaJsonRecorder.stop()
        } finally {
            audioFrameSink.close()
            metrics.currentSessions.dec()
            finalize()
            sessionIntelligenceClient?.close()
        }
    }

    private fun finalize() {
        if (!Config.finalizeScript.isNullOrBlank()) {
            logger.info("Running finalize script")
            val logFile = File(directory, "finalize.log")
            val process = ProcessBuilder(
                Config.finalizeScript,
                meetingId,
                directory.absolutePath,
                Config.recordingFormat.toString()
            ).apply {
                if (!Config.logFinalizeOutput) {
                    redirectOutput(logFile)
                    redirectError(logFile)
                }
            }.start()

            if (Config.logFinalizeOutput) {
                logFile.outputStream().use { fileOut ->
                    val logLine: (String) -> Unit = { line ->
                        logger.info("[finalize] $line")
                        fileOut.write((line + "\n").toByteArray())
                        fileOut.flush()
                    }

                    val stdoutThread = Thread {
                        process.inputStream.bufferedReader().forEachLine(logLine)
                    }
                    val stderrThread = Thread {
                        process.errorStream.bufferedReader().forEachLine(logLine)
                    }
                    stdoutThread.start()
                    stderrThread.start()
                    stdoutThread.join()
                    stderrThread.join()
                }
            }

            process.waitFor().let {
                if (it != 0) {
                    metrics.finalizeErrors.inc()
                    logger.warn("Error from finalize: $it")
                } else {
                    logger.info("Finalize script completed successfully")
                }
            }
        }

        sessionIntelligenceClient?.let { client ->
            logger.info("Calling session intelligence finalize endpoint for meetingId=$meetingId")
            runBlocking {
                client.postFinalize(meetingId)
            }
        }
    }

    private fun selectDirectory(meetingId: String): File {
        var suffix = ""
        var counter = 1
        var file: File

        do {
            val path = "${Config.recordingDirectory}/$meetingId$suffix"
            file = File(path)

            if (!file.exists()) {
                file.mkdirs()
                return file
            }

            suffix = "-$counter"
            counter++
        } while (counter < 100)

        throw RuntimeException("Failed to create directory for meetingId $meetingId after 100 attempts")
    }
}
