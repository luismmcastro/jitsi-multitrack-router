package org.jitsi.recorder.live

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.shouldBe
import org.jitsi.mediajson.CustomParameters
import org.jitsi.mediajson.Media
import org.jitsi.mediajson.MediaEvent
import org.jitsi.mediajson.MediaFormat
import org.jitsi.mediajson.Start
import org.jitsi.mediajson.StartEvent
import org.jitsi.recorder.MediaJsonMkaRecorder
import org.jitsi.recorder.setupInPlaceIoPool
import org.jitsi.utils.logging2.createLogger
import java.nio.file.Files
import java.util.Base64

// ---- In-memory capturing sink ----

data class TrackStartCall(val meetingId: String, val trackName: String, val endpointId: String?)
data class OpusFrameCall(
    val meetingId: String,
    val trackName: String,
    val endpointId: String?,
    val timestampMs: Long,
    val opusPayload: ByteArray
)
data class SessionEndCall(val meetingId: String)
data class SessionStartCall(val meetingId: String)
data class TrackEndCall(val meetingId: String, val trackName: String, val endpointId: String?)

class CapturingAudioFrameSink : AudioFrameSink {
    val sessionStarts = mutableListOf<SessionStartCall>()
    val trackStarts = mutableListOf<TrackStartCall>()
    val opusFrames = mutableListOf<OpusFrameCall>()
    val trackEnds = mutableListOf<TrackEndCall>()
    val sessionEnds = mutableListOf<SessionEndCall>()
    var closeCalled = false

    override fun onSessionStart(meetingId: String) {
        sessionStarts.add(SessionStartCall(meetingId))
    }

    override fun onTrackStart(meetingId: String, trackName: String, endpointId: String?) {
        trackStarts.add(TrackStartCall(meetingId, trackName, endpointId))
    }

    override fun onOpusFrame(
        meetingId: String,
        trackName: String,
        endpointId: String?,
        timestampMs: Long,
        opusPayload: ByteArray
    ) {
        opusFrames.add(OpusFrameCall(meetingId, trackName, endpointId, timestampMs, opusPayload))
    }

    override fun onTrackEnd(meetingId: String, trackName: String, endpointId: String?) {
        trackEnds.add(TrackEndCall(meetingId, trackName, endpointId))
    }

    override fun onSessionEnd(meetingId: String) {
        sessionEnds.add(SessionEndCall(meetingId))
    }

    override fun close() {
        closeCalled = true
    }
}

// ---- Tests ----

class LiveForwardingTest : ShouldSpec({
    setupInPlaceIoPool()

    context("MediaJsonMkaRecorder with in-memory AudioFrameSink") {
        should("call onTrackStart and onOpusFrame when processing StartEvent + MediaEvent") {
            val sink = CapturingAudioFrameSink()
            val tempDir = Files.createTempDirectory("LiveForwardingTest").toFile()
            try {
                val logger = createLogger()
                val recorder = MediaJsonMkaRecorder(
                    directory = tempDir,
                    parentLogger = logger,
                    meetingId = "test-meeting-123",
                    audioFrameSink = sink,
                    recordingEnabled = false
                )

                // Minimal valid Opus packet: TOC byte 0x00 (Silk NB, 10ms, mono, code 0) followed by 3 bytes of payload
                val opusBytes = byteArrayOf(0x00, 0x01, 0x02, 0x03)
                val payloadBase64 = Base64.getEncoder().encodeToString(opusBytes)

                val startEvent = StartEvent(
                    1,
                    Start(
                        "audio-track-1",
                        MediaFormat("opus", 48000, 2),
                        CustomParameters("endpoint-abc")
                    )
                )

                val mediaEvent = MediaEvent(
                    2,
                    Media(
                        "audio-track-1",
                        0,
                        960L,
                        payloadBase64
                    )
                )

                recorder.addEvent(startEvent)
                recorder.addEvent(mediaEvent)

                // Assert onTrackStart was called with expected values
                sink.trackStarts shouldHaveAtLeastSize 1
                sink.trackStarts[0].meetingId shouldBe "test-meeting-123"
                sink.trackStarts[0].trackName shouldBe "audio-track-1"
                sink.trackStarts[0].endpointId shouldBe "endpoint-abc"

                // Assert onOpusFrame was called at least once with expected values
                sink.opusFrames shouldHaveAtLeastSize 1
                sink.opusFrames[0].meetingId shouldBe "test-meeting-123"
                sink.opusFrames[0].trackName shouldBe "audio-track-1"
                sink.opusFrames[0].endpointId shouldBe "endpoint-abc"
                sink.opusFrames[0].opusPayload.isNotEmpty() shouldBe true

                // Stop and assert onSessionEnd was called
                recorder.stop()
                sink.sessionEnds shouldHaveAtLeastSize 1
                sink.sessionEnds[0].meetingId shouldBe "test-meeting-123"
            } finally {
                tempDir.deleteRecursively()
            }
        }
    }

    context("NoopAudioFrameSink") {
        should("allow all methods to be called without throwing an exception") {
            val noop = NoopAudioFrameSink()
            noop.onSessionStart("meeting-id")
            noop.onTrackStart("meeting-id", "track", "endpoint")
            noop.onTrackStart("meeting-id", "track", null)
            noop.onOpusFrame("meeting-id", "track", "endpoint", 1000L, byteArrayOf(0x00))
            noop.onOpusFrame("meeting-id", "track", null, 2000L, byteArrayOf())
            noop.onTrackEnd("meeting-id", "track", "endpoint")
            noop.onTrackEnd("meeting-id", "track", null)
            noop.onSessionEnd("meeting-id")
            noop.close()
        }
    }
})
