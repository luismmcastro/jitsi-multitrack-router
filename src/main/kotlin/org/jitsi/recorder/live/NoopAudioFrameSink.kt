package org.jitsi.recorder.live

class NoopAudioFrameSink : AudioFrameSink {
    override fun onSessionStart(meetingId: String) {}
    override fun onTrackStart(meetingId: String, trackName: String, endpointId: String?) {}
    override fun onOpusFrame(meetingId: String, trackName: String, endpointId: String?, timestampMs: Long, opusPayload: ByteArray) {}
    override fun onTrackEnd(meetingId: String, trackName: String, endpointId: String?) {}
    override fun onSessionEnd(meetingId: String) {}
    override fun close() {}
}
