package org.jitsi.recorder.live

interface AudioFrameSink {
    fun onSessionStart(meetingId: String)
    fun onTrackStart(meetingId: String, trackName: String, endpointId: String?)
    fun onOpusFrame(meetingId: String, trackName: String, endpointId: String?, timestampMs: Long, opusPayload: ByteArray)
    fun onTrackEnd(meetingId: String, trackName: String, endpointId: String?)
    fun onSessionEnd(meetingId: String)
    fun close()
}
