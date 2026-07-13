package org.jitsi.recorder.live

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.request.header
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.isSuccess
import org.jitsi.utils.logging2.Logger

class SessionIntelligenceClient(
    private val baseUrl: String,
    parentLogger: Logger
) {
    private val logger: Logger = parentLogger.createChildLogger(this.javaClass.name)
    private val client = HttpClient(CIO)

    private fun escapeJson(input: String): String {
        return input
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
    }

    suspend fun postTranscription(meetingId: String, participantId: String, transcriptionTime: String, text: String) {
        val body = "{\"meetingId\":\"${escapeJson(meetingId)}\",\"participantId\":\"${escapeJson(participantId)}\",\"transcriptionTime\":\"${escapeJson(transcriptionTime)}\",\"text\":\"${escapeJson(text)}\"}"
        try {
            val url = "${baseUrl.trimEnd('/')}/session-intelligence/transcriptions"
            val response: HttpResponse = client.post(url) {
                setBody(body)
                header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            }
            if (!response.status.isSuccess()) {
                logger.warn("postTranscription returned non-success status: ${response.status}")
            }
        } catch (t: Throwable) {
            logger.warn("postTranscription failed: ${t.message}")
        }
    }

    suspend fun postFinalize(meetingId: String) {
        val body = "{\"meetingId\":\"${escapeJson(meetingId)}\"}"
        try {
            val url = "${baseUrl.trimEnd('/')}/session-intelligence/finalize"
            val response: HttpResponse = client.post(url) {
                setBody(body)
                header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            }
            if (!response.status.isSuccess()) {
                logger.warn("postFinalize returned non-success status: ${response.status}")
            }
        } catch (t: Throwable) {
            logger.warn("postFinalize failed: ${t.message}")
        }
    }

    fun close() {
        client.close()
    }
}
