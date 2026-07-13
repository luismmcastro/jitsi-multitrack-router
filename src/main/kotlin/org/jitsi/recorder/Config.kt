package org.jitsi.recorder

import com.typesafe.config.ConfigFactory
import org.jitsi.config.ConfigSourceWrapper
import org.jitsi.config.TypesafeConfigSource
import org.jitsi.metaconfig.config
import org.jitsi.metaconfig.optionalconfig
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

class Config {
    companion object {
        val configSource = ConfigSourceWrapper(TypesafeConfigSource("config", ConfigFactory.load()))
        private const val BASE = "jitsi-multitrack-recorder"

        val port: Int by config {
            "$BASE.port".from(configSource)
        }

        val recordingDirectory: String by config {
            "$BASE.recording.directory".from(configSource)
        }

        val recordingFormat: RecordingFormat by config {
            "$BASE.recording.format".from(configSource).convertFrom<String> {
                RecordingFormat.valueOf(it.uppercase())
            }
        }

        val maxGapDuration: Duration by config {
            "$BASE.recording.max-gap-duration".from(configSource).convertFrom<java.time.Duration> {
                it.toKotlinDuration()
            }
        }

        val finalizeScript: String? by optionalconfig {
            "$BASE.finalize-script".from(configSource)
        }

        val sessionIntelligenceBaseUrl: String? by optionalconfig {
            "$BASE.session-intelligence.base-url".from(configSource)
        }

        val sessionIntelligenceToken: String? by optionalconfig {
            "$BASE.session-intelligence.token".from(configSource)
        }

        val logFinalizeOutput: Boolean by config {
            "$BASE.log-finalize-output".from(configSource)
        }

        val deepgramEnabled: Boolean by config {
            "$BASE.deepgram.enabled".from(configSource)
        }

        val deepgramApiKey: String by config {
            "$BASE.deepgram.api-key".from(configSource)
        }

        val deepgramModel: String by config {
            "$BASE.deepgram.model".from(configSource)
        }

        val deepgramLanguage: String by config {
            "$BASE.deepgram.language".from(configSource)
        }

        val deepgramInterimResults: Boolean by config {
            "$BASE.deepgram.interim-results".from(configSource)
        }

        val deepgramEndpointing: Int by config {
            "$BASE.deepgram.endpointing".from(configSource)
        }

        val recordingEnabled: Boolean by config {
            "$BASE.recording.enabled".from(configSource)
        }

        override fun toString(): String = """
            port: $port
            recordingDirectory: $recordingDirectory
            recordingFormat: $recordingFormat
            maxGapDuration: $maxGapDuration
            finalizeScript: $finalizeScript
            sessionIntelligenceBaseUrl: $sessionIntelligenceBaseUrl
            sessionIntelligenceToken: $sessionIntelligenceToken
            logFinalizeOutput: $logFinalizeOutput
            recordingEnabled: $recordingEnabled
            deepgramEnabled: $deepgramEnabled
            deepgramApiKey: $deepgramApiKey
            deepgramModel: $deepgramModel
            deepgramLanguage: $deepgramLanguage
            deepgramInterimResults: $deepgramInterimResults
            deepgramEndpointing: $deepgramEndpointing
        """.trimIndent()
    }
}
