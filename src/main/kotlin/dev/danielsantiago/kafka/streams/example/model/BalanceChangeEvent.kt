package dev.danielsantiago.kafka.streams.example.model

data class BalanceChangeEvent(
    val type: BalanceChangeEventType,
    val clientId: String,
    val senderId: String? = null,
    val receiverId: String? = null,
    val amountCents: Long
) {
    enum class BalanceChangeEventType {
        DEPOSIT, WITHDRAW, SEND, RECEIVE
    }
}
