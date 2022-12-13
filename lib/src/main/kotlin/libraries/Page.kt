package libraries

import kotlinx.serialization.Serializable


class Page {
    @Serializable
    data class Page (
        val url: Address = Address(),
        val ranks: Ranks = Ranks(),
        val content: Content = Content(),
    )

    @Serializable
    data class Content(
        val title: String = "",
        val description: String = "",
        val keywords: List<String> = listOf(),
        val anchors: List<String> = listOf(),
        val boldText: List<String> = listOf(),

        val headings: Headings = Headings(),
        val text: List<String> = listOf(),
    )

    @Serializable
    data class Headings(
        val h1: List<String> = listOf(),
        val h2: List<String> = listOf(),
        val h3: List<String> = listOf(),
        val h4: List<String> = listOf(),
        val h5: List<String> = listOf(),
        val h6: List<String> = listOf(),
    )

    @Serializable
    data class Ranks(
        val pagerank: Double = 0.0,
        val smartRank: Double = 0.0,

        val urlLength: Int = 0,
        val urlPathLength: Int = 0,
        val urlSegmentsCount: Int = 0,
        val urlParameterCount: Int = 0,
        val urlParameterCountUnique: Int = 0,
        val urlParameterCountUniquePercent: Double = 0.0,
        val totalUrlDocs: Int = 0
        )

    @Serializable
    data class Address(
        val url: String = "",
        val urlPathKeywords: List<String> = listOf(),
        val hostName: String = ""
    )
}

