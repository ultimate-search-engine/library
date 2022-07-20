package libraries

import kotlinx.serialization.Serializable


class Page {
    @Serializable
    data class Page (
        val url: Address,
        val ranks: Ranks,
        val content: Content,
    )

    @Serializable
    data class Content(
        val title: String,
        val description: String,
        val keywords: List<String>,
        val anchors: List<String>,
        val boldText: List<String>,

        val headings: Headings,
        val text: List<String>,
    )

    @Serializable
    data class Headings(
        val h1: List<String>,
        val h2: List<String>,
        val h3: List<String>,
        val h4: List<String>,
        val h5: List<String>,
        val h6: List<String>,
    )

    @Serializable
    data class Ranks(
        val pagerank: Double,
        val smartRank: Double,

        val urlLength: Int,
        val urlPathLength: Int,
        val urlSegmentsCount: Int,
        val urlParameterCount: Int,
        val urlParameterCountUnique: Int,
        val urlParameterCountUniquePercent: Double,
        )

    @Serializable
    data class Address(
        val url: String,
        val urlPathKeywords: List<String>,
        val hostName: String
    )
}

