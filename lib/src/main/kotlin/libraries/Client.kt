package libraries

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import org.litote.kmongo.*
import org.litote.kmongo.coroutine.coroutine
import org.litote.kmongo.reactivestreams.KMongo
import kotlin.math.roundToInt
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue


class PageRepository {

    @kotlinx.serialization.Serializable
    data class Page(
        val targetUrl: List<String>,
        val finalUrl: String,
        val content: String,
        val timestamp: Long,
        val statusCode: Int
    )


    abstract class Client {
        abstract suspend fun find(url: String): Page?
        abstract suspend fun findTarget(url: String): List<Page>
        abstract suspend fun findAfter(url: String?, limit: Int = 200, includeBody: Boolean = true): List<Page>

        abstract suspend fun add(page: Page)

        abstract suspend fun randomPages(size: Int = 1, code: Int = 200): List<Page>

        abstract suspend fun update(page: Page)
    }


    @Suppress("Unused")
    class MongoClient(db: String, location: String = "mongodb://localhost:27017", collectionName: String? = null) : Client() {
        private val client = KMongo.createClient(location).coroutine
        private val database = client.getDatabase(db)
        private val col = if (collectionName == null) database.getCollection<Page>() else database.getCollection(collectionName)

        override suspend fun find(url: String): Page? = col.find((Page::finalUrl) eq (url)).toList().firstOrNull()

        override suspend fun findTarget(url: String): List<Page> = col.find(Page::targetUrl contains (url)).toList()

        override suspend fun add(page: Page) {
            col.insertOne(page)
        }

        override suspend fun randomPages(size: Int, code: Int): List<Page> =
            col.aggregate<Page>(listOf(sample(size), match(Page::statusCode eq code))).toList()

        override suspend fun update(page: Page) {
            col.updateOne(Page::finalUrl eq page.finalUrl, page)
        }

        override suspend fun findAfter(url: String?, limit: Int, includeBody: Boolean): List<Page> {
            val firstUrl = url ?: ""
            return col.find(
                Page::finalUrl gt firstUrl
            ).limit(limit).toList()
        }

    }
}

suspend fun main() {
    val client = PageRepository.MongoClient("wiki2")
//    val clientUtil = PageRepository("wiki2").mongoClientUtil()
//    client.add(PageRepository.Page(listOf(PageRepository.Url("http://www.xdd.com", mapOf())), PageRepository.Url("http://www.xdd.com", mapOf("xd" to "xdd")), "", 0, 0))
//    println(client.getPagesByUrl("http://www.google.com", mapOf("xd" to "xdd")))
//    println(clientUtil.count())
//    println(client.find("https://en.wikipedia.org/wiki/Category:American_singer_stubs").first())
//    println(client.randomPages().first().finalUrl)
    client.findAfter(null, limit = 10).forEach { println(it.finalUrl) }
}
