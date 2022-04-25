package libraries

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.mapping.ObjectProperty
import co.elastic.clients.elasticsearch._types.mapping.Property
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.core.*
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.elasticsearch.indices.*
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import co.elastic.clients.util.ObjectBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient


data class Credentials(val userName: String, val password: String)
data class Address(val hostname: String, val port: Int)

open class Elastic(credentials: Credentials, address: Address, private val index: String) {
    private val client: ElasticsearchClient

    init {
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY, UsernamePasswordCredentials(credentials.userName, credentials.password)
        )

        val restClient = RestClient.builder(
            HttpHost(address.hostname, address.port)
        ).setHttpClientConfigCallback { httpClientBuilder ->
            httpClientBuilder.disableAuthCaching()
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }.build()

        val transport: ElasticsearchTransport = RestClientTransport(
            restClient, JacksonJsonpMapper()
        )
        client = ElasticsearchClient(transport)
    }


    @Suppress("Unused")
    val alias = Alias(client, index)

    @Suppress("Unused")
    suspend fun add(page: Page.Page, id: String? = null): IndexResponse = withContext(Dispatchers.IO) {
        client.index(addRequest(page, id))
    }


    @Suppress("Unused")
    fun addRequest(page: Page.Page, id: String? = null): IndexRequest<Page.Page> = IndexRequest.of {
        if (id != null) it.id(id)
        it.index(index)
        it.document(page)
    }


    @Suppress("Unused")
    suspend fun getByUrlOrNull(url: String, batchSize: Int = 10): Hit<Page.Page>? = withContext(Dispatchers.IO) {
        val search: SearchResponse<Page.Page> = search(docsByUrlRequest(url, batchSize))

        val hits = search.hits().hits()
        return@withContext hits.firstOrNull()
    }

    @Suppress("Unused")
    fun docsByUrlRequest(url: String, batchSize: Int = 10): SearchRequest = SearchRequest.of {
        it.size(batchSize)
        it.index(index).query { query ->
            query.term { term ->
                term.field("url").value { value ->
                    value.stringValue(url)
                }
            }
        }
    }

    @Suppress("Unused")
    suspend fun getByUrlOrNullBulk(urls: List<String>, batchSize: Long = 10): MsearchResponse<Page.Page> =
        withContext(Dispatchers.IO) {
            fun searchesBuilder(req: MsearchRequest.Builder, url: String): ObjectBuilder<MsearchRequest> =
                req.searches { searches ->
                    searches.header {
                        it.index(index)
                    }
                    searches.body {
                        it.size(batchSize.toInt())
                        it.query { query ->
                            query.ids { ids ->
                                ids.values(urls)
                            }
                            query.term { term ->
                                term.field("url").value { value ->
                                    value.stringValue(url)
                                }
                            }
                        }
                    }
                }

            val req = MsearchRequest.of { builder ->
                urls.forEach { searchesBuilder(builder, it) }
                builder.index(index)
            }
            return@withContext client.msearch(req, Page.Page::class.java)
        }

    data class PageById(val page: Page.Page, val id: String?)

    @Suppress("Unused")
    suspend fun addDocsBulkByIds(docs: List<PageById>): BulkResponse = withContext(Dispatchers.IO) {
        fun indexBuilder(req: BulkRequest.Builder, doc: PageById) {
            req.operations { operations ->
                operations.index(IndexOperation.of<Page.Page> { ind ->
                    if (doc.id != null) ind.id(doc.id)
                    ind.document(doc.page)
                    ind.index(index)
                })
            }
        }

        val req = BulkRequest.of { builder ->
            docs.forEach { indexBuilder(builder, it) }
            builder.index(index)
        }
        return@withContext client.bulk(req)
    }

    @Suppress("Unused")
    suspend fun addDocsBulk(pages: List<Page.Page>) = addDocsBulkByIds(pages.map { PageById(it, null) })


    @Suppress("Unused")
    suspend fun count(): Long = coroutineScope {
        val search: CountRequest = CountRequest.of {
            it.index(index)
            it.query { query ->
                query.bool { bool ->
                    bool.must { must ->
                        must.exists { exists ->
                            exists.field("url")
                        }
                    }
                }
            }
        }
        return@coroutineScope client.count(search).count()
    }

    @Suppress("Unused")
    suspend fun search(searchRequest: SearchRequest): SearchResponse<Page.Page> = coroutineScope {
        return@coroutineScope withContext(Dispatchers.IO) { client.search(searchRequest, Page.Page::class.java) }
    }

    @Suppress("Unused")
    suspend fun putMapping(numberOfShards: Int = 1, numberOfReplicas: Int = 0): CreateIndexResponse = coroutineScope {

        fun TypeMapping.Builder.field(
            key: String, f: (Property.Builder) -> ObjectBuilder<Property>
        ): TypeMapping.Builder = this.properties(key, f)

        fun objectProperty(
            typeMapping: Property.Builder, f: (ObjectProperty.Builder) -> ObjectProperty.Builder
        ): ObjectBuilder<Property> = typeMapping.`object`(f)

        fun ObjectProperty.Builder.field(
            key: String, f: (Property.Builder) -> ObjectBuilder<Property>
        ): ObjectProperty.Builder = this.properties(key, f)


        fun Property.Builder.keyword() = this.keyword { it }

        fun Property.Builder.text() = this.text { it }

        fun Property.Builder.double() = this.double_ { it }

        fun Property.Builder.rankComplex() = this.double_ { double_ ->
            double_.fields("rankFeature", Property.of { field ->
                field.rankFeature { it }
            })
        }

//        fun objectMapping(name: String, parent: ObjectProperty.Builder, f: (ObjectProperty.Builder) -> ObjectProperty.Builder): ObjectProperty.Builder = typeMappingProperty(parent, name) { property ->
//            objectProperty(property, f)
//        }

        fun Property.Builder.objectProperty(f: (ObjectProperty.Builder) -> ObjectProperty.Builder): ObjectBuilder<Property> =
            `object` { object1 ->
                f(object1)
            }

        val mappingRequest = CreateIndexRequest.of { indexRequest ->
            indexRequest.index(index)
            indexRequest.settings { settings ->
                settings.numberOfShards(numberOfShards.toString())
                settings.numberOfReplicas(numberOfReplicas.toString())
//                settings.analysis { analysis ->
//                    analysis.analyzer("standard", Analyzer.of { analyzer ->
//                        analyzer.language { language ->
//                            language.language(Language.English)
//                            language.stemExclusion(listOf(".", " ", ","))
//                        }
//                    })
//                }
            }
            indexRequest.mappings(TypeMapping.of { typeMapping ->
                typeMapping.field("url") {
                    it.keyword()
                }
                typeMapping.field("ranks") { ranks ->
                    ranks.objectProperty { op ->
                        op.field("pagerank") { it.rankComplex() }
                        op.field("smartRank") { it.rankComplex() }
                    }
                }
                typeMapping.field("content") { content ->
                    content.objectProperty { op ->
                        op.field("title") { it.text() }
                        op.field("description") { it.text() }
                        op.field("keywords") { it.text() }
                        op.field("anchors") { it.text() }
                        op.field("boldText") { it.text() }

                        op.field("headings") { headings ->
                            headings.objectProperty { op ->
                                op.field("h1") { it.text() }
                                op.field("h2") { it.text() }
                                op.field("h3") { it.text() }
                                op.field("h4") { it.text() }
                                op.field("h5") { it.text() }
                                op.field("h6") { it.text() }
                            }
                        }
                        op.field("text") { it.text() }
                    }
                }
            })
        }
        client.indices().create(mappingRequest)
    }

    @Suppress("Unused")
    suspend fun deleteIndex(index: String = this.index): DeleteIndexResponse = withContext(Dispatchers.IO) {
        client.indices().delete(DeleteIndexRequest.of {
            it.index(index)
        })
    }

    private fun searchAfterRequest(
        batchSize: Long,
        after: String?,
        field: String,
        sortOrder: SortOrder,
        must: (Query.Builder) -> ObjectBuilder<Query>,
        reqFields: List<String>? = null
    ): SearchRequest = SearchRequest.of { searchReq ->
        searchReq.index(index)
        if (reqFields != null) {
            searchReq.source { source ->
                source.filter {
                    it.includes(reqFields)
                }
                source
            }
        }
        searchReq.query { query ->
            query.bool { bool ->
                bool.must { must ->
                    must(must)
                }
            }
        }
        if (after != null) searchReq.searchAfter(after)
        searchReq.size(batchSize.toInt())
        searchReq.sort { sort ->
            sort.field {
                it.field(field)
                it.order(sortOrder)
            }
        }
    }

    @Suppress("Unused")
    suspend fun searchAfterUrl(batchSize: Long, url: String?) =
        search(searchAfterRequest(batchSize, url, "url", SortOrder.Asc, { must ->
            must.exists {
                it.field("url")
            }
        }))
}


class Alias(private val client: ElasticsearchClient, private val index: String) {

    @Suppress("Unused")
    suspend fun create(alias: String): PutAliasResponse = withContext(Dispatchers.IO) {
        client.indices().putAlias(PutAliasRequest.of {
            it.index(index)
            it.name(alias)
        })
    }

    @Suppress("Unused")
    suspend fun delete(alias: String) = withContext(Dispatchers.IO) {
        getIndexByAlias(alias).forEach { index ->
            client.indices().deleteAlias(DeleteAliasRequest.of {
                it.index(index)
                it.name(alias)
            })
        }
    }

    @Suppress("Unused")
    suspend fun getIndexByAlias(alias: String): Set<String> = withContext(Dispatchers.IO) {
        return@withContext try {
            client.indices().getAlias(GetAliasRequest.of {
                it.name(alias)
            }).result().keys
        } catch (e: Exception) {
            mutableSetOf(alias)
        }.toSet()
    }
}


suspend fun main() {
    val es = Elastic(
        Credentials("elastic", ""), Address("localhost", 9200), "search"
    )


    println("done")

}
